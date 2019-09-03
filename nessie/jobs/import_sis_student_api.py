"""
Copyright ©2019. The Regents of the University of California (Regents). All Rights Reserved.

Permission to use, copy, modify, and distribute this software and its documentation
for educational, research, and not-for-profit purposes, without fee and without a
signed licensing agreement, is hereby granted, provided that the above copyright
notice, this paragraph and the following two paragraphs appear in all copies,
modifications, and distributions.

Contact The Office of Technology Licensing, UC Berkeley, 2150 Shattuck Avenue,
Suite 510, Berkeley, CA 94720-1620, (510) 643-7201, otl@berkeley.edu,
http://ipira.berkeley.edu/industry-info for commercial licensing opportunities.

IN NO EVENT SHALL REGENTS BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF REGENTS HAS BEEN ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.

REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED
"AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
ENHANCEMENTS, OR MODIFICATIONS.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from itertools import repeat
import json
from timeit import default_timer as timer

from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.externals.sis_student_api import get_v1_student, get_v2_by_sids_list
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.berkeley import current_term_id
from nessie.lib.queries import get_all_student_ids
from nessie.lib.util import encoded_tsv_row, get_s3_sis_api_daily_path, resolve_sql_template_string

"""Logic for SIS student API import job."""


def async_get_feeds(app_obj, up_to_100_sids, as_of):
    with app_obj.app_context():
        feeds = get_v2_by_sids_list(up_to_100_sids, term_id=current_term_id(), with_registration=True, as_of=as_of)
        result = {
            'sids': up_to_100_sids,
            'feeds': feeds,
        }
    return result


def async_get_feed_v1(app_obj, sid):
    with app_obj.app_context():
        feed = get_v1_student(sid)
        result = {
            'sid': sid,
            'feed': feed,
        }
    return result


class ImportSisStudentApi(BackgroundJob):

    redshift_schema = app.config['REDSHIFT_SCHEMA_STUDENT']
    max_threads = app.config['STUDENT_API_MAX_THREADS']

    def run(self, csids=None):
        if app.config['STUDENT_V1_API_PREFERRED']:
            return self.run_v1(csids)
        if not csids:
            csids = [row['sid'] for row in get_all_student_ids()]
        app.logger.info(f'Starting SIS student API import job for {len(csids)} students...')

        rows, failure_count = self.load_concurrently(csids)

        s3_key = f'{get_s3_sis_api_daily_path()}/profiles.tsv'
        app.logger.info(f'Will stash {len(rows)} feeds in S3: {s3_key}')
        if not s3.upload_tsv_rows(rows, s3_key):
            raise BackgroundJobError('Error on S3 upload: aborting job.')

        app.logger.info('Will copy S3 feeds into Redshift...')
        if not redshift.execute(f'TRUNCATE {self.redshift_schema}_staging.sis_api_profiles'):
            raise BackgroundJobError('Error truncating old staging rows: aborting job.')
        if not redshift.copy_tsv_from_s3(f'{self.redshift_schema}_staging.sis_api_profiles', s3_key):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')
        staging_to_destination_query = resolve_sql_template_string(
            """
            DELETE FROM {redshift_schema_student}.sis_api_profiles WHERE sid IN
                (SELECT sid FROM {redshift_schema_student}_staging.sis_api_profiles);
            INSERT INTO {redshift_schema_student}.sis_api_profiles
                (SELECT * FROM {redshift_schema_student}_staging.sis_api_profiles);
            TRUNCATE {redshift_schema_student}_staging.sis_api_profiles;
            """,
        )
        if not redshift.execute(staging_to_destination_query):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')

        return f'SIS student API import job completed: {len(rows)} succeeded, {failure_count} failed.'

    def load_concurrently(self, all_sids):
        # Unlike the V1 Students API, V2 will not returns 'unitsTransferEarned' and 'unitsTransferAccepted' data
        # for incoming transfer students unless we request an 'as-of-date' in their enrolled term.
        near_future = (datetime.now() + timedelta(days=60)).strftime('%Y-%m-%d')

        chunked_sids = [all_sids[i:i + 100] for i in range(0, len(all_sids), 100)]
        rows = []
        failure_count = 0
        app_obj = app._get_current_object()
        start_loop = timer()
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            for result in executor.map(async_get_feeds, repeat(app_obj), chunked_sids, repeat(near_future)):
                remaining_sids = set(result['sids'])
                feeds = result['feeds']
                if feeds:
                    for feed in feeds:
                        sid = next(id['id'] for id in feed['identifiers'] if id['type'] == 'student-id')
                        remaining_sids.discard(sid)

                        # An extremely crude defense against SISRP-48296.
                        for ac_stat in feed.get('academicStatuses', []):
                            for ac_plan in ac_stat.get('studentPlans', []):
                                ac_plan['academicSubPlans'] = []

                        rows.append(encoded_tsv_row([sid, json.dumps(feed)]))
                if remaining_sids:
                    failure_count = len(remaining_sids)
                    app.logger.error(f'SIS student API import failed for SIDs {remaining_sids}.')
        app.logger.info(f'Wanted {len(all_sids)} students; got {len(rows)} in {timer() - start_loop} secs')
        return rows, failure_count

    def run_v1(self, csids=None):
        if not csids:
            csids = [row['sid'] for row in get_all_student_ids()]
        app.logger.info(f'Starting SIS student API V1 import job for {len(csids)} students...')

        rows, failure_count = self.load_concurrently_v1(csids)

        s3_key = f'{get_s3_sis_api_daily_path()}/profiles.tsv'
        app.logger.info(f'Will stash {len(rows)} feeds in S3: {s3_key}')
        if not s3.upload_tsv_rows(rows, s3_key):
            raise BackgroundJobError('Error on S3 upload: aborting job.')

        app.logger.info('Will copy S3 feeds into Redshift...')
        if not redshift.execute(f'TRUNCATE {self.redshift_schema}_staging.sis_api_profiles_v1'):
            raise BackgroundJobError('Error truncating old staging rows: aborting job.')
        if not redshift.copy_tsv_from_s3(f'{self.redshift_schema}_staging.sis_api_profiles_v1', s3_key):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')
        staging_to_destination_query = resolve_sql_template_string(
            """
            DELETE FROM {redshift_schema_student}.sis_api_profiles_v1 WHERE sid IN
                (SELECT sid FROM {redshift_schema_student}_staging.sis_api_profiles_v1);
            INSERT INTO {redshift_schema_student}.sis_api_profiles_v1
                (SELECT * FROM {redshift_schema_student}_staging.sis_api_profiles_v1);
            TRUNCATE {redshift_schema_student}_staging.sis_api_profiles_v1;
            """,
        )
        if not redshift.execute(staging_to_destination_query):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')

        return f'SIS student API V1 import job completed: {len(rows)} succeeded, {failure_count} failed.'

    def load_concurrently_v1(self, csids):
        rows = []
        failure_count = 0
        app_obj = app._get_current_object()
        start_loop = timer()
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            for result in executor.map(async_get_feed_v1, repeat(app_obj), csids):
                csid = result['sid']
                feed = result['feed']
                if feed:
                    rows.append(encoded_tsv_row([csid, json.dumps(feed)]))
                else:
                    failure_count += 1
                    app.logger.error(f'SIS student API V1 import failed for CSID {csid}.')
        app.logger.info(f'Wanted {len(csids)} students; got {len(rows)} in {timer() - start_loop} secs')
        return rows, failure_count
