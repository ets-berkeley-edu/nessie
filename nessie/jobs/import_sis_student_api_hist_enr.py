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
from itertools import repeat
import json
import tempfile
from timeit import default_timer as timer

from flask import current_app as app
from nessie.externals import redshift
from nessie.externals.sis_student_api import get_v2_by_sids_list
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.queries import get_non_advisee_unfetched_student_ids
from nessie.lib.util import encoded_tsv_row, resolve_sql_template_string
from nessie.models import student_schema

"""Logic for SIS student API import job."""


def async_get_feeds(app_obj, up_to_100_sids):
    with app_obj.app_context():
        feeds = get_v2_by_sids_list(up_to_100_sids, with_contacts=False)
        result = {
            'sids': up_to_100_sids,
            'feeds': feeds,
        }
    return result


class ImportSisStudentApiHistEnr(BackgroundJob):

    redshift_schema = app.config['REDSHIFT_SCHEMA_STUDENT']
    max_threads = app.config['STUDENT_API_MAX_THREADS']

    def run(self, sids=None):
        if not sids:
            sids = [row['sid'] for row in get_non_advisee_unfetched_student_ids()]
        app.logger.info(f'Starting SIS student API import job for {len(sids)} non-advisees...')

        with tempfile.TemporaryFile() as feed_file:
            saved_sids, failure_count = self.load_concurrently(sids, feed_file)
            if saved_sids:
                student_schema.truncate_staging_table('sis_api_profiles_hist_enr')
                student_schema.write_file_to_staging('sis_api_profiles_hist_enr', feed_file, len(saved_sids))

        if saved_sids:
            staging_to_destination_query = resolve_sql_template_string(
                """
                DELETE FROM {redshift_schema_student}.sis_api_profiles_hist_enr WHERE sid IN
                    (SELECT sid FROM {redshift_schema_student}_staging.sis_api_profiles_hist_enr);
                INSERT INTO {redshift_schema_student}.sis_api_profiles_hist_enr
                    (SELECT * FROM {redshift_schema_student}_staging.sis_api_profiles_hist_enr);
                TRUNCATE {redshift_schema_student}_staging.sis_api_profiles_hist_enr;
                """,
            )
            if not redshift.execute(staging_to_destination_query):
                raise BackgroundJobError('Error on Redshift copy: aborting job.')

        return f'SIS student API non-advisee import job completed: {len(saved_sids)} succeeded, {failure_count} failed.'

    def load_concurrently(self, all_sids, feed_file):

        chunked_sids = [all_sids[i:i + 100] for i in range(0, len(all_sids), 100)]
        saved_sids = []
        failure_count = 0
        app_obj = app._get_current_object()
        start_loop = timer()

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            for result in executor.map(async_get_feeds, repeat(app_obj), chunked_sids):
                remaining_sids = set(result['sids'])
                feeds = result['feeds']
                if feeds:
                    for feed in feeds:
                        sid = next(id['id'] for id in feed['identifiers'] if id['type'] == 'student-id')
                        uid = next(id['id'] for id in feed['identifiers'] if id['type'] == 'campus-uid')

                        # An extremely crude defense against SISRP-48296.
                        for ac_stat in feed.get('academicStatuses', []):
                            for ac_plan in ac_stat.get('studentPlans', []):
                                ac_plan['academicSubPlans'] = []

                        feed_file.write(encoded_tsv_row([sid, uid, json.dumps(feed)]) + b'\n')
                        remaining_sids.discard(sid)
                        saved_sids.append(sid)
                if remaining_sids:
                    failure_count = len(remaining_sids)
                    app.logger.error(f'SIS student API import failed for non-advisees {remaining_sids}.')

        app.logger.info(f'Wanted {len(all_sids)} non-advisees; got {len(saved_sids)} in {timer() - start_loop} secs')
        return saved_sids, failure_count
