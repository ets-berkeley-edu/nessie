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
from timeit import default_timer as timer

from flask import current_app as app
from nessie.externals import rds, redshift, s3, sis_student_api
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.queries import get_all_student_ids
from nessie.lib.util import encoded_tsv_row, get_s3_sis_api_daily_path, resolve_sql_template_string, split_tsv_row

"""Imports and stores SIS Students Registrations API data, including term GPAs and most recent registration."""


def async_get_feed(app_obj, sid):
    with app_obj.app_context():
        app.logger.info(f'Fetching registration history for SID {sid}')
        feed = sis_student_api.get_term_gpas_registration(sid)
        result = {
            'sid': sid,
            'feed': feed,
        }
    return result


class ImportTermGpas(BackgroundJob):

    rds_schema = app.config['RDS_SCHEMA_STUDENT']
    redshift_schema = app.config['REDSHIFT_SCHEMA_STUDENT']
    max_threads = app.config['STUDENT_API_MAX_THREADS']

    def run(self, sids=None):
        if not sids:
            sids = [row['sid'] for row in get_all_student_ids()]

        app.logger.info(f'Starting term GPA import job for {len(sids)} students...')

        rows = {
            'term_gpas': [],
            'last_registrations': [],
        }
        success_count, failure_count, no_registrations_count = self.load_concurrently(rows, sids)
        if (success_count == 0) and (failure_count > 0):
            raise BackgroundJobError('Failed to import registration histories: aborting job.')

        for key in rows.keys():
            s3_key = f'{get_s3_sis_api_daily_path()}/{key}.tsv'
            app.logger.info(f'Will stash {success_count} feeds in S3: {s3_key}')
            if not s3.upload_tsv_rows(rows[key], s3_key):
                raise BackgroundJobError('Error on S3 upload: aborting job.')
            app.logger.info('Will copy S3 feeds into Redshift...')
            if not redshift.execute(f'TRUNCATE {self.redshift_schema}_staging.student_{key}'):
                raise BackgroundJobError('Error truncating old staging rows: aborting job.')
            if not redshift.copy_tsv_from_s3(f'{self.redshift_schema}_staging.student_{key}', s3_key):
                raise BackgroundJobError('Error on Redshift copy: aborting job.')
            staging_to_destination_query = resolve_sql_template_string(
                """
                DELETE FROM {redshift_schema_student}.student_{table_key}
                    WHERE sid IN
                    (SELECT sid FROM {redshift_schema_student}_staging.student_{table_key});
                INSERT INTO {redshift_schema_student}.student_{table_key}
                    (SELECT * FROM {redshift_schema_student}_staging.student_{table_key});
                TRUNCATE TABLE {redshift_schema_student}_staging.student_{table_key};
                """,
                table_key=key,
            )
            if not redshift.execute(staging_to_destination_query):
                raise BackgroundJobError('Error inserting staging entries into destination: aborting job.')

        with rds.transaction() as transaction:
            if self.refresh_rds_indexes(sids, rows['term_gpas'], transaction):
                transaction.commit()
                app.logger.info('Refreshed RDS indexes.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Failed to refresh RDS indexes.')

        return (
            f'Term GPA import completed: {success_count} succeeded, '
            f'{no_registrations_count} returned no registrations, {failure_count} failed.'
        )

    def load_concurrently(self, rows, sids):
        success_count = 0
        failure_count = 0
        no_registrations_count = 0
        app_obj = app._get_current_object()
        start_loop = timer()
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            for result in executor.map(async_get_feed, repeat(app_obj), sids):
                sid = result['sid']
                reg_feed = result['feed']
                if reg_feed:
                    success_count += 1
                    rows['last_registrations'].append(
                        encoded_tsv_row([sid, json.dumps(reg_feed.get('last_registration', {}))]),
                    )
                    gpa_feed = reg_feed.get('term_gpas', {})
                    if gpa_feed:
                        for term_id, term_data in gpa_feed.items():
                            row = [
                                sid,
                                term_id,
                                (term_data.get('gpa') or '0'),
                                (term_data.get('unitsTakenForGpa') or '0'),
                            ]
                            rows['term_gpas'].append(encoded_tsv_row(row))
                    else:
                        app.logger.info(f'No past UGRD registrations found for SID {sid}.')
                        no_registrations_count += 1
                else:
                    failure_count += 1
                    app.logger.error(f'Registration history import failed for SID {sid}.')
        app.logger.info(f'Wanted {len(sids)} students; got {success_count} in {timer() - start_loop} secs')
        return success_count, failure_count, no_registrations_count

    def refresh_rds_indexes(self, sids, rows, transaction):
        sql = f'DELETE FROM {self.rds_schema}.student_term_gpas WHERE sid = ANY(%s)'
        params = (sids,)
        if not transaction.execute(sql, params):
            return False
        if not transaction.insert_bulk(
            f"""INSERT INTO {self.rds_schema}.student_term_gpas
                (sid, term_id, gpa, units_taken_for_gpa) VALUES %s""",
            [split_tsv_row(r) for r in rows],
        ):
            return False

        return True
