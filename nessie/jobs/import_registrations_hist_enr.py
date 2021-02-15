"""
Copyright ©2021. The Regents of the University of California (Regents). All Rights Reserved.

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
from nessie.externals import redshift, s3, sis_student_api
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.berkeley import edl_registration_to_json, feature_flag_edl
from nessie.lib.queries import get_edl_student_registrations, get_non_advisees_without_registration_imports, \
    student_schema
from nessie.lib.util import encoded_tsv_row, get_s3_sis_api_daily_path, resolve_sql_template_string
import numpy as np

"""Imports historical student registration data."""


class ImportRegistrationsHistEnr(BackgroundJob):

    rds_schema = app.config['RDS_SCHEMA_STUDENT']
    max_threads = app.config['STUDENT_API_MAX_THREADS']

    def run(self, load_mode='batch'):
        new_sids = [row['sid'] for row in get_non_advisees_without_registration_imports()]

        # The size of the non-advisee population makes it unlikely that a one-shot load of all these slow feeds will
        # finish successfully without interfering with other work. Therefore the default approach is to apply a strict
        # upper limit on the number of feeds loaded in any one job run, no matter how many SIDs remain to be processed.
        if load_mode == 'new':
            sids = new_sids
        elif load_mode == 'batch':
            max_batch = app.config['HIST_ENR_REGISTRATIONS_IMPORT_BATCH_SIZE']
            if max_batch >= len(new_sids):
                sids = new_sids
            else:
                sids = new_sids[0:(max_batch)]

        app.logger.info(f'Starting import of historical registration data for {len(sids)} students...')
        redshift.execute('VACUUM; ANALYZE;')

        rows = {
            'term_gpas': [],
            'last_registrations': [],
        }
        successes, failures = self._query_edl(rows, sids) if feature_flag_edl() else self._query_student_api(rows, sids)
        if len(successes) > 0:
            for key in rows.keys():
                filename = f'{key}_edl' if feature_flag_edl() else f'{key}_api'
                s3_key = f'{get_s3_sis_api_daily_path()}/{filename}.tsv'
                app.logger.info(f'Upload {key} data to s3:{s3_key}. The file represents {len(rows[key])} students.')
                if not s3.upload_tsv_rows(rows[key], s3_key):
                    raise BackgroundJobError(f'Error during S3 upload: {s3_key}. Aborting job.')

                staging_table = f'{student_schema()}_staging.hist_enr_{key}'
                if not redshift.execute(f'TRUNCATE {staging_table}'):
                    raise BackgroundJobError('Error truncating old staging rows: aborting job.')

                app.logger.info(f'Populate {staging_table} (Redshift table) with s3:{s3_key}')
                if not redshift.copy_tsv_from_s3(staging_table, s3_key):
                    raise BackgroundJobError('Error on Redshift copy: aborting job.')

                app.logger.info(f'Insert student data into {student_schema()}.hist_enr_{key}')
                staging_to_destination_query = resolve_sql_template_string(
                    """
                    DELETE FROM {student_schema}.hist_enr_{table_key}
                        WHERE sid IN
                        (SELECT sid FROM {student_schema}_staging.hist_enr_{table_key});
                    INSERT INTO {student_schema}.hist_enr_{table_key}
                        (SELECT * FROM {student_schema}_staging.hist_enr_{table_key});
                    TRUNCATE TABLE {student_schema}_staging.hist_enr_{table_key};
                    """,
                    table_key=key,
                    student_schema=student_schema(),
                )
                if not redshift.execute(staging_to_destination_query):
                    raise BackgroundJobError('Error inserting staging entries into destination: aborting job.')

        redshift.execute('VACUUM; ANALYZE;')
        app.logger.info(f'Finished import of historical registration data: {len(successes)} successes and {len(failures)} failures.')
        return successes, failures

    def _query_edl(self, rows, sids):
        successes = []
        for edl_row in get_edl_student_registrations(sids):
            sid = edl_row['student_id']
            if sid not in successes:
                # Based on the SQL order_by above, the first result per SID will be 'last_registration'.
                successes.append(sid)
                rows['last_registrations'].append(
                    encoded_tsv_row([sid, edl_registration_to_json(edl_row)]),
                )
            rows['term_gpas'].append(
                encoded_tsv_row(
                    [
                        sid,
                        edl_row['term_id'],
                        edl_row['current_term_gpa'] or '0',
                        edl_row.get('unt_taken_gpa') or '0',  # TODO: Does EDL give us 'unitsTakenForGpa'?
                    ],
                ),
            )
        failures = list(np.setdiff1d(sids, successes))
        return successes, failures

    def _query_student_api(self, rows, sids):
        successes = []
        failures = []
        app_obj = app._get_current_object()
        start_loop = timer()
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            for result in executor.map(_async_get_feed, repeat(app_obj), sids):
                sid = result['sid']
                full_feed = result['feed']
                if full_feed:
                    successes.append(sid)
                    rows['last_registrations'].append(
                        encoded_tsv_row([sid, json.dumps(full_feed.get('last_registration', {}))]),
                    )
                    gpa_feed = full_feed.get('term_gpas', {})
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
                else:
                    failures.append(sid)
                    app.logger.error(f'Registration history import failed for SID {sid}.')
        app.logger.info(f'Wanted {len(sids)} students; got {len(successes)} in {timer() - start_loop} secs')
        return successes, failures


def _async_get_feed(app_obj, sid):
    with app_obj.app_context():
        app.logger.info(f'Fetching registration history for SID {sid}')
        feed = sis_student_api.get_term_gpas_registration_demog(sid, with_demog=False)
        result = {
            'sid': sid,
            'feed': feed,
        }
    return result
