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

from flask import current_app as app
from nessie.externals import rds, redshift, s3
from nessie.jobs.abstract.abstract_registrations_job import AbstractRegistrationsJob
from nessie.jobs.background_job import BackgroundJobError
from nessie.lib.metadata import update_registration_import_status
from nessie.lib.queries import get_active_sids_with_oldest_registration_imports, get_all_student_ids, \
    get_sids_with_registration_imports, student_schema
from nessie.lib.util import get_s3_sis_api_daily_path, resolve_sql_template_string, split_tsv_row

"""Imports and stores SIS Students Registrations API data, including term GPAs and most recent registration."""


class ImportRegistrations(AbstractRegistrationsJob):

    rds_schema = app.config['RDS_SCHEMA_STUDENT']

    def run(self, load_mode='new'):
        all_sids = [row['sid'] for row in get_all_student_ids()]
        previous_backfills = {row['sid'] for row in get_sids_with_registration_imports()}

        if load_mode == 'new':
            sids = list(set(all_sids).difference(previous_backfills))
        elif load_mode == 'batch':
            new_sids = list(set(all_sids).difference(previous_backfills))
            limit = app.config['CYCLICAL_API_IMPORT_BATCH_SIZE'] - len(new_sids)
            if limit > 0:
                oldest_backfills = [row['sid'] for row in get_active_sids_with_oldest_registration_imports(limit=limit)]
                sids = new_sids + oldest_backfills
            else:
                sids = new_sids
        elif load_mode == 'all':
            sids = all_sids

        app.logger.info(f'Starting registrations/demographics import job for {len(sids)} students...')

        rows = {
            'term_gpas': [],
            'last_registrations': [],
            self.demographics_key: [],
        }
        successes, failures = self.get_registration_data_per_sids(rows, sids)
        if load_mode != 'new' and (len(successes) == 0) and (len(failures) > 0):
            raise BackgroundJobError('Failed to import registration histories: aborting job.')

        for key in rows.keys():
            s3_key = f'{get_s3_sis_api_daily_path(use_edl_if_feature_flag=True)}/{key}.tsv'
            app.logger.info(f'Will stash {len(successes)} feeds in S3: {s3_key}')
            if not s3.upload_tsv_rows(rows[key], s3_key):
                raise BackgroundJobError('Error on S3 upload: aborting job.')
            app.logger.info('Will copy S3 feeds into Redshift...')
            if not redshift.execute(f'TRUNCATE {student_schema()}_staging.student_{key}'):
                raise BackgroundJobError('Error truncating old staging rows: aborting job.')
            if not redshift.copy_tsv_from_s3(f'{student_schema()}_staging.student_{key}', s3_key):
                raise BackgroundJobError('Error on Redshift copy: aborting job.')
            staging_to_destination_query = resolve_sql_template_string(
                """
                DELETE FROM {student_schema}.student_{table_key}
                    WHERE sid IN
                    (SELECT sid FROM {student_schema}_staging.student_{table_key});
                INSERT INTO {student_schema}.student_{table_key}
                    (SELECT * FROM {student_schema}_staging.student_{table_key});
                TRUNCATE TABLE {student_schema}_staging.student_{table_key};
                """,
                table_key=key,
                student_schema=student_schema(),
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

        update_registration_import_status(successes, failures)

        return (
            f'Registrations import completed: {len(successes)} succeeded, {len(failures)} failed.'
        )

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
