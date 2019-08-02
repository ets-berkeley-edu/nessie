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

from datetime import datetime, timedelta

from flask import current_app as app
from nessie.externals import calnet, rds, redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError, verify_external_schema
from nessie.lib.util import get_s3_sis_sysadm_daily_path, resolve_sql_template

"""Logic for SIS Advising Notes schema creation job."""


class CreateSisAdvisingNotesSchema(BackgroundJob):

    def run(self):
        app.logger.info(f'Starting SIS Advising Notes schema creation job...')

        daily_path = get_s3_sis_sysadm_daily_path()
        bucket = app.config['LOCH_S3_PROTECTED_BUCKET']
        if not s3.get_keys_with_prefix(f'{daily_path}/advising-notes', bucket=bucket):
            daily_path = get_s3_sis_sysadm_daily_path(datetime.now() - timedelta(days=1))
            if not s3.get_keys_with_prefix(f'{daily_path}/advising-notes', bucket=bucket):
                raise BackgroundJobError('No timely SIS advising notes data found, aborting')
            else:
                app.logger.info(f'Falling back to yesterday\'s SIS advising notes data')

        app.logger.info(f'Executing SQL...')
        external_schema = app.config['REDSHIFT_SCHEMA_SIS_ADVISING_NOTES']
        redshift.drop_external_schema(external_schema)
        self.create_historical_tables(external_schema)
        self.create_internal_schema(external_schema, daily_path)
        app.logger.info(f'Redshift schema created. Creating RDS indexes...')
        self.create_indexes()
        app.logger.info(f'RDS indexes created. Importing note authors...')
        self.import_note_authors()
        self.index_author_names()

        return 'SIS Advising Notes schema creation job completed.'

    def create_historical_tables(self, external_schema):
        resolved_ddl = resolve_sql_template('create_sis_advising_notes_historical_schema.template.sql')
        if redshift.execute_ddl_script(resolved_ddl):
            verify_external_schema(external_schema, resolved_ddl)
        else:
            raise BackgroundJobError(f'SIS Advising Notes schema creation job failed to load historical data.')

    def create_internal_schema(self, external_schema, daily_path):
        bucket = app.config['LOCH_S3_PROTECTED_BUCKET']
        s3_data_url = f's3://{bucket}/{daily_path}/advising-notes'
        resolved_ddl = resolve_sql_template('create_sis_advising_notes_schema.template.sql', loch_s3_sis_notes_path_today=s3_data_url)
        if redshift.execute_ddl_script(resolved_ddl):
            verify_external_schema(external_schema, resolved_ddl)
        else:
            raise BackgroundJobError(f'SIS Advising Notes schema creation job failed to load incremental data and create internal schema.')

    def create_indexes(self):
        resolved_ddl = resolve_sql_template('index_sis_advising_notes.template.sql')
        if rds.execute(resolved_ddl):
            app.logger.info('Created SIS Advising Notes RDS indexes.')
        else:
            raise BackgroundJobError('SIS Advising Notes schema creation job failed to create indexes.')

    def import_note_authors(self):
        notes_schema = app.config['RDS_SCHEMA_SIS_ADVISING_NOTES']
        advisor_schema_redshift = app.config['REDSHIFT_SCHEMA_ADVISOR_INTERNAL']

        advisor_sids_from_notes = set(
            [r['advisor_sid'] for r in rds.fetch(f'SELECT DISTINCT advisor_sid FROM {notes_schema}.advising_notes')],
        )
        advisor_sids_from_advisors = set(
            [r['sid'] for r in redshift.fetch(f'SELECT DISTINCT sid FROM {advisor_schema_redshift}.advisor_departments')],
        )
        advisor_sids = list(advisor_sids_from_notes | advisor_sids_from_advisors)
        advisor_attributes = calnet.client(app).search_csids(advisor_sids)

        if not advisor_attributes:
            raise BackgroundJobError('Failed to fetch note author attributes.')

        with rds.transaction() as transaction:
            if not transaction.execute(f'TRUNCATE {notes_schema}.advising_note_authors'):
                transaction.rollback()
                raise BackgroundJobError('Failed to truncate advising note author index.')

            insertable_rows = []
            for entry in advisor_attributes:
                first_name, last_name = calnet.split_sortable_name(entry)
                insertable_rows.append(tuple((entry.get('uid'), entry.get('csid'), first_name, last_name)))

            result = transaction.insert_bulk(
                f'INSERT INTO {notes_schema}.advising_note_authors (uid, sid, first_name, last_name) VALUES %s',
                insertable_rows,
            )
            if result:
                transaction.commit()
                app.logger.info('Import advising note author attributes.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Failed to import advising note author attributes.')
