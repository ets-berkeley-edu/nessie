"""
Copyright ©2020. The Regents of the University of California (Regents). All Rights Reserved.

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

from datetime import datetime

from flask import current_app as app
from nessie.externals import rds, redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError, verify_external_schema
from nessie.lib.util import get_s3_oua_daily_path, resolve_sql_template

"""Logic for OUA Slate Admissions schema creation job."""


class CreateOUASchema(BackgroundJob):

    def run(self):
        app.logger.info('Starting OUA Slate schema creation job...')
        app.logger.info('Executing SQL...')

        s3_protected_bucket = app.config['LOCH_S3_PROTECTED_BUCKET']
        oua_slate_sftp_path = app.config['LOCH_S3_SLATE_DATA_SFTP_PATH'] + '/' + self.get_sftp_date_offset() + '/'
        oua_daily_dest_path = get_s3_oua_daily_path() + '/admissions/'

        # Gets list of keys under SFTP prefix and looks for csv files to migrate to OUA daily location
        keys = s3.get_keys_with_prefix(oua_slate_sftp_path, full_objects=False, bucket=s3_protected_bucket)

        if len(keys) > 0:
            for source_key in keys:
                if source_key.endswith('.csv'):
                    destination_key = source_key.replace(oua_slate_sftp_path, oua_daily_dest_path)
                    if not s3.copy(s3_protected_bucket, source_key, s3_protected_bucket, destination_key):
                        raise BackgroundJobError(f'Copy from SFTP location {source_key} to daily OUA destination {destination_key} failed.')
            external_schema = app.config['REDSHIFT_SCHEMA_OUA']
            redshift.drop_external_schema(external_schema)
            resolved_ddl = resolve_sql_template('create_oua_schema_template.sql')
            if redshift.execute_ddl_script(resolved_ddl):
                verify_external_schema(external_schema, resolved_ddl)
                self.create_rds_tables_and_indexes()
                app.logger.info('OUA Slate RDS indexes created.')
                return 'OUA schema creation job completed.'

            else:
                raise BackgroundJobError('OUA Slate schema creation job failed.')

        else:
            return 'No OUA files found in SFTP location today. Skipping OUA data refresh'

    @staticmethod
    def create_rds_tables_and_indexes():
        resolved_ddl = resolve_sql_template('index_oua_admissions.template.sql')
        if rds.execute(resolved_ddl):
            app.logger.info('Created OUA Slate RDS tables and indexes successfully')
        else:
            raise BackgroundJobError('OUA Slate schema creation job failed to create rds tables and indexes.')

    @staticmethod
    def get_sftp_date_offset():
        current_date = datetime.now()
        date_part = current_date.strftime('%Y/%m/%d')
        return date_part
