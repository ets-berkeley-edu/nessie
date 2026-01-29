"""
Copyright ©2025. The Regents of the University of California (Regents). All Rights Reserved.

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

from nessie.externals import redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError, verify_external_schema
from nessie.lib.util import (
    get_redshift_schema_boa_app_rds_data,
    get_s3_boa_app_rds_data_daily_path,
    resolve_sql_template,
)

"""Logic for BOA App RDS Data Redshift external schema creation and refresh Job."""


class RefreshBoaAppRdsDataSchema(BackgroundJob):

    def run(self, job_source='search'):
        app.logger.info('Starting full BOA App RDS Data Redshift external schema refresh...')
        return self.create_schema(job_source)

    def create_schema(self, job_source='search'):
        external_schema = get_redshift_schema_boa_app_rds_data(job_source)
        sql_filename = 'create_boa_app_rds_data_schema.template.sql'
        s3_boa_app_rds_data_daily = get_s3_boa_app_rds_data_daily_path(cutoff=None, job_source=job_source)

        if not s3.get_keys_with_prefix(s3_boa_app_rds_data_daily):
            s3_boa_app_rds_data_daily = _get_yesterdays_boa_app_rds_data(job_source)
        s3_path = '/'.join([f"s3://{app.config['LOCH_S3_BUCKET']}", s3_boa_app_rds_data_daily])

        app.logger.info('Executing SQL...')
        app.logger.info(f'Data Source: {external_schema} and Daily Path: {s3_boa_app_rds_data_daily}')
        app.logger.info('Dropping Redshift external schema now that we have found timely S3 BOA App RDS Data')
        redshift.drop_external_schema(external_schema)

        resolved_ddl = resolve_sql_template(
            sql_filename,
            loch_s3_boa_app_rds_data_path_daily=s3_path,
            redshift_schema_boa_app_rds_data=external_schema,
        )
        if not redshift.execute_ddl_script(resolved_ddl):
            raise BackgroundJobError(f'Redshift execute_ddl_script failed on {sql_filename}')

        verify_external_schema(
            external_schema,
            resolved_ddl,
            is_zero_count_acceptable=app.config['BOA_APP_RDS_DATA_ZERO_COUNT_ACCEPTABLE'],
        )
        app.logger.info('BOA App RDS Data Redshift external schema created and populated.')

        return True


def _get_yesterdays_boa_app_rds_data(job_source):
    s3_boa_app_rds_data_daily = get_s3_boa_app_rds_data_daily_path(datetime.now() - timedelta(days=1), job_source)
    if not s3.get_keys_with_prefix(s3_boa_app_rds_data_daily):
        raise BackgroundJobError('No timely BOA App RDS data found in S3 for today or previous day')

    app.logger.info('Falling back to last stable BOA App RDS data in S3')
    return s3_boa_app_rds_data_daily
