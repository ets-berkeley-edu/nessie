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
from nessie.lib.util import get_s3_bi_boa_rds_data_daily_path, resolve_sql_template

"""Logic for BOA RDS Data schema creation and refresh job."""


class RefreshBiBoaRdsDataSchema(BackgroundJob):

    external_schema = app.config['BI_REDSHIFT_SCHEMA_BOA_RDS_DATA']

    def run(self):
        app.logger.info('Starting full BOA RDS Data refresh...')
        return self.create_schema()

    def create_schema(self):
        s3_boa_rds_daily = get_s3_bi_boa_rds_data_daily_path()
        if not s3.get_keys_with_prefix(s3_boa_rds_daily):
            s3_boa_rds_daily = _get_yesterdays_boa_rds_data()
        s3_path = '/'.join([f"s3://{app.config['LOCH_S3_BUCKET']}", s3_boa_rds_daily])

        app.logger.info('Executing SQL...')
        app.logger.info('Dropping External Schema now that we have found timely S3 BOA RDS Data')
        redshift.drop_external_schema(self.external_schema)
        sql_filename = 'bi_create_boa_rds_data_schema.template.sql'
        resolved_ddl = resolve_sql_template(sql_filename, bi_loch_s3_boa_rds_data_path=s3_path)
        if not redshift.execute_ddl_script(resolved_ddl):
            raise BackgroundJobError(f'Redshift execute_ddl_script failed on {sql_filename}')
        verify_external_schema(
            self.external_schema,
            resolved_ddl,
            is_zero_count_acceptable=app.config['BI_BOA_RDS_ZERO_COUNT_ACCEPTABLE'],
        )
        app.logger.info('Redshift schema created.')

        return True


def _get_yesterdays_boa_rds_data():
    s3_boa_rds_daily = get_s3_bi_boa_rds_data_daily_path(datetime.now() - timedelta(days=1))
    if not s3.get_keys_with_prefix(s3_boa_rds_daily):
        raise BackgroundJobError('No timely BOA RDS S3 data found for today and previous day')

    app.logger.info('Falling back to last stable BOA RDS2 S3 data ')
    return s3_boa_rds_daily
