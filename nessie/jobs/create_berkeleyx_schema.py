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

from flask import current_app as app
from nessie.externals import redshift
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.util import resolve_sql_template

"""Logic for Canvas schema creation job."""


class CreateBerkeleyxSchema(BackgroundJob):

    def run(self):
        success = 0
        failure = 0
        berkeleyx_tenants = {
            'edx': {
                'database': 'berkeleyx_prod_ext',
                'instance': 'prod-analytics',
            },
            'edge': {
                'database': 'berkeleyx_prod_edge_ext',
                'instance': 'prod-edge-analytics',
            },
        }

        for tenant, value in berkeleyx_tenants.items():
            app.logger.info(f'Starting Berkeleyx schema creation job for {tenant}...')
            berkeleyx_data_path = 's3://{}/{}/{}'.format(
                app.config['LOCH_EDX_S3_BUCKET'],
                app.config['LOCH_EDX_S3_WEEKLY_DATA_PATH'],
                value['instance'],
            )
            berkeleyx_transaction_log_path = 's3://{}/{}/{}'.format(
                app.config['LOCH_EDX_S3_BUCKET'],
                app.config['LOCH_EDX_S3_TRANSACTION_LOG_PATH'],
                tenant,
            )
            external_schema = value['database'] + '_' + app.config['LOCH_EDX_NESSIE_ENV']
            redshift.drop_external_schema(external_schema)
            resolved_ddl = resolve_sql_template(
                'create_berkeleyx_schema.template.sql',
                loch_s3_berkeleyx_data_path=berkeleyx_data_path,
                loch_s3_berkeleyx_transaction_log_path=berkeleyx_transaction_log_path,
                redshift_berkeleyx_ext_schema=external_schema,
            )

            if redshift.execute_ddl_script(resolved_ddl):
                app.logger.info(f'BerkeleyX schema {external_schema} creation completed.')
                success += 1
            else:
                app.logger.error(f'BerkeleyX schema creation {external_schema} failed.')
                failure += 1

        if failure > 0:
            raise BackgroundJobError(f'Berkeleyx Schema creation jobs failed')
        else:
            app.logger.info(f'Bekreleyx schema creation jobs completed successfully')
            return True
