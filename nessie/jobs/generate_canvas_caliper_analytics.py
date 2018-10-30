"""
Copyright ©2018. The Regents of the University of California (Regents). All Rights Reserved.

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
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.util import resolve_sql_template

"""Generate analytics from exploded canvas caliper statements."""


class GenerateCanvasCaliperAnalytics(BackgroundJob):
    def run(self):
        app.logger.info('Start generating canvas caliper analytics')
        redshift_schema_lrs_external = app.config['REDSHIFT_SCHEMA_LRS']
        canvas_caliper_explode_table = 'canvas_caliper_explode'
        caliper_explode_url = 's3://{}/{}'.format(app.config['LOCH_S3_BUCKET'], app.config['LRS_CANVAS_CALIPER_EXPLODE_OUTPUT_PATH'])
        resolved_ddl_caliper_explode = resolve_sql_template(
            'create_lrs_canvas_explode_table.template.sql',
            canvas_caliper_explode_table=canvas_caliper_explode_table,
            loch_s3_caliper_explode_path=caliper_explode_url,
        )
        redshift.drop_external_schema(redshift_schema_lrs_external)
        if redshift.execute_ddl_script(resolved_ddl_caliper_explode):
            app.logger.info('Caliper explode schema and table successfully created.')
        else:
            app.logger.error('Caliper explode schema and table creation failed.')
            return False

        app.logger.info('Verify if data exists in caliper explode tables')
        redshift_response = redshift.fetch(f'select count(*) from {redshift_schema_lrs_external}.{canvas_caliper_explode_table}')

        if redshift_response:
            if redshift_response[0].get('count'):
                app.logger.info('Generating user request tables and caliper analytics.')

                resolved_ddl_caliper_analytics = resolve_sql_template('generate_caliper_analytics.template.sql')
                if redshift.execute_ddl_script(resolved_ddl_caliper_analytics):
                    return 'Caliper analytics tables successfully created.'
                else:
                    app.logger.error('Caliper analytics tables creation failed.')
                    return False
            else:
                return False
        else:
            app.logger.error(f"Failed to verify caliper explode schema and tables '{redshift_schema_lrs_external}'.'{canvas_caliper_explode_table}'.")
            return False
