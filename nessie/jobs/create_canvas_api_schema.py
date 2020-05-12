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

from datetime import datetime, timedelta

from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError, verify_external_schema
from nessie.lib.util import get_s3_canvas_api_daily_path, resolve_sql_template

"""Logic for Canvas API schema creation job."""


class CreateCanvasApiSchema(BackgroundJob):

    def run(self):
        app.logger.info('Starting Canvas API schema creation job...')
        canvas_api_path = get_s3_canvas_api_daily_path()
        if not s3.get_keys_with_prefix(canvas_api_path):
            canvas_api_path = get_s3_canvas_api_daily_path(datetime.now() - timedelta(days=1))
            if not s3.get_keys_with_prefix(canvas_api_path):
                raise BackgroundJobError('No timely Canvas API data found, aborting')
            else:
                app.logger.info('Falling back to yesterday\'s Canvas API data')

        external_schema = app.config['REDSHIFT_SCHEMA_CANVAS_API']
        s3_prefix = 's3://' + app.config['LOCH_S3_BUCKET'] + '/'
        s3_canvas_api_data_url = s3_prefix + canvas_api_path

        redshift.drop_external_schema(external_schema)
        resolved_ddl = resolve_sql_template(
            'create_canvas_api_schema.template.sql',
            loch_s3_canvas_api_data_path_today=s3_canvas_api_data_url,
        )
        if redshift.execute_ddl_script(resolved_ddl):
            verify_external_schema(external_schema, resolved_ddl)
            return 'Canvas API schema creation job completed.'
        else:
            raise BackgroundJobError('Canvas API schema creation job failed.')
