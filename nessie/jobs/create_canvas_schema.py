"""
Copyright ©2022. The Regents of the University of California (Regents). All Rights Reserved.

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
from nessie.lib import berkeley
from nessie.lib.util import get_s3_canvas_daily_path, resolve_sql_template

"""Logic for Canvas schema creation job."""


class CreateCanvasSchema(BackgroundJob):

    def run(self):
        app.logger.info('Starting Canvas schema creation job...')

        canvas_path = get_s3_canvas_daily_path()
        if not s3.get_keys_with_prefix(canvas_path):
            canvas_path = get_s3_canvas_daily_path(datetime.now() - timedelta(days=1))
            if not s3.get_keys_with_prefix(canvas_path):
                raise BackgroundJobError('No timely Canvas data found, aborting')
            else:
                app.logger.info('Falling back to yesterday\'s Canvas data')

        external_schema = app.config['REDSHIFT_SCHEMA_CANVAS']
        s3_prefix = 's3://' + app.config['LOCH_S3_BUCKET'] + '/'
        s3_canvas_data_url = s3_prefix + canvas_path
        s3_canvas_data_path_current_term = s3_prefix + berkeley.s3_canvas_data_path_current_term()

        redshift.drop_external_schema(external_schema)
        resolved_ddl = resolve_sql_template(
            'create_canvas_schema.template.sql',
            loch_s3_canvas_data_path_today=s3_canvas_data_url,
            loch_s3_canvas_data_path_current_term=s3_canvas_data_path_current_term,
        )
        if redshift.execute_ddl_script(resolved_ddl):
            verify_external_schema(external_schema, resolved_ddl)
            return 'Canvas schema creation job completed.'
        else:
            raise BackgroundJobError('Canvas schema creation job failed.')
