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


"""Logic for Canvas schema creation job."""


from flask import current_app as app
from nessie.externals import redshift
from nessie.jobs.background_job import BackgroundJob, verify_external_schema
from nessie.lib.util import resolve_sql_template


class CreateCanvasSchema(BackgroundJob):

    def run(self):
        app.logger.info(f'Starting Canvas schema creation job...')
        external_schema = app.config['REDSHIFT_SCHEMA_CANVAS']
        redshift.drop_external_schema(external_schema)
        resolved_ddl = resolve_sql_template('create_canvas_schema.template.sql')
        if redshift.execute_ddl_script(resolved_ddl):
            app.logger.info(f'Canvas schema creation job completed.')
            return verify_external_schema(external_schema, resolved_ddl)
        else:
            app.logger.error(f'Canvas schema creation job failed.')
            return False
