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
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.util import resolve_sql_template

"""Logic for student schema creation job."""


class CreateStudentSchema(BackgroundJob):

    def run(self):
        app.logger.info(f'Starting student schema creation job...')
        app.logger.info(f'Executing SQL...')
        resolved_ddl = resolve_sql_template('create_student_schema.template.sql')
        if redshift.execute_ddl_script(resolved_ddl):
            app.logger.info(f"Schema '{app.config['REDSHIFT_SCHEMA_STUDENT']}' found or created.")
        else:
            app.logger.error(f'Student schema creation failed.')
            return False
        resolved_ddl_staging = resolve_sql_template(
            'create_student_schema.template.sql',
            redshift_schema_student=app.config['REDSHIFT_SCHEMA_STUDENT'] + '_staging',
        )
        if redshift.execute_ddl_script(resolved_ddl_staging):
            app.logger.info(f"Schema '{app.config['REDSHIFT_SCHEMA_STUDENT']}_staging' found or created.")
        else:
            app.logger.error(f'Student staging schema creation failed.')
            return False
        return True
