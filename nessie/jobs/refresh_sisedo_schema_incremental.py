"""
Copyright ©2024. The Regents of the University of California (Regents). All Rights Reserved.

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
from nessie.externals import rds, redshift
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.util import resolve_sql_template

"""Logic for SISEDO schema creation job."""


class RefreshSisedoSchemaIncremental(BackgroundJob):

    def run(self):
        app.logger.info('Starting incremental SISEDO refresh...')
        return self.create_schema()

    def create_schema(self):
        app.logger.info('Executing SQL...')

        sql_filename = 'refresh_sisedo_schema_incremental.template.sql'
        resolved_ddl = resolve_sql_template(sql_filename)
        if not redshift.execute_ddl_script(resolved_ddl):
            raise BackgroundJobError(f'Redshift execute_ddl_script failed on {sql_filename}')
        app.logger.info('Redshift tables created.')

        resolved_ddl_rds = resolve_sql_template('update_rds_indexes_sisedo_incremental.template.sql')
        if not rds.execute(resolved_ddl_rds):
            raise BackgroundJobError('Failed to update RDS indexes for incremental SISEDO refresh.')
        app.logger.info('RDS indexes updated.')

        return True
