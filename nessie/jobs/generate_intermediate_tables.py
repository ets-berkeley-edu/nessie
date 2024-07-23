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
from nessie.lib.berkeley import current_term_id
from nessie.lib.util import resolve_sql_template

"""Logic for intermediate table generation job."""


class GenerateIntermediateTables(BackgroundJob):

    def run(self):
        app.logger.info('Starting intermediate table generation job...')

        resolved_ddl_redshift = resolve_sql_template(
            'create_intermediate_schema_cd2.template.sql',
            current_term_id=current_term_id(),
            where_clause_exclude_withdrawn="AND en.enrollment_status_reason <> 'WDRW'",
        )
        if redshift.execute_ddl_script(resolved_ddl_redshift):
            app.logger.info('Redshift tables generated.')
        else:
            raise BackgroundJobError('Intermediate table creation job failed.')

        resolved_ddl_rds = resolve_sql_template('update_rds_indexes_sis.template.sql')
        if rds.execute(resolved_ddl_rds):
            app.logger.info('RDS indexes updated.')
        else:
            raise BackgroundJobError('Failed to update RDS indexes for intermediate schema.')

        return 'Intermediate table generation job completed.'
