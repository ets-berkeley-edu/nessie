"""
Copyright ©2021. The Regents of the University of California (Regents). All Rights Reserved.

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

        if app.config['FEATURE_FLAG_EDL_SIS_VIEWS']:
            sis_source_schema = app.config['REDSHIFT_SCHEMA_EDL']
            where_clause_exclude_withdrawn = "AND en.enrollment_status_reason <> 'WDRW'"
        else:
            sis_source_schema = app.config['REDSHIFT_SCHEMA_SIS']
            where_clause_exclude_withdrawn = f"""/* Enrollment with no primary section is likely a withdrawal. */
                AND EXISTS (
                    SELECT
                        en0.term_id,
                        en0.section_id,
                        en0.ldap_uid
                    FROM {app.config['REDSHIFT_SCHEMA_SIS']}.enrollments en0
                    JOIN {app.config['REDSHIFT_SCHEMA_INTERMEDIATE']}.course_sections crs0
                        ON crs0.sis_section_id = en0.section_id
                        AND crs0.sis_term_id = en0.term_id
                    WHERE en0.term_id = en.term_id
                    AND en0.ldap_uid = en.ldap_uid
                    AND crs0.sis_course_name = crs.sis_course_name
                    AND crs0.sis_primary = TRUE
                    AND en0.enrollment_status != 'D'
                    AND en0.grade != 'W'
                )"""

        resolved_ddl_redshift = resolve_sql_template(
            'create_intermediate_schema.template.sql',
            current_term_id=current_term_id(),
            redshift_schema_sis=sis_source_schema,
            where_clause_exclude_withdrawn=where_clause_exclude_withdrawn,
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
