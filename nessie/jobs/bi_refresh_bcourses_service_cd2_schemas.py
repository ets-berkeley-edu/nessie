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

from flask import current_app as app

from nessie.externals import rds, redshift
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.util import resolve_sql_template

"""Logic for BI Reports bCourses Service (CD2) Redshift and RDS schemas refresh job."""


class BiRefreshBcoursesServiceCd2Schemas(BackgroundJob):

    def run(self):
        app.logger.info('Starting BI Reports bCourses Service CD2 Redshift and RDS schemas refresh job...')
        app.logger.info('Executing SQL...')

        resolved_ddl_redshift = resolve_sql_template('bi_create_bcourses_service_cd2_redshift_schema.template.sql')
        if redshift.execute_ddl_script(resolved_ddl_redshift):
            app.logger.info('BI Reports bCourses Service CD2 Redshift schema refreshed.')

            resolved_ddl_rds = resolve_sql_template('bi_create_bcourses_service_cd2_rds_schema.template.sql')
            bi_rds_uri_la_reports = app.config['BI_RDS_URI_LA_REPORTS']
            if rds.execute(resolved_ddl_rds, rds_uri=bi_rds_uri_la_reports):
                app.logger.info('BI Reports bCourses Service CD2 RDS schema refreshed.')
            else:
                raise BackgroundJobError('Failed to refresh BI Reports BOA Advising Notes RDS schema.')
        else:
            raise BackgroundJobError('Failed to refresh BI Reports BOA Advising Notes Redshift schema.')

        return 'BI Reports BOA Advising Notes Redshift and RDS schemas refresh job completed.'
