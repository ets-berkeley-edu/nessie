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

from nessie.externals import rds
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.util import resolve_sql_template

"""Logic for BOA App RDS Data Advising Notes schema and index creation job."""


class CreateBardAdvisingNotesSchema(BackgroundJob):

    def run(self):
        app.logger.info('Starting BOA App RDS Data Advising Notes schema and index creation job...')
        app.logger.info('Executing SQL...')

        rds_template = 'create_bard_advising_notes_schema.template.sql'
        resolved_ddl_rds = resolve_sql_template(rds_template)

        if rds.execute(resolved_ddl_rds):
            app.logger.info('Created BOA App RDS Data Advising Notes RDS schema and indexes.')
        else:
            raise BackgroundJobError('BOA App RDS Data Advising Notes RDS schema and index creation job failed.')

        return 'BOA App RDS Data Advising Notes RDS schema and index creation job completed.'
