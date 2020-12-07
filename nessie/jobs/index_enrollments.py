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
from nessie.externals import rds
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.berkeley import current_term_id
from nessie.lib.queries import get_enrolled_primary_sections


"""Logic for current-term enrollments index job."""


class IndexEnrollments(BackgroundJob):

    rds_schema = app.config['RDS_SCHEMA_SIS_INTERNAL']

    def run(self, term_id=None):
        if not term_id:
            term_id = current_term_id()
        if term_id == 'all':
            app.logger.info('Starting enrollments index job for all terms...')
        else:
            app.logger.info(f'Starting enrollments index job for term {term_id}...')

        with rds.transaction() as transaction:
            if self.refresh_enrollments_index(term_id, transaction):
                transaction.commit()
                app.logger.info('Refreshed RDS indexes.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Failed to refresh RDS indexes.')

        return f'Enrollments index job completed for term {term_id}.'

    def refresh_enrollments_index(self, term_id, transaction):
        if term_id == 'all':
            section_results = get_enrolled_primary_sections()
            if not section_results:
                return False
            if not transaction.execute(f'TRUNCATE {self.rds_schema}.enrolled_primary_sections'):
                return False
        else:
            section_results = get_enrolled_primary_sections(term_id)
            if not section_results:
                return False
            if not transaction.execute(f"DELETE FROM {self.rds_schema}.enrolled_primary_sections WHERE term_id = '{term_id}'"):
                return False

        def insertable_tuple(row):
            subject_area, catalog_id = row['sis_course_name'].rsplit(' ', 1)
            subject_area_compressed = subject_area.translate({ord(c): None for c in '&-, '})
            return tuple([
                row['sis_term_id'],
                row['sis_section_id'],
                row['sis_course_name'],
                row['sis_course_name_compressed'],
                subject_area_compressed,
                catalog_id,
                row['sis_course_title'],
                row['sis_instruction_format'],
                row['sis_section_num'],
                row['instructors'],
            ])
        insert_result = transaction.insert_bulk(
            f"""INSERT INTO {self.rds_schema}.enrolled_primary_sections
                (term_id, sis_section_id, sis_course_name, sis_course_name_compressed, sis_subject_area_compressed, sis_catalog_id,
                 sis_course_title, sis_instruction_format, sis_section_num, instructors)
                VALUES %s""",
            [insertable_tuple(r) for r in section_results],
        )
        if not insert_result:
            return False
        return True
