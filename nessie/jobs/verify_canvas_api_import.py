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

from flask import current_app as app
from nessie.externals import redshift
from nessie.jobs.background_job import BackgroundJob

"""Logic for Canvas API schema verification job."""


class VerifyCanvasApiImport(BackgroundJob):

    def run(self):
        app.logger.info('Starting Canvas API import verification job...')
        expected_courses = self.get_expected_courses()
        gradebook_history_result = self.verify_import('gradebook_history', self.get_imported_gradebook_history(), expected_courses)
        grade_change_log_result = self.verify_import('grade_change_log', self.get_imported_grade_change_log(), expected_courses)

        return f"""Verified Canvas API imports for {len(expected_courses)} courses.
                {gradebook_history_result}
                {grade_change_log_result}"""

    def get_expected_courses(self):
        schema = app.config['REDSHIFT_SCHEMA_CANVAS']
        results = redshift.fetch(
            f"""SELECT course.canvas_id AS course_id, count(assign.id) AS assignment_count
            FROM {schema}.enrollment_term_dim term
            JOIN {schema}.course_dim course ON term.id = course.enrollment_term_id
            LEFT OUTER JOIN {schema}.assignment_dim assign ON course.id = assign.course_id
            WHERE term.name IN ('Spring 2020')
            GROUP BY course.canvas_id
            ORDER BY course.canvas_id""",
        )
        return {r['course_id']: r['assignment_count'] for r in results}

    def verify_import(self, table_name, imported_courses, expected_courses):
        app.logger.info(f'Verifying {table_name} import for {len(expected_courses)} courses.')
        result = {
            'success': [],
            'fail': [],
            'no_data': [],
        }
        for course_id, expected_assignment_count in expected_courses.items():
            imported_assignment_count = imported_courses.get(course_id)
            if expected_assignment_count == 0:
                result['no_data'].append(str(course_id))
            elif imported_assignment_count == expected_assignment_count:
                result['success'].append(str(course_id))
            else:
                result['fail'].append(str(course_id))

        # TODO: verify submission counts

        app.logger.info(f"""Verified {table_name} import is complete for {len(result['success'])} of {len(expected_courses)} courses.
                        {len(result['no_data'])} courses have no graded assignments.
                        Import failed for {len(result['fail'])} courses.""")
        if len(result['fail']):
            failed_course_ids = ','.join(result['fail'])
            app.logger.warn(f'Failed to import {table_name} for course IDs {failed_course_ids}')
        return {table_name: {status: len(courses) for status, courses in result.items()}}

    def get_imported_gradebook_history(self):
        return self.get_imported(
            """SELECT course_id, count(distinct assignment_id) AS assignment_count
                FROM {}.gradebook_history
                GROUP BY course_id""",
        )

    def get_imported_grade_change_log(self):
        return self.get_imported(
            """SELECT g.course_id, count(distinct l.assignment) AS assignment_count
                FROM {}.grade_change_log g, g.links l
                GROUP BY g.course_id""",
        )

    def get_imported(self, query):
        schema = app.config['REDSHIFT_SCHEMA_CANVAS_API']
        results = redshift.fetch(query.format(schema))
        return {r['course_id']: r['assignment_count'] for r in results}
