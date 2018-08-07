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


import io

from nessie.lib import queries
from nessie.lib.mockingdata import MockRows, register_mock


class TestQueries:

    def test_canvas_course_scores_fixture(self, app):
        results = queries.get_canvas_course_scores(['7654321'])
        assert len(results) > 0
        assert {'course_id': 7654321, 'canvas_user_id': 9000100, 'current_score': 84, 'last_activity_at': 1535275620} in results

    def test_sis_sections_in_canvas_course(self, app):
        sections = queries.get_sis_sections_for_canvas_courses([7654320, 7654321, 7654323, 9999991])

        burmese_sections = [s for s in sections if s['canvas_course_id'] == 7654320]
        assert len(burmese_sections) == 2
        assert burmese_sections[0]['sis_section_id'] == 90100
        assert burmese_sections[1]['sis_section_id'] == 90101

        medieval_sections = [s for s in sections if s['canvas_course_id'] == 7654321]
        assert len(medieval_sections) == 1
        assert medieval_sections[0]['sis_section_id'] == 90200

        nuclear_sections = [s for s in sections if s['canvas_course_id'] == 7654323]
        assert len(nuclear_sections) == 2
        assert nuclear_sections[0]['sis_section_id'] == 90299
        assert nuclear_sections[1]['sis_section_id'] == 90300

        # No SIS-linked site sections
        project_site_sections = [s for s in sections if s['canvas_course_id'] == 9999991]
        assert len(project_site_sections) == 1
        assert project_site_sections[0]['sis_section_id'] is None

    def test_sis_enrollments(self, app):
        enrollments = queries.get_sis_enrollments(61889)
        assert len(enrollments) == 9

        assert enrollments[4]['sis_course_name'] == 'BURMESE 1A'
        assert enrollments[4]['sis_section_num'] == '001'
        assert enrollments[4]['sis_enrollment_status'] == 'E'
        assert enrollments[4]['units'] == 4
        assert enrollments[4]['grading_basis'] == 'GRD'

        assert enrollments[5]['sis_course_name'] == 'MED ST 205'
        assert enrollments[5]['sis_section_num'] == '001'
        assert enrollments[5]['sis_enrollment_status'] == 'E'
        assert enrollments[5]['units'] == 5
        assert enrollments[5]['grading_basis'] == 'GRD'

        assert enrollments[6]['sis_course_name'] == 'NUC ENG 124'
        assert enrollments[6]['sis_section_num'] == '201'
        assert enrollments[6]['sis_enrollment_status'] == 'E'
        assert enrollments[6]['units'] == 0
        assert enrollments[6]['grading_basis'] == 'NON'
        assert not enrollments[6]['grade']

        assert enrollments[7]['sis_course_name'] == 'NUC ENG 124'
        assert enrollments[7]['sis_section_num'] == '002'
        assert enrollments[7]['sis_enrollment_status'] == 'E'
        assert enrollments[7]['units'] == 3
        assert enrollments[7]['grading_basis'] == 'PNP'
        assert enrollments[7]['grade'] == 'P'

        assert enrollments[8]['sis_course_name'] == 'PHYSED 11'
        assert enrollments[8]['sis_section_num'] == '001'
        assert enrollments[8]['sis_enrollment_status'] == 'E'
        assert enrollments[8]['units'] == 0.5
        assert enrollments[8]['grading_basis'] == 'PNP'
        assert enrollments[8]['grade'] == 'P'

    def test_student_canvas_courses(self, app):
        courses = queries.get_student_canvas_courses(61889)
        assert len(courses) == 6
        assert courses[0]['canvas_course_id'] == 7654320
        assert courses[0]['canvas_course_name'] == 'Introductory Burmese'
        assert courses[0]['canvas_course_code'] == 'BURMESE 1A'
        assert courses[0]['canvas_course_term'] == 'Fall 2017'
        assert courses[1]['canvas_course_id'] == 7654321
        assert courses[1]['canvas_course_name'] == 'Medieval Manuscripts as Primary Sources'
        assert courses[1]['canvas_course_code'] == 'MED ST 205'
        assert courses[1]['canvas_course_term'] == 'Fall 2017'
        assert courses[2]['canvas_course_id'] == 7654330
        assert courses[2]['canvas_course_name'] == 'Optional Friday Night Radioactivity Group'
        assert courses[2]['canvas_course_code'] == 'NUC ENG 124'
        assert courses[2]['canvas_course_term'] == 'Fall 2017'
        assert courses[3]['canvas_course_id'] == 7654323
        assert courses[3]['canvas_course_name'] == 'Radioactive Waste Management'
        assert courses[3]['canvas_course_code'] == 'NUC ENG 124'
        assert courses[3]['canvas_course_term'] == 'Fall 2017'
        assert courses[4]['canvas_course_id'] == 7654325
        assert courses[4]['canvas_course_name'] == 'Modern Statistical Prediction and Machine Learning'
        assert courses[4]['canvas_course_code'] == 'STAT 154'
        assert courses[4]['canvas_course_term'] == 'Spring 2017'

    def test_submissions_turned_in_relative_to_user_fixture(self, app):
        data = queries.get_submissions_turned_in_relative_to_user(9000100)
        assert len(data) > 0
        assert {'course_id': 7654321, 'canvas_user_id': 9000100, 'submissions_turned_in': 8} in data

    def test_override_fixture(self, app):
        mr = MockRows(io.StringIO('canvas_course_id,sis_section_id\n7654320,13131'))
        with register_mock(queries.get_sis_sections_for_canvas_courses, mr):
            data = queries.get_sis_sections_for_canvas_courses([7654320])
        assert len(data) == 1
        assert {'canvas_course_id': 7654320, 'sis_section_id': 13131} == data[0]

    def test_user_for_uid(self, app):
        data = queries.get_user_for_uid(2040)
        assert len(data) == 1
        assert {'canvas_id': 10001, 'name': 'Oliver Heyer', 'uid': '2040'} in data
        data = queries.get_user_for_uid(242881)
        assert len(data) == 1
        assert {'canvas_id': 10002, 'name': 'Paul Kerschen', 'uid': '242881'} in data
