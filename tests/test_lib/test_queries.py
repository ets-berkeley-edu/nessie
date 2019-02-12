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

import io

from nessie.lib import queries
from nessie.lib.mockingdata import MockRows, register_mock


class TestQueries:

    def test_canvas_course_scores_fixture(self, app):
        results = queries.get_all_enrollments_in_advisee_canvas_sites()
        assert len(results) > 0
        assert {
            'canvas_course_id': 7654321, 'uid': '9000100', 'canvas_user_id': 9000100,
            'current_score': 84, 'last_activity_at': 1535275620, 'sis_enrollment_status': 'E',
        } in results

    def test_sis_sections_in_canvas_course(self, app):
        sections = queries.get_advisee_enrolled_canvas_sites()

        burmese_sections = next(s['sis_section_ids'] for s in sections if s['canvas_course_id'] == 7654320)
        assert burmese_sections == '90100,90101'

        medieval_sections = next(s['sis_section_ids'] for s in sections if s['canvas_course_id'] == 7654321)
        assert medieval_sections == '90200'

        nuclear_sections = next(s['sis_section_ids'] for s in sections if s['canvas_course_id'] == 7654323)
        assert nuclear_sections == '90299,90300'

        # No SIS-linked site sections
        project_site_sections = next(s['sis_section_ids'] for s in sections if s['canvas_course_id'] == 9999991)
        assert project_site_sections is None

    def test_sis_enrollments(self, app):
        enrollments = queries.get_all_advisee_sis_enrollments()
        assert len(enrollments) == 10

        for enr in enrollments:
            assert enr['ldap_uid'] == '61889'
            assert enr['sid'] == '11667051'

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
        assert enrollments[6]['sis_section_num'] == '002'
        assert enrollments[6]['sis_enrollment_status'] == 'E'
        assert enrollments[6]['units'] == 3
        assert enrollments[6]['grading_basis'] == 'PNP'
        assert enrollments[6]['grade'] == 'P'

        assert enrollments[7]['sis_course_name'] == 'NUC ENG 124'
        assert enrollments[7]['sis_section_num'] == '201'
        assert enrollments[7]['sis_enrollment_status'] == 'E'
        assert enrollments[7]['units'] == 0
        assert enrollments[7]['grading_basis'] == 'NON'
        assert not enrollments[7]['grade']

        assert enrollments[8]['sis_course_name'] == 'PHYSED 11'
        assert enrollments[8]['sis_section_num'] == '001'
        assert enrollments[8]['sis_enrollment_status'] == 'E'
        assert enrollments[8]['units'] == 0.5
        assert enrollments[8]['grading_basis'] == 'PNP'
        assert enrollments[8]['grade'] == 'P'

    def test_student_canvas_courses(self, app):
        courses = queries.get_advisee_enrolled_canvas_sites()
        assert len(courses) == 6
        # Canvas sites should be sorted by Course ID number
        assert courses[0]['canvas_course_id'] == 7654320
        assert courses[0]['canvas_course_name'] == 'Introductory Burmese'
        assert courses[0]['canvas_course_code'] == 'BURMESE 1A'
        assert courses[0]['canvas_course_term'] == 'Fall 2017'
        assert courses[1]['canvas_course_id'] == 7654321
        assert courses[1]['canvas_course_name'] == 'Medieval Manuscripts as Primary Sources'
        assert courses[1]['canvas_course_code'] == 'MED ST 205'
        assert courses[1]['canvas_course_term'] == 'Fall 2017'
        assert courses[2]['canvas_course_id'] == 7654323
        assert courses[2]['canvas_course_name'] == 'Radioactive Waste Management'
        assert courses[2]['canvas_course_code'] == 'NUC ENG 124'
        assert courses[2]['canvas_course_term'] == 'Fall 2017'
        assert courses[3]['canvas_course_id'] == 7654325
        assert courses[3]['canvas_course_name'] == 'Modern Statistical Prediction and Machine Learning'
        assert courses[3]['canvas_course_code'] == 'STAT 154'
        assert courses[3]['canvas_course_term'] == 'Spring 2017'
        assert courses[4]['canvas_course_id'] == 7654330
        assert courses[4]['canvas_course_name'] == 'Optional Friday Night Radioactivity Group'
        assert courses[4]['canvas_course_code'] == 'NUC ENG 124'
        assert courses[4]['canvas_course_term'] == 'Fall 2017'

    def test_submissions_turned_in_relative_to_user_fixture(self, app):
        data = queries.get_advisee_submissions_sorted()
        assert len(data) > 0
        assert {
            'reference_user_id': 9000100,
            'sid': '9000100',
            'canvas_course_id': 7654321,
            'canvas_user_id': 9000100,
            'submissions_turned_in': 8,
        } in data

    def test_override_fixture(self, app):
        mr = MockRows(io.StringIO('course_id,uid,canvas_user_id,current_score,last_activity_at,sis_enrollment_status\n1,2,3,4,5,F'))
        with register_mock(queries.get_all_enrollments_in_advisee_canvas_sites, mr):
            data = queries.get_all_enrollments_in_advisee_canvas_sites()
        assert len(data) == 1
        assert {
            'course_id': 1, 'uid': '2', 'canvas_user_id': 3, 'current_score': 4, 'last_activity_at': 5,
            'sis_enrollment_status': 'F',
        } == data[0]

    def test_user_for_uid(self, app):
        data = queries.get_advisee_student_profile_feeds()
        oliver = next(r for r in data if r['ldap_uid'] == '2040')
        assert oliver['canvas_user_id'] == 10001
        assert oliver['canvas_user_name'] == 'Oliver Heyer'
        paulk = next(r for r in data if r['ldap_uid'] == '242881')
        assert paulk['canvas_user_id'] == 10002
        assert paulk['canvas_user_name'] == 'Paul Kerschen'
