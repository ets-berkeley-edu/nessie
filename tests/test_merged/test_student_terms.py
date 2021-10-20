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

from nessie.merged.student_terms import merge_enrollment
import pytest


# TODO fix integration with legacy GenerateMergedStudentFeeds structure
def generate_student_term_maps(advisees_by_sid):
    pass


@pytest.mark.skip(reason='Tests need to be rewritten against the now-testable GenerateMergedStudentFeeds job.')
class TestMergedSisEnrollments:
    oski_sid = '11667051'
    oski_canvas_id = '9000100'
    advisees_by_canvas_id = {oski_canvas_id: {'sid': oski_sid}}
    advisees_by_sid = {oski_sid: {'canvas_user_id': oski_canvas_id}}

    def test_merges_drops(self, app, student_tables):
        terms_feed, canvas_site_feed = generate_student_term_maps(self.advisees_by_sid)
        drops = terms_feed['2178'][self.oski_sid]['droppedSections']
        assert 2 == len(drops)
        assert drops[0] == {
            'component': 'STD',
            'displayName': 'ENV,RES C9',
            'dropDate': None,
            'instructionMode': 'P',
            'sectionNumber': '001',
            'withdrawAfterDeadline': True,
        }
        assert drops[1] == {
            'component': 'LEC',
            'displayName': 'HISTORY 10CH',
            'dropDate': '2021-07-29',
            'instructionMode': 'P',
            'sectionNumber': '003',
            'withdrawAfterDeadline': False,
        }
        enrollments = terms_feed['2178'][self.oski_sid]['enrollments']
        assert any(enr['displayName'] == 'ENV,RES C9' for enr in enrollments) is False

    def test_includes_midterm_grades(self, app, student_tables):
        terms_feed, canvas_site_feed = generate_student_term_maps(self.advisees_by_sid)
        term_feed = terms_feed['2178'][self.oski_sid]
        assert '2178' == term_feed['termId']
        enrollments = term_feed['enrollments']
        assert 4 == len(enrollments)
        assert 'D-' == enrollments[0]['midtermGrade']
        assert 'BURMESE 1A' == enrollments[0]['displayName']
        assert 90100 == enrollments[0]['sections'][0]['ccn']

    def test_includes_instruction_modes(self, app, student_tables):
        terms_feed, canvas_site_feed = generate_student_term_maps(self.advisees_by_sid)
        term_feed = terms_feed['2178'][self.oski_sid]
        assert term_feed['enrollments']
        for enrollment in term_feed['enrollments']:
            assert enrollment['sections']
            for section in enrollment['sections']:
                if section['ccn'] == 90200:
                    assert 'H' == section['instructionMode']
                elif section['ccn'] == 90399:
                    assert 'W' == section['instructionMode']
                else:
                    assert 'P' == section['instructionMode']

    def test_includes_course_site_section_mappings(self, app):
        """Maps Canvas sites to SIS courses and sections."""
        terms_feed, canvas_site_feed = generate_student_term_maps(self.advisees_by_sid)
        term_feed = terms_feed['2178'][self.oski_sid]
        enrollments = term_feed['enrollments']
        assert len(enrollments[0]['canvasSites']) == 1
        assert enrollments[0]['canvasSites'][0]['canvasCourseId'] == 7654320
        assert enrollments[0]['sections'][0]['canvasCourseIds'] == [7654320]
        assert len(enrollments[1]['canvasSites']) == 2
        assert enrollments[1]['canvasSites'][0]['canvasCourseId'] == 7654321
        assert enrollments[1]['canvasSites'][1]['canvasCourseId'] == 7654330
        assert enrollments[1]['sections'][0]['canvasCourseIds'] == [7654321, 7654330]
        assert len(enrollments[2]['canvasSites']) == 2
        assert enrollments[2]['canvasSites'][0]['canvasCourseId'] == 7654323
        assert enrollments[2]['canvasSites'][1]['canvasCourseId'] == 7654330
        assert (enrollments[2]['sections'][0]['canvasCourseIds']) == [7654323, 7654330]
        assert (enrollments[2]['sections'][1]['canvasCourseIds']) == [7654330]

    def test_reasonable_precision(self, app):
        enrollments = [
            {
                'grade': 'A',
                'grade_midterm': None,
                'grading_basis': 'GRD',
                'ldap_uid': '1234567',
                'sis_course_name': 'HIB 34-35',
                'sis_course_title': 'GIBBON',
                'sis_enrollment_status': 'E',
                'sis_instruction_format': 'LEC',
                'sis_instruction_mode': 'P',
                'sis_primary': True,
                'sis_section_id': 123,
                'sis_section_num': '1',
                'sis_term_id': 2182,
                'units': 3.3,
            },
            {
                'grade': 'B',
                'grade_midterm': None,
                'grading_basis': 'GRD',
                'ldap_uid': '1234567',
                'sis_course_name': 'HIB 45-35',
                'sis_course_title': 'HUME',
                'sis_enrollment_status': 'E',
                'sis_instruction_format': 'LEC',
                'sis_instruction_mode': 'P',
                'sis_primary': True,
                'sis_section_id': 234,
                'sis_section_num': '1',
                'sis_term_id': 2182,
                'units': 3.3,
            },
            {
                'grade': 'A-',
                'grade_midterm': None,
                'grading_basis': 'GRD',
                'ldap_uid': '1234567',
                'sis_course_name': 'HIB 22-35',
                'sis_course_title': 'BURNEY',
                'sis_enrollment_status': 'E',
                'sis_instruction_format': 'LEC',
                'sis_instruction_mode': 'P',
                'sis_primary': True,
                'sis_section_id': 345,
                'sis_section_num': '1',
                'sis_term_id': 2182,
                'units': 3.3,
            },
            {
                'grade': 'A',
                'grade_midterm': None,
                'grading_basis': 'GRD',
                'ldap_uid': '1234567',
                'sis_course_name': 'HIB 34-35',
                'sis_course_title': 'BOZ',
                'sis_enrollment_status': 'E',
                'sis_instruction_format': 'LEC',
                'sis_instruction_mode': 'P',
                'sis_primary': True,
                'sis_section_id': 456,
                'sis_section_num': '1',
                'sis_term_id': 2182,
                'units': 1.7,
            },
            {
                'grade': 'P',
                'grade_midterm': None,
                'grading_basis': 'EPN',
                'ldap_uid': '1234567',
                'sis_course_name': 'HIB 66-35',
                'sis_course_title': 'SMART',
                'sis_enrollment_status': 'E',
                'sis_instruction_format': 'LEC',
                'sis_instruction_mode': 'P',
                'sis_primary': True,
                'sis_section_id': 567,
                'sis_section_num': '1',
                'sis_term_id': 2182,
                'units': 3.3,
            },
        ]
        parsed = merge_enrollment(enrollments, '2182', 'Spring 2018')
        assert str(parsed['enrolledUnits']) == '14.9'
