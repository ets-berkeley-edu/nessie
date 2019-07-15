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

import nessie.externals.sis_student_api as student_api
from nessie.lib.berkeley import current_term_id
from nessie.lib.mockingbird import MockResponse, register_mock
import pytest


TEST_SID_LIST = ['11667051', '1234567890', '2345678901']


class TestSisStudentApi:
    """SIS student API query."""

    def test_get_student(self, app):
        """Returns unwrapped data."""
        results = student_api.get_sis_students_list(TEST_SID_LIST)
        students = results['all_feeds']
        assert len(students) == len(TEST_SID_LIST)
        student = students[0]
        assert len(student['academicStatuses']) == 2
        assert student['academicStatuses'][0]['studentCareer']['academicCareer']['code'] == 'UCBX'
        assert student['academicStatuses'][1]['studentCareer']['academicCareer']['code'] == 'UGRD'
        assert student['academicStatuses'][1]['cumulativeGPA']['average'] == pytest.approx(3.8, 0.01)
        assert student['academicStatuses'][1]['studentPlans'][0]['academicPlan']['plan']['description'] == 'English BA'
        assert student['academicStatuses'][1]['termsInAttendance'] == 5
        assert student['registrations'][0]['academicLevels'][0]['type']['code'] == 'BOT'
        assert student['registrations'][0]['academicLevels'][0]['level']['description'] == 'Junior'
        assert student['registrations'][0]['athlete'] is True
        assert student['registrations'][0]['termUnits'][0]['unitsMax'] == 24
        assert student['registrations'][0]['termUnits'][0]['unitsMin'] == 15
        assert student['emails'][0]['emailAddress'] == 'oski@berkeley.edu'
        assert len(results['missing_sids']) == 0
        assert results['ucbx_only_sids'] == ['1234567890']

    def test_inner_get_students(self, app):
        """Returns fixture data."""
        oski_response = student_api._get_v2_by_sids_list(TEST_SID_LIST, term_id=current_term_id(), with_registration=True)
        assert oski_response
        assert oski_response.status_code == 200
        students = oski_response.json()['apiResponse']['response']['students']
        assert len(students) == 3

    def test_get_term_gpas_registration(self, app):
        reg_feed = student_api.get_term_gpas_registration(11667051)
        gpas = reg_feed['term_gpas']
        assert len(gpas) == 7
        assert gpas['2148']['gpa'] == 3.3
        assert gpas['2158']['unitsTakenForGpa'] > 0
        assert gpas['2152']['gpa'] == 4.0
        assert gpas['2152']['unitsTakenForGpa'] > 0
        assert gpas['2155']['gpa'] == 0.0
        assert gpas['2155']['unitsTakenForGpa'] == 0
        assert gpas['2168']['gpa'] == 3.0
        assert gpas['2168']['unitsTakenForGpa'] > 0
        last_registration = reg_feed['last_registration']
        assert last_registration['term']['id'] == '2172'
        assert last_registration['academicCareer']['code'] == 'UGRD'
        assert len(last_registration['academicLevels']) == 2
        assert last_registration['academicLevels'][0]['type']['code'] == 'BOT'
        assert len(last_registration['termUnits']) == 3
        assert last_registration['termGPA']['average'] == 3.3

    def test_inner_get_registrations(self, app):
        oski_response = student_api._get_v2_registrations(11667051)
        assert oski_response
        assert oski_response.status_code == 200
        registrations = oski_response.json()['apiResponse']['response']['registrations']
        assert len(registrations) == 10

    def test_server_error(self, app, caplog):
        """Logs unexpected server errors and returns informative message."""
        api_error = MockResponse(500, {}, '{"message": "Internal server error."}')
        with register_mock(student_api._get_v2_registrations, api_error):
            response = student_api._get_v2_registrations(11667051)
            assert '500 Server Error' in caplog.text
            assert not response
            assert response.raw_response.status_code == 500
            assert response.raw_response.json()['message']
