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

import nessie.externals.sis_student_api as student_api
from nessie.lib.berkeley import current_term_id
from nessie.lib.mockingbird import MockResponse, register_mock
import pytest


TEST_SID_LIST = ['11667051', '1234567890', '2345678901']


class TestSisStudentApi:
    """SIS student API query."""

    def test_inner_get_students(self, app):
        """Returns fixture data."""
        oski_response = student_api._get_v2_by_sids_list(
            TEST_SID_LIST,
            term_id=current_term_id(),
            as_of=None,
            with_registration=True,
            with_contacts=True,
        )
        assert oski_response
        assert oski_response.status_code == 200
        students = oski_response.json()['apiResponse']['response']['students']
        assert len(students) == 3

    def test_get_term_gpas_registration_demog(self, app):
        reg_feed = student_api.get_term_gpas_registration_demog(11667051)
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

    def test_get_demog_data(self, app):
        demog_feed = student_api.get_term_gpas_registration_demog(1234567890)['demographics']
        ethn = demog_feed['ethnicities']
        assert len(ethn) == 2
        assert ethn[0]['group']['description'] == 'White'
        assert ethn[1]['detail']['description'] == 'Korean'
        assert ethn[1]['group']['description'] == 'Asian'
        foreign = demog_feed['foreignCountries']
        assert len(foreign) == 1
        assert foreign[0]['code'] == 'KOR'
        assert foreign[0]['description'] == 'Korea, Republic of'
        gender = demog_feed['gender']
        assert gender['genderOfRecord']['description'] == 'Female'
        assert gender['sexAtBirth']['description'] == 'Female'
        assert demog_feed['residency']['official']['description'] == 'Non-Resident'
        assert demog_feed['usaCountry']['citizenshipStatus']['description'] == 'Alien Temporary'
        visa = demog_feed['usaCountry']['visa']
        assert visa['status'] == 'G'
        assert visa['type']['code'] == 'F1'
        assert visa['type']['description'] == 'Student in Academic Program'

    def test_inner_get_registrations_demog(self, app):
        oski_response = student_api._get_v2_registrations_demog(11667051)
        assert oski_response
        assert oski_response.status_code == 200
        feed = oski_response.json()['apiResponse']['response']
        assert len(feed['registrations']) == 10
        assert len(feed['ethnicities']) == 2
        assert feed['gender']['sexAtBirth']['code'] == 'M'
        assert feed['gender']['genderOfRecord']['code'] == 'F'
        assert feed['gender']['genderIdentity']['code'] == 'TF'
        assert feed['residency']['official']['code'] == 'RES'
        assert feed['usaCountry']['citizenshipStatus']['description'] == 'Native'
        assert not feed['usaCountry']['visa']

    def test_server_error(self, app, caplog):
        """Logs unexpected server errors and returns informative message."""
        api_error = MockResponse(500, {}, '{"message": "Internal server error."}')
        with register_mock(student_api._get_v2_registrations_demog, api_error):
            response = student_api._get_v2_registrations_demog(11667051)
            assert '500 Server Error' in caplog.text
            assert not response
            assert response.raw_response.status_code == 500
            assert response.raw_response.json()['message']

    def test_get_v1_student(self, app):
        """Returns unwrapped data."""
        student = student_api.get_v1_student(11667051)
        assert len(student['academicStatuses']) == 2
        assert student['academicStatuses'][0]['currentRegistration']['academicCareer']['code'] == 'UCBX'
        assert student['academicStatuses'][1]['cumulativeGPA']['average'] == pytest.approx(3.8, 0.01)
        assert student['academicStatuses'][1]['currentRegistration']['academicLevel']['level']['description'] == 'Junior'
        assert student['academicStatuses'][1]['currentRegistration']['athlete'] is True
        assert student['academicStatuses'][1]['currentRegistration']['termUnits'][0]['unitsMax'] == 24
        assert student['academicStatuses'][1]['currentRegistration']['termUnits'][0]['unitsMin'] == 15
        assert student['academicStatuses'][1]['studentPlans'][0]['academicPlan']['plan']['description'] == 'English BA'
        assert student['academicStatuses'][1]['termsInAttendance'] == 5
        assert student['emails'][0]['emailAddress'] == 'oski@berkeley.edu'

    def test_inner_get_v1_student(self, app):
        """Returns fixture data."""
        oski_response = student_api._get_v1_student(11667051)
        assert oski_response
        assert oski_response.status_code == 200
        students = oski_response.json()['apiResponse']['response']['any']['students']
        assert len(students) == 1
