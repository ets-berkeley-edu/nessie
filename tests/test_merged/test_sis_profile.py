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

import json

from nessie.merged.sis_profile import parse_merged_sis_profile
import pytest
from tests.util import mock_s3


@pytest.fixture()
def sis_api_profiles(app, student_tables):
    from nessie.externals import redshift
    from nessie.jobs.import_sis_student_api import ImportSisStudentApi
    with mock_s3(app):
        ImportSisStudentApi().run_wrapped()
    sql = f"""SELECT sid, feed FROM student_test.sis_api_profiles"""
    return redshift.fetch(sql)


@pytest.fixture()
def sis_api_degree_progress(app, student_tables):
    from nessie.externals import redshift
    sql = f"""SELECT sid, feed FROM student_test.sis_api_degree_progress"""
    return redshift.fetch(sql)


@pytest.fixture()
def sis_api_last_registrations(app, student_tables):
    from nessie.externals import redshift
    from nessie.jobs.import_term_gpas import ImportTermGpas
    with mock_s3(app):
        ImportTermGpas().run_wrapped()
    sql = f"""SELECT sid, feed FROM student_test.student_last_registrations"""
    return redshift.fetch(sql)


def merged_profile(sid, profile_rows, degree_progress_rows, last_registration_rows):
    profile_feed = next((r['feed'] for r in profile_rows if r['sid'] == sid), None)
    progress_feed = next((r['feed'] for r in degree_progress_rows if r['sid'] == sid), None)
    last_registration_feed = next((r['feed'] for r in last_registration_rows if r['sid'] == sid), None)
    return parse_merged_sis_profile(profile_feed, progress_feed, last_registration_feed)


class TestMergedSisProfile:
    """Test merged SIS profile."""

    def test_skips_concurrent_academic_status(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        """Skips concurrent academic status if another academic status exists."""
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['academicCareer'] == 'UGRD'

    def test_falls_back_on_concurrent_academic_status(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        """Selects concurrent academic status if no other academic status exists."""
        profile = merged_profile('1234567890', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['academicCareer'] == 'UCBX'

    def test_withdrawal_cancel_ignored_if_empty(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert 'withdrawalCancel' not in profile

    def test_withdrawal_cancel_included_if_present(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('2345678901', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['withdrawalCancel']['description'] == 'Withdrew'
        assert profile['withdrawalCancel']['reason'] == 'Personal'
        assert profile['withdrawalCancel']['date'] == '2017-10-31'

    def test_degree_progress(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['degreeProgress']['reportDate'] == '2017-03-03'
        assert len(profile['degreeProgress']['requirements']) == 4
        assert profile['degreeProgress']['requirements'][0] == {'entryLevelWriting': {'status': 'Satisfied'}}

    def test_no_holds(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['holds'] == []

    def test_multiple_holds(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('2345678901', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        holds = profile['holds']
        assert len(holds) == 2
        assert holds[0]['reason']['code'] == 'CSBAL'
        assert holds[1]['reason']['code'] == 'ADVHD'

    def test_current_term(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['currentRegistration']['term']['id'] == '2178'
        assert profile['level'] == 'Junior'
        assert profile['currentTerm']['unitsMaxOverride'] == 24
        assert profile['currentTerm']['unitsMinOverride'] == 15

    def test_zero_gpa_when_gpa_units(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        for row in sis_api_profiles:
            if row['sid'] == '11667051':
                feed = json.loads(row['feed'], strict=False)
                feed['academicStatuses'][1]['cumulativeGPA']['average'] = 0
                row['feed'] = json.dumps(feed)
                break
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['cumulativeGPA'] == 0

    def test_null_gpa_when_no_gpa_units(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        for row in sis_api_profiles:
            if row['sid'] == '11667051':
                feed = json.loads(row['feed'], strict=False)
                feed['academicStatuses'][1]['cumulativeGPA']['average'] = 0
                feed['academicStatuses'][1]['cumulativeUnits'][1]['unitsTaken'] = 0
                row['feed'] = json.dumps(feed)
                break
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['cumulativeGPA'] is None

    def test_expected_graduation_term(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['expectedGraduationTerm']['id'] == '2198'
        assert profile['expectedGraduationTerm']['name'] == 'Fall 2019'

    def test_transfer_true_if_notation_present(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('2345678901', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['transfer'] is True

    def test_transfer_false_if_notation_not_present(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['transfer'] is False

    def test_no_registrations_in_list(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        """Falls back to last term-with-units if the student is not active in the current term."""
        profile = merged_profile('1234567890', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['currentRegistration']['term']['id'] == '2172'
        assert profile['level'] == 'Sophomore'
