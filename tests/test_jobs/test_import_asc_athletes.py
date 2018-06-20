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


import json
from nessie.externals import asc_athletes_api
from nessie.jobs.import_asc_athletes import ImportAscAthletes
from nessie.lib import asc
from nessie.lib.mockingbird import MockResponse, register_mock
from nessie.models.athletics import Athletics
from nessie.models.student import Student
from tests.util import assert_background_job_status


class TestImportAscAthletes:

    def test_do_import_from_asc_fixture(self, app, metadata_db):
        status = ImportAscAthletes().run_wrapped()
        assert_background_job_status('ImportAscAthletes')
        assert status is not None
        water_polo_team = Athletics.query.filter_by(group_code='MWP').first()
        assert len(water_polo_team.athletes) == 1
        football_backs = Athletics.query.filter_by(group_code='MFB-DB').first()
        assert len(football_backs.athletes) == 1
        # John has been recruited.
        assert Student.find_by_sid('8901234567').athletics[0].group_code == 'MFB-DL'
        # PaulK has dropped out.
        assert Student.find_by_sid('3456789012') is None
        # Sandeep relaxed.
        assert len(Student.find_by_sid('5678901234').athletics) == 1
        # Siegfried caught hydrophobia.
        inactive_student = Student.find_by_sid('890127492')
        assert not inactive_student.is_active_asc
        assert inactive_student.status_asc == 'Beyond Aid'
        assert inactive_student.athletics[0].group_code == 'MWP'
        assert status is not False
        assert (not status['warnings'])
        counts = status['change_counts']
        assert counts['deleted_students'] == 1
        assert counts['deleted_memberships'] == 7
        assert counts['new_memberships'] == 2
        assert counts['new_team_groups'] == 1
        assert True

    def test_update_safety_check(self, app):
        this_acad_yr = app.config['ASC_THIS_ACAD_YR']
        assert len(this_acad_yr)
        skinny_import = {
            '1166.3': {
                'SID': '5678901234',
                'AcadYr': this_acad_yr,
                'IntensiveYN': 'No',
                'SportCode': 'MTE',
                'SportCodeCore': 'MTE',
                'Sport': 'Men\'s Tennis',
                'SportCore': 'Men\'s Tennis',
                'ActiveYN': 'Yes',
                'SportStatus': 'Compete',
                'SyncDate': '2018-01-31',
            },
        }
        modified_response = MockResponse(200, {}, json.dumps(skinny_import))
        with register_mock(asc_athletes_api._get_asc_feed_response, modified_response):
            status = ImportAscAthletes().run()
            assert status is False


class TestAscAthletesApiUpdates:

    def test_feed_stashing(self, app):
        feed_date = '2018-01-31'
        assert asc.get_cached_feed(feed_date) is None
        status = ImportAscAthletes().run()
        assert status['last_sync_date'] is None
        assert status['this_sync_date'] == feed_date
        api_results_count = status['api_results_count']
        assert api_results_count == 9
        stashed = asc.get_cached_feed(feed_date)
        assert len(stashed) == 9
        assert stashed[0]['SyncDate'] == feed_date

    def test_last_sync_date(self, app):
        first_date = '2018-01-21'
        asc.confirm_sync(first_date)
        status = ImportAscAthletes().run()
        assert status['last_sync_date'] == first_date
        assert status['this_sync_date'] == '2018-01-31'

    def test_consistency_check(self, app):
        bad_date = '"It\'s not you, it\'s me"'
        with open(app.config['BASE_DIR'] + '/fixtures/asc_athletes.json') as file:
            modified_response_body = file.read().replace('"2018-01-31"', bad_date, 1)
            modified_response = MockResponse(200, {}, modified_response_body)
            with register_mock(asc_athletes_api._get_asc_feed_response, modified_response):
                assert ImportAscAthletes().run() is False

    def test_repeat_date_not_stashed(self, app):
        feed_date = '2018-01-31'
        asc.confirm_sync(feed_date)
        ImportAscAthletes().run()
        assert asc.get_cached_feed(feed_date) is None
