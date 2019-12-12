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

import json

from nessie.externals import asc_athletes_api
from nessie.jobs.background_job import BackgroundJobError
from nessie.jobs.import_asc_athletes import ImportAscAthletes
from nessie.lib.mockingbird import MockResponse, register_mock
import pytest


class TestImportAscAthletes:

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
            with pytest.raises(BackgroundJobError):
                ImportAscAthletes().run()


class TestAscAthletesApiUpdates:

    def test_consistency_check(self, app):
        bad_date = '"It\'s not you, it\'s me"'
        with open(app.config['BASE_DIR'] + '/fixtures/asc_athletes.json') as file:
            modified_response_body = file.read().replace('"2018-01-31"', bad_date, 1)
            modified_response = MockResponse(200, {}, modified_response_body)
            with register_mock(asc_athletes_api._get_asc_feed_response, modified_response):
                with pytest.raises(BackgroundJobError):
                    ImportAscAthletes().run()
