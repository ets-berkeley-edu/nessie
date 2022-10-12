"""
Copyright ©2022. The Regents of the University of California (Regents). All Rights Reserved.

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

from nessie.externals import redshift
from nessie.lib.queries import edl_schema
import pytest
from tests.util import mock_s3


class TestCreateEdlSchema:

    @pytest.mark.skip
    def test_generate_demographics_feeds(self, app, student_tables):
        """Builds JSON feeds and uploads to S3."""
        from nessie.jobs.create_edl_schema import DemographicsFeedBuilder
        with mock_s3(app):
            DemographicsFeedBuilder().build()

        rows = redshift.fetch(f'SELECT * FROM {edl_schema()}.student_demographics')
        assert len(rows) == 11
        assert rows[0]['sid'] == '11667051'
        feed = json.loads(rows[0]['feed'])
        assert feed['gender'] == 'Female'
        assert feed['ethnicities'] == ['African-American / Black', 'Chinese / Chinese-American', 'East Indian / Pakistani']
        assert feed['nationalities'] == ['Singapore']
        assert feed['underrepresented'] is True
        assert feed['visa']['type'] == 'PR'
        assert feed['visa']['status'] == 'A'

        assert rows[1]['sid'] == '1234567890'
        feed = json.loads(rows[1]['feed'])
        assert feed['gender'] == 'Male'
        assert feed['ethnicities'] == ['Mexican / Mexican-American / Chicano', 'White']
        assert feed['nationalities'] == ['Iran (Islamic Republic Of)']
        assert feed['underrepresented'] is True
        assert feed['visa']['type'] == 'F1'
        assert feed['visa']['status'] == 'A'

        assert rows[2]['sid'] == '2345678901'
        feed = json.loads(rows[2]['feed'])
        assert feed['gender'] == 'Female'
        assert feed['ethnicities'] == ['White']
        assert feed['nationalities'] == ['Taiwan']
        assert feed['underrepresented'] is False
        assert feed['visa'] is None

        assert rows[3]['sid'] == '3456789012'
        feed = json.loads(rows[3]['feed'])
        assert feed['gender'] == 'Decline to State'
        assert feed['ethnicities'] == ['American Indian / Alaska Native', 'Filipino / Filipino-American']
        assert feed['nationalities'] == ['Korea, Republic of']
        assert feed['underrepresented'] is True
        assert feed['visa']['type'] == 'J1'
        assert feed['visa']['status'] == 'G'

        assert rows[4]['sid'] == '5000000000'
        feed = json.loads(rows[4]['feed'])
        assert feed['gender'] == 'Female'
        assert feed['ethnicities'] == ['Not Specified']
        assert feed['nationalities'] == []
        assert feed['underrepresented'] is False
        assert feed['visa'] is None

        assert rows[7]['sid'] == '8901234567'
        feed = json.loads(rows[7]['feed'])
        assert feed['gender'] == 'Decline to State'
        assert feed['ethnicities'] == ['Not Specified']
        assert feed['nationalities'] == []
        assert feed['underrepresented'] is False
        assert feed['visa'] is None

        assert rows[9]['sid'] == '9000000000'
        feed = json.loads(rows[9]['feed'])
        assert feed['gender'] == 'Different Identity'
        assert feed['ethnicities'] == ['African-American / Black', 'Other Asian', 'Pacific Islander']
        assert feed['nationalities'] == ["Lao People's Democratic Rep", 'Saint Kitts and Nevis']
        assert feed['underrepresented'] is True
        assert feed['visa'] is None
