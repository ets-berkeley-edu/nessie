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

from datetime import datetime

from nessie.externals import ycbm_api
from nessie.lib.mockingbird import MockResponse, register_mock


class TestYcbmApi:
    """YouCanBookMe API client."""

    def test_bookings(self, app):
        """Returns course enrollments."""
        feed = ycbm_api.get_bookings_for_date(datetime.strptime('2021-06-04', '%Y-%m-%d'))
        assert feed
        assert len(feed) == 2
        assert feed[0]['title'] == 'L&S Advising Appt:  Laverne DeFazio/Lenny Kosnowski'
        assert feed[0]['teamMember']['name'] == 'Laverne DeFazio'
        assert feed[1]['title'] == 'L&S Advising Appt:  Shirley Feeney/Andrew Squiggman'
        assert feed[1]['teamMember']['name'] == 'Shirley Feeney'

    def test_server_error(self, app, caplog):
        """Logs unexpected server errors."""
        ycbm_error = MockResponse(500, {}, '{"message": "Internal server error."}')
        with register_mock(ycbm_api.get_authorized_response, ycbm_error):
            response = ycbm_api.get_bookings_for_date(datetime.strptime('2021-06-04', '%Y-%m-%d'))
            assert '500 Server Error' in caplog.text
            assert not response
