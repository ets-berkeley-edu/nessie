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

from nessie.externals import canvas_data
from nessie.lib.mockingbird import MockResponse, register_mock
from tests.util import capture_app_logs


class TestCanvasData:
    """Canvas Data API client."""

    def test_get_snapshots(self, app):
        """Returns fixture data."""
        response = canvas_data.get_snapshots()
        assert response['incomplete'] is False
        assert response['schemaVersion'] == '2.0.0'
        assert len(response['files']) == 348
        assert response['files'][0]['filename'] == 'account_dim-00000-5eb7ee9e.gz'
        assert response['files'][0]['table'] == 'account_dim'
        assert response['files'][0]['partial'] is False
        assert response['files'][0]['url'].startswith('https://hosted-data-work.s3.amazonaws.com/20180320T160000.415/dw_split/12345600000054321')

    def test_server_error(self, app, caplog):
        """Logs unexpected server errors."""
        with capture_app_logs(app):
            canvas_error = MockResponse(429, {}, '{"message": "Easy, tiger."}')
            with register_mock(canvas_data.get_snapshots, canvas_error):
                response = canvas_data.get_snapshots()
                assert '429 Client Error: Too Many Requests' in caplog.text
                assert not response
