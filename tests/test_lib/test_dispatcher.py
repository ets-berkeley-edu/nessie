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

from nessie.lib import dispatcher
from nessie.lib.mockingbird import MockResponse, register_mock
from tests.util import capture_app_logs


class TestDispatcher:
    """Master-to-worker dispatcher."""

    def test_dispatch_fixture(self, app):
        """Returns fixture data."""
        response = dispatcher.dispatch('create_canvas_schema')
        assert response['status'] == 'started'

    def test_server_error(self, app, caplog):
        """Logs unexpected server errors."""
        with capture_app_logs(app):
            canvas_error = MockResponse(401, {}, '{"message": "Unauthorized."}')
            with register_mock(dispatcher.dispatch, canvas_error):
                response = dispatcher.dispatch('create_canvas_schema')
                assert '401 Client Error: Unauthorized' in caplog.text
                assert not response
