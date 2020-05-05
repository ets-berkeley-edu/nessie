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

from nessie.externals import canvas_api
from nessie.lib.mockingbird import MockResponse, register_mock
from tests.util import capture_app_logs


class TestCanvasApi:
    """Canvas API client."""

    def test_course_enrollments(self, app):
        """Returns course enrollments."""
        feed = canvas_api.get_course_enrollments(7654321)
        assert feed
        assert len(feed) == 44
        assert feed[0]['user_id'] == 9000100
        assert feed[0]['last_activity_at'] == '2017-11-28T23:01:51Z'
        assert feed[43]['user_id'] == 5432100
        assert feed[43]['last_activity_at'] == '2017-11-29T22:15:26Z'

    def test_course_not_found(self, app, caplog):
        """Logs 404 for unknown course."""
        with capture_app_logs(app):
            response = canvas_api.get_course_enrollments(9999999)
            assert '404 Client Error' in caplog.text
            assert not response

    def test_server_error(self, app, caplog):
        """Logs unexpected server errors."""
        canvas_error = MockResponse(500, {}, '{"message": "Internal server error."}')
        with register_mock(canvas_api.get_course_enrollments, canvas_error):
            response = canvas_api.get_course_enrollments(7654320)
            assert '500 Server Error' in caplog.text
            assert not response

    def test_course_gradebook_history(self, app):
        """Returns course gradebook history."""
        feed = canvas_api.get_course_gradebook_history(7654321)
        assert feed
        assert len(feed) == 2
        assert feed[0]['id'] == 92781685
        assert feed[1]['id'] == 92781685

    def test_course_grade_change_log(self, app):
        """Returns course grade change log."""
        feed = canvas_api.get_course_grade_change_log(7654321)
        app.logger.error(feed)
        assert feed
        assert len(feed) == 3
        assert feed[0]['id'] == '3dbdce27-674f'
        assert feed[1]['id'] == 'e157fe26-8615'
        assert feed[2]['id'] == '4e942713-73e2'
