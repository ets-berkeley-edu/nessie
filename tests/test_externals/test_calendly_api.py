"""
Copyright ©2025. The Regents of the University of California (Regents). All Rights Reserved.

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
from datetime import datetime, timedelta

import pytest
import pytz

from nessie.externals import calendly_api
from nessie.jobs.background_job import BackgroundJobError
from nessie.lib.mockingbird import MockResponse, register_mock


class TestCalendlyApi:
    """Calendly API client."""

    def test_calendly_server_error(self, app):
        """Logs unexpected Calendly server errors."""
        with app.app_context():
            calendly_error = MockResponse(500, {}, '{"message": "Internal server error."}')
            with register_mock(calendly_api._get_authorized_response, calendly_error):
                with pytest.raises(BackgroundJobError) as error:
                    calendly_api.get_scheduled_events(
                        min_start_time=datetime.now(pytz.utc),
                        max_start_time=datetime.now(pytz.utc) + timedelta(days=1),
                    )
                assert 'Failed GET https://api.calendly.com/organizations' in str(error.value)
