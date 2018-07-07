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


import re

import pytest
from tests.util import credentials, post_basic_auth


@pytest.fixture()
def scheduler(app):
    """Re-initialize job scheduler from configs on teardown so that changes don't persist."""
    yield
    from nessie.jobs.scheduling import initialize_job_schedules
    initialize_job_schedules(app, force=True)


class TestGetSchedule:

    def test_get_schedule(self, app, client):
        """Returns job schedule based on default config values."""
        jobs = client.get('/api/schedule').json
        assert len(jobs) == 9
        assert jobs[0]['id'] == 'job_sync_canvas_snapshots'
        assert jobs[0]['components'] == ['SyncCanvasSnapshots']
        assert jobs[0]['locked'] is False
        assert jobs[1]['id'] == 'job_resync_canvas_snapshots'
        assert jobs[1]['components'] == ['ResyncCanvasSnapshots']
        assert jobs[1]['locked'] is False
        assert jobs[6]['id'] == 'job_generate_all_tables'
        assert jobs[6]['components'] == ['CreateCanvasSchema', 'CreateSisSchema', 'GenerateIntermediateTables', 'GenerateBoacAnalytics']
        assert jobs[6]['locked'] is False
        assert jobs[6]['trigger'] == "cron[hour='3', minute='30']"
        assert re.match('\d{4}-\d{2}-\d{2} 03:30:00', jobs[6]['nextRun'])


class TestUpdateSchedule:

    def test_no_authentication(self, app, client):
        """Refuses a request with no authentication."""
        response = client.post('/api/schedule/job_sync_canvas_snapshots')
        assert response.status_code == 401

    def test_bad_authentication(self, app, client):
        """Refuse a request with bad authentication."""
        response = post_basic_auth(
            client,
            '/api/schedule/job_sync_canvas_snapshots',
            ('arrant', 'knave'),
        )
        assert response.status_code == 401

    def test_unknown_job_id(self, app, client):
        """Handles unknown job id."""
        response = post_basic_auth(
            client,
            '/api/schedule/job_churn_butter',
            credentials(app),
        )
        assert response.status_code == 400

    def test_unknown_cron_param(self, app, client):
        """Handles unknown cron params."""
        response = post_basic_auth(
            client,
            '/api/schedule/job_sync_canvas_snapshots',
            credentials(app),
            {'h': 'j'},
        )
        assert response.status_code == 400

    def test_job_reschedule(self, app, client, scheduler):
        """Reschedules a job."""
        response = post_basic_auth(
            client,
            '/api/schedule/job_sync_canvas_snapshots',
            credentials(app),
            {'hour': 12, 'minute': 30},
        )
        assert response.status_code == 200
        assert response.json['trigger'] == "cron[hour='12', minute='30']"
        assert '12:30:00' in response.json['nextRun']

    def test_job_pause_resume(self, app, client, scheduler):
        """Pauses and resumes a job."""
        response = post_basic_auth(
            client,
            '/api/schedule/job_sync_canvas_snapshots',
            credentials(app),
            {},
        )
        assert response.status_code == 200
        assert response.json['trigger'] == "cron[hour='1', minute='0']"
        assert response.json['nextRun'] is None

        response = post_basic_auth(
            client,
            '/api/schedule/job_sync_canvas_snapshots',
            credentials(app),
            {'hour': 1, 'minute': 0},
        )
        assert response.status_code == 200
        assert response.json['trigger'] == "cron[hour='1', minute='0']"
        assert '01:00:00' in response.json['nextRun']
