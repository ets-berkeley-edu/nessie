"""
Copyright ©2021. The Regents of the University of California (Regents). All Rights Reserved.

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
from tests.util import credentials, delete_basic_auth, post_basic_auth


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
        assert len(jobs) == 10
        for job in jobs:
            assert job['locked'] is False
        assert next(job for job in jobs if job['id'] == 'job_sync_canvas_snapshots')
        assert next(job for job in jobs if job['id'] == 'job_resync_canvas_snapshots')
        generate_tables_job = next(job for job in jobs if job['id'] == 'job_generate_all_tables')
        assert generate_tables_job['components'] == [
            'RefreshCanvasDataCatalog',
            'GenerateIntermediateTables',
            'IndexEnrollments',
            'GenerateBoacAnalytics',
        ]
        assert generate_tables_job['trigger'] == "cron[hour='3', minute='30']"
        assert re.match(r'\d{4}-\d{2}-\d{2} 03:30:00', generate_tables_job['nextRun'])


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

    def test_reload_configured_schedule(self, app, client):
        post_basic_auth(
            client,
            '/api/schedule/job_sync_canvas_snapshots',
            credentials(app),
            {'hour': 12, 'minute': 30},
        )
        post_basic_auth(
            client,
            '/api/schedule/job_resync_canvas_snapshots',
            credentials(app),
            {},
        )
        response = post_basic_auth(
            client,
            '/api/schedule/reload',
            credentials(app),
        )
        assert response.status_code == 200
        jobs = response.json
        assert len(jobs) == 10
        sync_job = next(job for job in jobs if job['id'] == 'job_sync_canvas_snapshots')
        assert sync_job['trigger'] == "cron[hour='1', minute='0']"
        resync_job = next(job for job in jobs if job['id'] == 'job_resync_canvas_snapshots')
        assert '01:40:00' in resync_job['nextRun']

    def test_delete_job(self, app, client):
        """Deletes obsolete job definition from schedule."""
        post_basic_auth(
            client,
            '/api/schedule/reload',
            credentials(app),
        )
        jobs = client.get('/api/schedule').json
        assert len(jobs) == 10
        assert next((job for job in jobs if job['id'] == 'job_generate_all_tables'), None) is not None
        response = delete_basic_auth(
            client,
            '/api/schedule/job_generate_all_tables',
            credentials(app),
        )
        assert response.status_code == 200
        jobs = response.json
        assert len(jobs) == 9
        assert next((job for job in jobs if job['id'] == 'job_generate_all_tables'), None) is None

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
