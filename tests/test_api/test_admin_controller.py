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

from tests.util import credentials, get_basic_auth, post_basic_auth


class TestAdminController:
    """Admin API."""

    def test_no_authentication(self, client):
        """Refuse a request with no authentication."""
        response = client.post('/api/admin/background_job_status')
        assert response.status_code == 401

    def test_background_job_status(self, app, client, metadata_db):
        """Returns a well-formed response."""
        response = post_basic_auth(
            client,
            '/api/admin/background_job_status',
            credentials(app),
        )
        assert response.status_code == 200
        assert response.json == []

    def test_runnable_jobs(self, app, client):
        """Returns jobs runnable via Admin Console."""
        response = get_basic_auth(client=client, path='/api/admin/runnable_jobs', credentials=credentials(app))
        assert response.status_code == 200
        job = next((job for job in response.json if job.get('name') == 'Generate merged student feeds'), None)
        assert job.get('path') == '/api/job/generate_merged_student_feeds'
        assert 'POST' in job.get('methods')
