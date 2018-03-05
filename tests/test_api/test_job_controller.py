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


import base64
import pytest


def credentials(app):
    return (app.config['WORKER_USERNAME'], app.config['WORKER_PASSWORD'])


def post_basic_auth(client, path, credentials, data=None):
    auth_string = bytes(credentials[0] + ':' + credentials[1], 'utf-8')
    encoded_credentials = base64.b64encode(auth_string).decode('utf-8')
    return client.post(path, data=data, headers={'Authorization': 'Basic ' + encoded_credentials})


@pytest.mark.parametrize(
    'api_path_authenticated',
    [
        '/api/job/create_canvas_schema',
        '/api/job/generate_boac_analytics',
        '/api/job/sync_canvas_dumps',
        '/api/job/sync_file_to_s3',
    ],
)
class TestJobControllerAuthentication:
    """Authenticated job controllers."""

    def test_no_authentication(self, client, api_path_authenticated):
        """Refuse a request with no authentication."""
        response = client.post(api_path_authenticated)
        assert response.status_code == 401

    def test_bad_authentication(self, client, api_path_authenticated):
        """Refuse a request with bad authentication."""
        response = post_basic_auth(
            client,
            api_path_authenticated,
            ('arrant', 'knave'),
        )
        assert response.status_code == 401


@pytest.mark.parametrize(
    'api_path_no_params',
    [
        '/api/job/create_canvas_schema',
        '/api/job/generate_boac_analytics',
        '/api/job/sync_canvas_dumps',
    ],
)
class TestJobControllerNoParams:
    """Job controllers that take no params."""

    def test_status(self, app, client, api_path_no_params):
        """Return job status."""
        response = post_basic_auth(client, api_path_no_params, credentials(app))
        assert response.status_code == 200
        assert response.json['status'] == 'started'


class TestJobControllerSyncFileToS3:
    """S3 file upload job controller."""

    def test_requires_url(self, app, client):
        """Requires url parameter."""
        response = post_basic_auth(
            client,
            '/api/job/sync_file_to_s3',
            credentials(app),
            {'table': 'requests', 'filename': 'requests.tar.gz'},
        )
        assert response.status_code == 400
        assert response.json['message'] == 'Required "url" parameter missing.'

    def test_requires_table(self, app, client):
        """Requires table parameter."""
        response = post_basic_auth(
            client,
            '/api/job/sync_file_to_s3',
            credentials(app),
            {'url': 'https://foo.instructure.com', 'filename': 'requests.tar.gz'},
        )
        assert response.status_code == 400
        assert response.json['message'] == 'Required "table" parameter missing.'

    def test_requires_filename(self, app, client):
        """Requires filename parameter."""
        response = post_basic_auth(
            client,
            '/api/job/sync_file_to_s3',
            credentials(app),
            {'url': 'https://foo.instructure.com', 'table': 'requests'},
        )
        assert response.status_code == 400
        assert response.json['message'] == 'Required "filename" parameter missing.'

    def test_status(self, app, client):
        """Returns job status."""
        response = post_basic_auth(
            client,
            '/api/job/sync_file_to_s3',
            credentials(app),
            {'url': 'https://foo.instructure.com', 'table': 'requests', 'filename': 'requests.tar.gz'},
        )
        assert response.status_code == 200
        assert response.json['status'] == 'started'
