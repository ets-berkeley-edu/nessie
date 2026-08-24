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

import io
from datetime import datetime, timezone
from unittest import mock

import pytest
from botocore.exceptions import ClientError as BotoClientError
from botocore.exceptions import ConnectionError as BotoConnectionError

from tests.util import mock_s3, override_config

# 21:30 UTC is 14:30 in America/Los_Angeles, the configured TIMEZONE.
FROZEN_NOW = datetime(2026, 8, 11, 21, 30, tzinfo=timezone.utc)

UPLOAD_PATHS = [
    '/api/upload/asc_advising_notes',
    '/api/upload/coe_advisees',
    '/api/upload/oua_admissions',
]


def post_file(client, path, api_key, contents=b'', filename='upload.txt'):
    return client.post(
        path,
        data={'file': (io.BytesIO(contents), filename)},
        headers={'app-key': api_key},
        content_type='multipart/form-data',
    )


def object_exists(m3, bucket, key):
    try:
        m3.Object(bucket, key).load()
        return True
    except (BotoClientError, BotoConnectionError, ValueError):
        return False


def create_buckets(app, m3, buckets):
    region_name = app.config['LOCH_S3_REGION']
    for bucket in buckets:
        m3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={'LocationConstraint': region_name},
        )


@pytest.mark.parametrize('api_path', UPLOAD_PATHS)
class TestUploadControllerAuthentication:
    """Upload API authentication."""

    def test_no_authentication(self, client, api_path):
        """Refuse a request with no app-key."""
        response = client.post(api_path)
        assert response.status_code == 401

    def test_bad_authentication(self, client, api_path):
        """Refuse a request with a bad app-key."""
        response = post_file(client, api_path, 'not-a-valid-key')
        assert response.status_code == 401


@pytest.mark.parametrize('api_path', UPLOAD_PATHS)
class TestUploadControllerFileHandling:
    """Upload API file handling."""

    def test_no_file(self, app, client, api_path):
        """Refuse a request with no file."""
        response = client.post(
            api_path,
            headers={'app-key': app.config['API_UPLOAD_KEYS'][0]},
            content_type='multipart/form-data',
        )
        assert response.status_code == 400
        assert response.json['message'] == 'Request must include exactly one file.'

    def test_multiple_files(self, app, client, api_path):
        """Refuse a request with more than one file."""
        response = client.post(
            api_path,
            data={
                'file1': (io.BytesIO(b'one'), 'one.txt'),
                'file2': (io.BytesIO(b'two'), 'two.txt'),
            },
            headers={'app-key': app.config['API_UPLOAD_KEYS'][0]},
            content_type='multipart/form-data',
        )
        assert response.status_code == 400
        assert response.json['message'] == 'Request must include exactly one file.'


class TestUploadAscAdvisingNotes:
    """ASC advising notes upload."""

    def test_invalid_json(self, app, client):
        """Refuse a request whose body is not valid JSON."""
        response = post_file(
            client,
            '/api/upload/asc_advising_notes',
            app.config['API_UPLOAD_KEYS'][0],
            contents=b'not json',
        )
        assert response.status_code == 400
        assert response.json['message'] == 'Uploaded file is not valid JSON.'

    @mock.patch('nessie.api.upload_controller.datetime', autospec=True)
    def test_upload(self, mock_datetime, app, client):
        """Upload a JSON file to every configured bucket."""
        mock_datetime.now.return_value = FROZEN_NOW
        buckets = app.config['API_UPLOAD_BUCKETS']
        contents = b'{"notes": []}'
        expected_key = 'asc-data/asc-sftp/incremental/advising_notes/asc_advising_notes_20260811T143000.json'

        with mock_s3(app, bucket=buckets[0]) as m3:
            create_buckets(app, m3, buckets[1:])
            response = post_file(
                client,
                '/api/upload/asc_advising_notes',
                app.config['API_UPLOAD_KEYS'][0],
                contents=contents,
                filename='notes.json',
            )
            assert response.status_code == 200
            assert response.json['key'] == expected_key
            assert response.json['buckets'] == buckets
            for bucket in buckets:
                assert object_exists(m3, bucket, expected_key)

    def test_no_buckets(self, app, client):
        """Refuse a request when no upload buckets are configured."""
        with override_config(app, 'API_UPLOAD_BUCKETS', []):
            response = post_file(
                client,
                '/api/upload/asc_advising_notes',
                app.config['API_UPLOAD_KEYS'][0],
                contents=b'{"notes": []}',
            )
            assert response.status_code == 500
            assert response.json['message'] == 'No API upload destination buckets configured.'


class TestUploadCoeAdvisees:
    """COE advisees upload."""

    def test_empty_file(self, app, client):
        """Refuse a request whose TSV contains no rows."""
        response = post_file(
            client,
            '/api/upload/coe_advisees',
            app.config['API_UPLOAD_KEYS'][0],
            contents=b'',
        )
        assert response.status_code == 400
        assert response.json['message'] == 'Uploaded file contains no rows.'

    def test_invalid_tsv(self, app, client):
        """Refuse a request whose body is not valid UTF-8 TSV."""
        response = post_file(
            client,
            '/api/upload/coe_advisees',
            app.config['API_UPLOAD_KEYS'][0],
            contents=b'\xff\xfe',
        )
        assert response.status_code == 400
        assert response.json['message'] == 'Uploaded file is not valid TSV.'

    @mock.patch('nessie.lib.util.datetime', autospec=True)
    def test_upload(self, mock_datetime, app, client):
        """Upload a TSV file and leave no stale coe_student_adviser files behind."""
        mock_datetime.now.return_value = FROZEN_NOW
        buckets = app.config['API_UPLOAD_BUCKETS']
        contents = b'sid\tadvisor\n123\tu456\n'
        expected_key = 'coe-data/students/coe_student_adviser_2026-08-11.tsv'
        stale_key = 'coe-data/students/coe_student_adviser_2026-08-10.tsv'

        with mock_s3(app, bucket=buckets[0]) as m3:
            create_buckets(app, m3, buckets[1:])
            for bucket in buckets:
                m3.Object(bucket, stale_key).put(Body=b'old rows')

            response = post_file(
                client,
                '/api/upload/coe_advisees',
                app.config['API_UPLOAD_KEYS'][0],
                contents=contents,
                filename='advisees.tsv',
            )
            assert response.status_code == 200
            assert response.json['key'] == expected_key
            assert response.json['buckets'] == buckets
            for bucket in buckets:
                assert object_exists(m3, bucket, expected_key)
                assert not object_exists(m3, bucket, stale_key)


class TestUploadOuaAdmissions:
    """OUA admissions upload."""

    def test_empty_file(self, app, client):
        """Refuse a request whose CSV contains no rows."""
        response = post_file(
            client,
            '/api/upload/oua_admissions',
            app.config['API_UPLOAD_KEYS'][0],
            contents=b'',
        )
        assert response.status_code == 400
        assert response.json['message'] == 'Uploaded file contains no rows.'

    def test_invalid_csv(self, app, client):
        """Refuse a request whose body is not valid UTF-8 CSV."""
        response = post_file(
            client,
            '/api/upload/oua_admissions',
            app.config['API_UPLOAD_KEYS'][0],
            contents=b'\xff\xfe',
        )
        assert response.status_code == 400
        assert response.json['message'] == 'Uploaded file is not valid CSV.'

    @mock.patch('nessie.api.upload_controller.datetime', autospec=True)
    def test_upload(self, mock_datetime, app, client):
        """Upload a CSV file to every protected bucket under a date-partitioned key."""
        mock_datetime.now.return_value = FROZEN_NOW
        buckets = app.config['API_UPLOAD_BUCKETS_PROTECTED']
        contents = b'sid,status\n123,admitted\n'
        expected_key = 'oua-data/slate-sftp/2026/08/11/oua_admissions_20260811T143000.csv'

        with mock_s3(app, bucket=buckets[0]) as m3:
            create_buckets(app, m3, buckets[1:])
            response = post_file(
                client,
                '/api/upload/oua_admissions',
                app.config['API_UPLOAD_KEYS'][0],
                contents=contents,
                filename='admissions.csv',
            )
            assert response.status_code == 200
            assert response.json['key'] == expected_key
            assert response.json['buckets'] == buckets
            for bucket in buckets:
                assert object_exists(m3, bucket, expected_key)
