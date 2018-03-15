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

import httpretty
from nessie.externals import s3
import pytest
from tests.util import capture_app_logs


@pytest.fixture
def bad_bucket(app):
    _bucket = app.config['LOCH_S3_BUCKET']
    app.config['LOCH_S3_BUCKET'] = 'not-a-bucket-nohow'
    yield
    app.config['LOCH_S3_BUCKET'] = _bucket


@pytest.mark.testext
class TestS3:
    """S3 client."""

    @httpretty.activate
    def test_source_url_error_handling(self, app, caplog):
        """Handles and logs connection errors to source URL."""
        with capture_app_logs(app):
            url = 'http://shakespeare.mit.edu/Poetry/sonnet.XLV.html'
            key = '00001/sonnet-xlv.html'
            httpretty.register_uri(httpretty.GET, url, status=500, body='{"message": "Internal server error."}')
            response = s3.upload_from_url(url, key)
            assert response is False
            assert 'Received unexpected status code, aborting S3 upload' in caplog.text
            assert 'status=500' in caplog.text
            assert 'body={"message": "Internal server error."}' in caplog.text
            assert f'url={url}' in caplog.text
            assert f'key={key}' in caplog.text

    def test_s3_file_exists_error_handling(self, app, caplog, bad_bucket):
        """Handles and logs connection errors on S3 existence check."""
        with capture_app_logs(app):
            key = '00001/sonnet-xlv.html'
            response = s3.file_exists(key)
            assert response is False
            assert 'Error on S3 existence check' in caplog.text
            assert 'An error occurred (404) when calling the HeadObject operation' in caplog.text

    def test_s3_upload_error_handling(self, app, caplog, bad_bucket):
        """Handles and logs connection errors on S3 upload."""
        with capture_app_logs(app):
            url = 'http://shakespeare.mit.edu/Poetry/sonnet.XLV.html'
            key = '00001/sonnet-xlv.html'
            response = s3.upload_from_url(url, key)
            assert response is False
            assert 'Error on S3 upload' in caplog.text
            assert 'the bucket \'not-a-bucket-nohow\' does not exist, or is forbidden for access' in caplog.text

    def test_file_upload_and_delete(self, app, ensure_s3_bucket_empty):
        """Can upload and delete files in S3."""
        url1 = 'http://shakespeare.mit.edu/Poetry/sonnet.XLV.html'
        key1 = '00001/sonnet-xlv.html'

        url2 = 'http://shakespeare.mit.edu/Poetry/sonnet.LXII.html'
        key2 = '00002/sonnet-xlii.html'

        assert s3.file_exists(key1) is False
        assert s3.upload_from_url(url1, key1) is True
        assert s3.file_exists(key1) is True

        assert s3.file_exists(key2) is False
        assert s3.upload_from_url(url2, key2) is True
        assert s3.file_exists(key2) is True

        client = s3.get_client()
        contents1 = client.get_object(Bucket=app.config['LOCH_S3_BUCKET'], Key=key1)['Body'].read().decode('utf-8')
        assert 'These present-absent with swift motion slide' in contents1
        contents2 = client.get_object(Bucket=app.config['LOCH_S3_BUCKET'], Key=key2)['Body'].read().decode('utf-8')
        assert 'Beated and chopp\'d with tann\'d antiquity' in contents2
