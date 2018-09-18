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

from botocore.exceptions import ConnectionError
from nessie.externals import s3
import pytest
import responses
from tests.util import capture_app_logs, mock_s3


@pytest.fixture
def bad_bucket(app):
    _bucket = app.config['LOCH_S3_BUCKET']
    app.config['LOCH_S3_BUCKET'] = 'not-a-bucket-nohow'
    yield
    app.config['LOCH_S3_BUCKET'] = _bucket


class TestS3:
    """S3 client with mocked external connections."""

    def test_list_keys_matching_prefix(self, app):
        """Lists keys matching prefix."""
        bucket = app.config['LOCH_S3_BUCKET']
        prefix = app.config['LOCH_S3_CANVAS_DATA_PATH_CURRENT_TERM'] + '/requests'

        with mock_s3(app) as m:
            m.Object(bucket, f'{prefix}/requests-aaa.gz').put(Body=b'some data')
            m.Object(bucket, f'{prefix}/requests-bbb.gz').put(Body=b'some more data')
            m.Object(bucket, f'{prefix}/requests-ccc.gz').put(Body=b'yet more data')
            m.Object(bucket, 'another-prefix/requests-ddd.gz').put(Body=b'utterly unrelated data')

            response = s3.get_keys_with_prefix(prefix)
            assert len(response) == 3
            assert f'{prefix}/requests-aaa.gz' in response
            assert f'{prefix}/requests-bbb.gz' in response
            assert f'{prefix}/requests-ccc.gz' in response


@pytest.mark.testext
class TestS3Testext:
    """S3 client with live external connections."""

    @responses.activate
    def test_source_url_error_handling(self, app, caplog):
        """Handles and logs connection errors to source URL."""
        with capture_app_logs(app):
            url = 'http://shakespeare.mit.edu/Poetry/sonnet.XLV.html'
            key = app.config['LOCH_S3_PREFIX_TESTEXT'] + '/00001/sonnet-xlv.html'
            responses.add(responses.GET, url, status=500, body='{"message": "Internal server error."}')
            with pytest.raises(ConnectionError):
                s3.upload_from_url(url, key)
                assert 'Received unexpected status code, aborting S3 upload' in caplog.text
                assert 'status=500' in caplog.text
                assert 'body={"message": "Internal server error."}' in caplog.text
                assert f'url={url}' in caplog.text
                assert f'key={key}' in caplog.text

    def test_s3_nonexistent_object(self, app, caplog, bad_bucket):
        """Returns false on S3 checks for nonexistent objects."""
        with capture_app_logs(app):
            key = app.config['LOCH_S3_PREFIX_TESTEXT'] + '/00001/sonnet-xlv.html'
            response = s3.object_exists(key)
            assert response is False

    def test_s3_upload_error_handling(self, app, caplog, bad_bucket):
        """Handles and logs connection errors on S3 upload."""
        with capture_app_logs(app):
            url = 'http://shakespeare.mit.edu/Poetry/sonnet.XLV.html'
            key = app.config['LOCH_S3_PREFIX_TESTEXT'] + '/00001/sonnet-xlv.html'
            with pytest.raises(ValueError):
                s3.upload_from_url(url, key)
                assert 'Error on S3 upload' in caplog.text
                assert 'the bucket \'not-a-bucket-nohow\' does not exist, or is forbidden for access' in caplog.text

    def test_file_upload_and_delete(self, app, cleanup_s3):
        """Can upload and delete files in S3."""
        url1 = 'http://shakespeare.mit.edu/Poetry/sonnet.XLV.html'
        key1 = app.config['LOCH_S3_PREFIX_TESTEXT'] + '/00001/sonnet-xlv.html'

        url2 = 'http://shakespeare.mit.edu/Poetry/sonnet.LXII.html'
        key2 = app.config['LOCH_S3_PREFIX_TESTEXT'] + '/00002/sonnet-xlii.html'

        assert s3.object_exists(key1) is False
        assert s3.upload_from_url(url1, key1)['ContentLength'] == 767
        assert s3.object_exists(key1) is True
        assert s3.get_keys_with_prefix(app.config['LOCH_S3_PREFIX_TESTEXT'] + '/00001') == [key1]

        assert s3.object_exists(key2) is False
        assert s3.upload_from_url(url2, key2)['ContentLength'] == 743
        assert s3.object_exists(key2) is True
        assert s3.get_keys_with_prefix(app.config['LOCH_S3_PREFIX_TESTEXT'] + '/00002') == [key2]

        client = s3.get_client()
        contents1 = client.get_object(Bucket=app.config['LOCH_S3_BUCKET'], Key=key1)['Body'].read().decode('utf-8')
        assert 'These present-absent with swift motion slide' in contents1
        contents2 = client.get_object(Bucket=app.config['LOCH_S3_BUCKET'], Key=key2)['Body'].read().decode('utf-8')
        assert 'Beated and chopp\'d with tann\'d antiquity' in contents2
