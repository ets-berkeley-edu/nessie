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

import gzip
from unittest import mock

import botocore.response
import pytest
import responses
from botocore.exceptions import ClientError as BotoClientError
from botocore.exceptions import ConnectionError as BotoConnectionError

from nessie.externals import s3
from tests.util import capture_app_logs, mock_s3


@pytest.fixture
def bad_bucket(app):
    _bucket = app.config['LOCH_S3_BUCKET']
    app.config['LOCH_S3_BUCKET'] = 'not-a-bucket-nohow'
    yield
    app.config['LOCH_S3_BUCKET'] = _bucket


class FakeStreamingBody:
    """Stands in for a botocore StreamingBody, yielding one queued chunk (or raising a queued error) per read()."""

    def __init__(self, chunks):
        self._chunks = list(chunks)

    def read(self, amt=None):  # noqa: ARG002
        if not self._chunks:
            return b''
        item = self._chunks.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def set_socket_timeout(self, timeout):
        pass


class FakeS3Client:
    """Returns queued FakeStreamingBody instances from get_object, recording the kwargs of each call."""

    def __init__(self, bodies):
        self._bodies = list(bodies)
        self.get_object_calls = []

    def get_object(self, **kwargs):
        self.get_object_calls.append(kwargs)
        return {'Body': self._bodies.pop(0)}


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


class TestResilientS3ObjectStream:
    """A streaming S3 read survives a mid-stream connection reset by reopening at the last byte read."""

    def test_resumes_with_range_request_after_connection_reset(self, app):  # noqa: ARG002
        """On a reset, reopens the object with a Range request picking up where the last read left off."""
        client = FakeS3Client([
            FakeStreamingBody([b'first-chunk-', ConnectionResetError(104, 'Connection reset by peer')]),
            FakeStreamingBody([b'second-chunk', b'']),
        ])
        stream = s3._ResilientS3ObjectStream(client, 'a-bucket', 'a-key')
        stream.retry_backoff_seconds = 0

        assert stream.read(1024) == b'first-chunk-'
        assert stream.read(1024) == b'second-chunk'
        assert stream.read(1024) == b''

        assert len(client.get_object_calls) == 2
        assert 'Range' not in client.get_object_calls[0]
        assert client.get_object_calls[1]['Range'] == 'bytes=12-'

    def test_gives_up_after_max_retries(self, app):  # noqa: ARG002
        """Re-raises once the retry budget for a single read is exhausted."""
        always_broken = ConnectionResetError(104, 'Connection reset by peer')
        client = FakeS3Client([FakeStreamingBody([always_broken]) for _ in range(10)])
        stream = s3._ResilientS3ObjectStream(client, 'a-bucket', 'a-key')
        stream.retry_backoff_seconds = 0

        with pytest.raises(ConnectionResetError):
            stream.read(1024)
        assert len(client.get_object_calls) == stream.max_retries + 1

    def test_treats_invalid_range_after_reset_as_eof(self, app):  # noqa: ARG002
        """If a reset happens right at EOF, a Range reopen past the object's end is treated as end of stream."""
        invalid_range_error = BotoClientError({'Error': {'Code': 'InvalidRange'}}, 'GetObject')

        class RaisingClient(FakeS3Client):
            def get_object(self, **kwargs):
                self.get_object_calls.append(kwargs)
                if 'Range' in kwargs:
                    raise invalid_range_error
                return {'Body': self._bodies.pop(0)}

        client = RaisingClient([FakeStreamingBody([b'only-chunk', ConnectionResetError(104, 'Connection reset by peer')])])
        stream = s3._ResilientS3ObjectStream(client, 'a-bucket', 'a-key')
        stream.retry_backoff_seconds = 0

        assert stream.read(1024) == b'only-chunk'
        assert stream.read(1024) == b''


class TestGetTsvStream:
    """The higher-level TSV streaming helpers recover from a connection reset via the resilient stream."""

    def test_get_tsv_stream_recovers_from_connection_reset(self, app):
        """A reset on the very first read is retried transparently, and all rows still come through."""
        bucket = app.config['LOCH_S3_BUCKET']
        prefix = 'unloads/term_gpas'
        key = f'{prefix}/0000_part_00.gz'
        rows = [('1234567', '2232', '3.750'), ('7654321', '2232', '3.200')]
        tsv = 'sid\tterm_id\tgpa\n' + ''.join(f'{sid}\t{term_id}\t{gpa}\n' for sid, term_id, gpa in rows)

        with mock_s3(app) as m:
            m.Object(bucket, key).put(Body=gzip.compress(tsv.encode('utf-8')))

            original_read = botocore.response.StreamingBody.read
            calls = {'n': 0}

            def flaky_read(self, amt=None):
                calls['n'] += 1
                if calls['n'] == 1:
                    raise ConnectionResetError(104, 'Connection reset by peer')
                return original_read(self, amt)

            with mock.patch.object(s3._ResilientS3ObjectStream, 'retry_backoff_seconds', 0), \
                    mock.patch.object(botocore.response.StreamingBody, 'read', flaky_read), \
                    mock.patch.object(botocore.response.StreamingBody, 'set_socket_timeout'):
                # moto's mocked HTTP response doesn't expose a real socket to set a timeout on.
                result = list(s3.get_tsv_stream(prefix))

        assert [(r['sid'], r['term_id'], r['gpa']) for r in result] == rows


# TODO: @pytest.mark.testext
@pytest.mark.skip(reason="Skipping tests that rely on NESSIE_ENV=testext")
class TestS3Testext:
    """S3 client with live external connections."""

    @responses.activate
    def test_source_url_error_handling(self, app, caplog):
        """Handles and logs connection errors to source URL."""
        with capture_app_logs(app):
            url = 'http://shakespeare.mit.edu/Poetry/sonnet.XLV.html'
            key = app.config['LOCH_S3_PREFIX_TESTEXT'] + '/00001/sonnet-xlv.html'
            responses.add(responses.GET, url, status=500, body='{"message": "Internal server error."}')
            with pytest.raises(BotoConnectionError):
                s3.upload_from_url(url, key)
            assert 'Received unexpected status code, aborting S3 upload' in caplog.text
            assert 'status=500' in caplog.text
            assert 'body={"message": "Internal server error."}' in caplog.text
            assert f'url={url}' in caplog.text
            assert f'key={key}' in caplog.text

    def test_s3_nonexistent_object(self, app, caplog, bad_bucket):  # noqa: ARG002
        """Returns false on S3 checks for nonexistent objects."""
        with capture_app_logs(app):
            key = app.config['LOCH_S3_PREFIX_TESTEXT'] + '/00001/sonnet-xlv.html'
            response = s3.object_exists(key)
            assert response is False

    def test_s3_upload_error_handling(self, app, caplog, bad_bucket):  # noqa: ARG002
        """Handles and logs connection errors on S3 upload."""
        with capture_app_logs(app):
            url = 'http://shakespeare.mit.edu/Poetry/sonnet.XLV.html'
            key = app.config['LOCH_S3_PREFIX_TESTEXT'] + '/00001/sonnet-xlv.html'
            with pytest.raises(ValueError):  # noqa: PT011
                s3.upload_from_url(url, key)
            assert 'Error on S3 upload' in caplog.text
            assert 'the bucket \'not-a-bucket-nohow\' does not exist, or is forbidden for access' in caplog.text

    def test_file_upload_and_delete(self, app, cleanup_s3):  # noqa: ARG002
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
