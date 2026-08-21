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

import csv
import io
import json
import tempfile
import time
from gzip import GzipFile
from zipfile import ZipFile

import boto3
import requests
import smart_open
from botocore.exceptions import ClientError as BotoClientError
from botocore.exceptions import ConnectionError as BotoConnectionError
from botocore.exceptions import ReadTimeoutError as BotoReadTimeoutError
from botocore.exceptions import ResponseStreamingError as BotoResponseStreamingError
from flask import current_app as app
from urllib3.exceptions import ProtocolError as Urllib3ProtocolError

from nessie.lib import metadata

"""Client code to run file operations against S3."""


class _ResilientS3ObjectStream(io.RawIOBase):
    """A raw byte stream for a single S3 object that survives a mid-read connection reset.

    Some Nessie jobs (e.g. GenerateMergedStudentFeeds) hold a single streaming GetObject open for
    tens of minutes, pulling from it in bursts while a slow consumer (a merge-join against another
    stream) works through the data. A transient network blip anywhere along that path would
    kill the whole job. This wrapper catches that class of error and transparently reopens the
    connection with a Range request picking up at the last byte we successfully read, so callers
    (GzipFile, TextIOWrapper) never see the interruption.
    """

    _RETRYABLE_ERRORS = (
        BotoConnectionError,
        BotoReadTimeoutError,
        BotoResponseStreamingError,
        Urllib3ProtocolError,
        ConnectionResetError,
    )

    max_retries = 3
    retry_backoff_seconds = 2

    def __init__(self, client, bucket, key):
        super().__init__()
        self.client = client
        self.bucket = bucket
        self.key = key
        self.bytes_read = 0
        self._body = self._open()

    def _open(self):
        kwargs = {'Bucket': self.bucket, 'Key': self.key}
        if self.bytes_read:
            kwargs['Range'] = f'bytes={self.bytes_read}-'
        try:
            body = self.client.get_object(**kwargs)['Body']
        except BotoClientError as e:
            # We already read to (or past) the end of the object; the interrupted read was for trailing EOF bytes.
            if kwargs.get('Range') and e.response.get('Error', {}).get('Code') == 'InvalidRange':
                return None
            raise
        body.set_socket_timeout(app.config['AWS_S3_SESSION_DURATION'])
        return body

    def readable(self):
        return True

    def readinto(self, b):
        if self._body is None:
            return 0
        size = len(b)
        chunk = None
        for attempt in range(1, self.max_retries + 2):
            try:
                chunk = self._body.read(size)
                break
            except self._RETRYABLE_ERRORS as e:
                if attempt > self.max_retries:
                    raise
                app.logger.warning(
                    f'S3 stream read interrupted (bucket={self.bucket}, key={self.key}, bytes_read={self.bytes_read}); '
                    f'reopening connection at that offset, attempt {attempt}/{self.max_retries}.',
                    exc_info=e,
                )
                time.sleep(self.retry_backoff_seconds * attempt)
                self._body = self._open()
                if self._body is None:
                    # The interrupted read turned out to be for trailing EOF bytes; nothing left to deliver.
                    return 0
        n = len(chunk)
        b[:n] = chunk
        self.bytes_read += n
        return n


def build_s3_url(key):
    bucket = app.config['LOCH_S3_BUCKET']
    return f's3://{bucket}/{key}'


def copy(source_bucket, source_key, dest_bucket, dest_key):
    client = get_client()
    source = {
        'Bucket': source_bucket,
        'Key': source_key,
    }
    try:
        return client.copy_object(
            Bucket=dest_bucket,
            Key=dest_key,
            CopySource=source,
            ServerSideEncryption=app.config['LOCH_S3_ENCRYPTION'],
        )
    except (BotoClientError, BotoConnectionError, ValueError) as e:
        app.logger.exception(f'Error on S3 object copy: ({source_bucket}/{source_key} to {dest_bucket}/{dest_key}', exc_info=e)
        return False


def delete_objects(keys, bucket=None):
    client = get_client()
    if not bucket:
        bucket = app.config['LOCH_S3_BUCKET']
    try:
        for i in range(0, len(keys), 1000):
            objects_to_delete = [{'Key': key} for key in keys[i:i + 1000]]
            client.delete_objects(Bucket=bucket, Delete={'Objects': objects_to_delete})
        return True
    except (BotoClientError, BotoConnectionError, ValueError) as e:
        app.logger.exception(f'Error on S3 object deletion: bucket={bucket}, keys={keys}', exc_info=e)
        return False


def delete_objects_with_prefix(prefix, whitelist=()):
    keys_to_delete = []
    existing_keys = get_keys_with_prefix(prefix)
    if existing_keys is None:
        app.logger.error('Error listing keys, aborting job.')
        return False
    for key in existing_keys:
        filename = key.split('/')[-1]
        if filename not in whitelist:
            keys_to_delete.append(key)
    app.logger.info(
        f'Found {len(existing_keys)} key(s) matching prefix "{prefix}", {len(existing_keys) - len(keys_to_delete)} '
        f'key(s) in whitelist, will delete {len(keys_to_delete)} object(s)')
    if not keys_to_delete:
        return True
    if delete_objects(keys_to_delete):
        metadata.delete_canvas_snapshots(keys_to_delete)
        return True
    else:
        return False


def delete_s3_location_with_prefix(prefix):
    keys_to_delete = []
    existing_keys = get_keys_with_prefix(prefix)
    if existing_keys is None:
        app.logger.error('Error listing keys, aborting job.')
        return False
    for key in existing_keys:
        keys_to_delete.append(key)  # noqa: PERF402
    app.logger.info(
        f'Found {len(existing_keys)} key(s) matching prefix "{prefix}", {len(existing_keys) - len(keys_to_delete)} '
        f'key(s). Will delete {len(keys_to_delete)} object(s)')
    if not keys_to_delete:
        return True
    return bool(delete_objects(keys_to_delete))


def get_sts_credentials():
    sts_client = boto3.client('sts')
    role_arn = app.config['AWS_APP_ROLE_ARN']
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName='AssumeAppRoleSession',
        DurationSeconds=app.config['AWS_S3_SESSION_DURATION'],
    )
    return assumed_role_object['Credentials']


def get_session():
    credentials = get_sts_credentials()
    return boto3.Session(
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
    )


def get_client():
    session = get_session()
    return session.client('s3', region_name=app.config['LOCH_S3_REGION'])


def get_keys_with_prefix(prefix, full_objects=False, bucket=None):
    client = get_client()
    if not bucket:
        bucket = app.config['LOCH_S3_BUCKET']
    objects = []
    paginator = client.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    try:
        for page in page_iterator:
            if 'Contents' in page:
                if full_objects:
                    objects += page['Contents']
                else:
                    objects += [o.get('Key') for o in page['Contents']]
    except (BotoClientError, BotoConnectionError, ValueError) as e:
        app.logger.exception(f'Error listing S3 keys with prefix: bucket={bucket}, prefix={prefix}', exc_info=e)
        return None
    return objects


def get_object_json(s3_key):
    text = get_object_text(s3_key)
    if text:
        return json.loads(text)


def get_object_compressed_text_reader(key):
    """Read a .zip file as text; a blend of get_object_text and get_unzipped_text_reader."""
    client = get_client()
    bucket = app.config['LOCH_S3_BUCKET']
    try:
        _object = client.get_object(Bucket=bucket, Key=key)
        return ZipFile(io.BytesIO(_object['Body'].read()), mode='r')
    except (BotoClientError, BotoConnectionError, ValueError) as e:
        app.logger.exception(f'Error retrieving S3 object text: bucket={bucket}, key={key}', exc_info=e)
        return None


def get_object_text(key):
    client = get_client()
    bucket = app.config['LOCH_S3_BUCKET']
    try:
        _object = client.get_object(Bucket=bucket, Key=key)
        contents = _object.get('Body')
        if not contents:
            app.logger.error(f'Failed to get S3 object contents: bucket={bucket}, key={key})')
            return None
        return contents.read().decode('utf-8')
    except (BotoClientError, BotoConnectionError, ValueError) as e:
        app.logger.exception(f'Error retrieving S3 object text: bucket={bucket}, key={key}', exc_info=e)
        return None


def get_subfolders_with_prefix(prefix):
    client = get_client()
    bucket = app.config['LOCH_S3_BUCKET']
    subfolders = []
    paginator = client.get_paginator('list_objects')
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
            if 'CommonPrefixes' in page:
                subfolders += [o.get('Prefix') for o in page['CommonPrefixes']]
    except (BotoClientError, BotoConnectionError, ValueError) as e:
        app.logger.exception(f'Error listing S3 subfolders with prefix: bucket={bucket}, prefix={prefix}', exc_info=e)
        return None
    return sorted(subfolders)


def get_text_reader(key):
    """Iterate over millions of rows (non-zipped source) with minimal memory consumption."""
    client = get_client()
    bucket = app.config['LOCH_S3_BUCKET']
    try:
        fileobj = io.BufferedReader(_ResilientS3ObjectStream(client, bucket, key))
        return io.TextIOWrapper(fileobj, encoding='utf-8')
    except (BotoClientError, BotoConnectionError, ValueError) as e:
        app.logger.exception(f'Error retrieving S3 object text: bucket={bucket}, key={key}', exc_info=e)
        return None


def get_unzipped_text_reader(key):
    """Iterate over millions of rows (zipped source) with minimal memory consumption."""
    client = get_client()
    bucket = app.config['LOCH_S3_BUCKET']
    try:
        fileobj = io.BufferedReader(_ResilientS3ObjectStream(client, bucket, key))
        gzipped = GzipFile(None, 'rb', fileobj=fileobj)
        return io.TextIOWrapper(gzipped)
    except (BotoClientError, BotoConnectionError, ValueError) as e:
        app.logger.exception(f'Error retrieving S3 object text: bucket={bucket}, key={key}', exc_info=e)
        return None


def get_tsv_stream(path, delimiter='\t', zipped=True):
    for key in get_keys_with_prefix(path):
        if zipped:
            data = get_unzipped_text_reader(key)
        else:
            data = get_text_reader(key)
        for row in csv.DictReader(data, delimiter=delimiter, escapechar='\\', quotechar='"'):
            yield row


def object_exists(key):
    client = get_client()
    bucket = app.config['LOCH_S3_BUCKET']
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except BotoClientError as e:
        # Log an error only if we get something other than the usual response for a nonexistent object.
        if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 404:
            app.logger.exception(f'Unexpected error response on S3 existence check: bucket={bucket}, key={key}', exc_info=e)
        return False
    except (BotoConnectionError, ValueError) as e:
        app.logger.exception(f'Error on S3 existence check: bucket={bucket}, key={key}', exc_info=e)
        return False


def upload_data(data, s3_key, bucket=None):
    if bucket is None:
        bucket = app.config['LOCH_S3_BUCKET']
    try:
        client = get_client()
        client.put_object(Bucket=bucket, Key=s3_key, Body=data, ServerSideEncryption=app.config['LOCH_S3_ENCRYPTION'])
    except (BotoClientError, BotoConnectionError, ValueError) as e:
        app.logger.exception(f'Error on S3 upload: bucket={bucket}, key={s3_key}', exc_info=e)
        return False
    app.logger.info(f'S3 upload complete: bucket={bucket}, key={s3_key}')
    return True


def upload_file(file, s3_key, bucket=None):
    # Be kind; rewind
    file.seek(0)
    return upload_data(file, s3_key, bucket)


def upload_json(obj, s3_key, bucket=None):
    tmpfile = tempfile.NamedTemporaryFile()
    with open(tmpfile.name, mode='wt', encoding='utf-8') as f:
        json.dump(obj, f)
    with open(tmpfile.name, mode='rb') as f:
        return upload_file(f, s3_key, bucket)


def upload_from_response(response, s3_key, on_stream_opened=None):
    bucket = app.config['LOCH_S3_BUCKET']
    s3_url = build_s3_url(s3_key)
    if response.status_code != 200:
        app.logger.error(
            f'Received unexpected status code, aborting S3 upload '
            f'(status={response.status_code}, body={response.text}, key={s3_key})')
        raise ConnectionError(f'Response {response.status_code}: {response.text}')
    if on_stream_opened:
        on_stream_opened(response.headers)
    try:
        s3_upload_args = {'ServerSideEncryption': app.config['LOCH_S3_ENCRYPTION']}
        if s3_url.endswith('.gz'):
            s3_upload_args.update({
                'ContentEncoding': 'gzip',
                'ContentType': 'text/plain',
            })
        session = get_session()
        with smart_open.open(
            s3_url,
            'wb',
            compression='disable',
            transport_params=dict(client=session.client('s3'), client_kwargs={'S3.Client.create_multipart_upload': s3_upload_args}),
        ) as s3_out:
            for chunk in response.iter_content(chunk_size=1024):
                s3_out.write(chunk)
    except (BotoClientError, BotoConnectionError, ValueError) as e:
        app.logger.exception(f'Error on S3 upload: bucket={bucket}, key={s3_key}', exc_info=e)
        raise e
    s3_response = get_client().head_object(Bucket=bucket, Key=s3_key)
    if s3_response:
        app.logger.info(f'S3 upload complete: bucket={bucket}, key={s3_key}')
        return s3_response


def upload_from_url(url, s3_key, on_stream_opened=None):
    app.logger.info(f'Will upload URL to S3: url={url}, key={s3_key}')
    with requests.get(url, stream=True) as response:  # noqa: S113
        return upload_from_response(response, s3_key, on_stream_opened)


def upload_tsv_rows(rows, s3_key):
    data = b'\n'.join(rows)
    return upload_data(data, s3_key)
