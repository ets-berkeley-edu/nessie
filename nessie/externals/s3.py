"""
Copyright ©2022. The Regents of the University of California (Regents). All Rights Reserved.

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
from gzip import GzipFile
import io
import json
import tempfile
from zipfile import ZipFile

import boto3
from botocore.exceptions import ClientError, ConnectionError
from flask import current_app as app
from nessie.lib import metadata
import requests
import smart_open

"""Client code to run file operations against S3."""


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
    except (ClientError, ConnectionError, ValueError) as e:
        app.logger.error(f'Error on S3 object copy: ({source_bucket}/{source_key} to {dest_bucket}/{dest_key}, error={e}')
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
    except (ClientError, ConnectionError, ValueError) as e:
        app.logger.error(f'Error on S3 object deletion: bucket={bucket}, keys={keys}, error={e}')
        return False


def delete_objects_with_prefix(prefix, whitelist=[]):
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
    except (ClientError, ConnectionError, ValueError) as e:
        app.logger.error(f'Error listing S3 keys with prefix: bucket={bucket}, prefix={prefix}, error={e}')
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
    except (ClientError, ConnectionError, ValueError) as e:
        app.logger.error(f'Error retrieving S3 object text: bucket={bucket}, key={key}, error={e}')
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
    except (ClientError, ConnectionError, ValueError) as e:
        app.logger.error(f'Error retrieving S3 object text: bucket={bucket}, key={key}, error={e}')
        return None


def get_unzipped_text_reader(key):
    """Iterate over millions of rows with minimal memory consumption."""
    client = get_client()
    bucket = app.config['LOCH_S3_BUCKET']
    try:
        _object = client.get_object(Bucket=bucket, Key=key)
        gzipped = GzipFile(None, 'rb', fileobj=_object['Body'])
        return io.TextIOWrapper(gzipped)
    except (ClientError, ConnectionError, ValueError) as e:
        app.logger.error(f'Error retrieving S3 object text: bucket={bucket}, key={key}, error={e}')
        return None


def get_tsv_stream(path, delimiter='\t'):
    for key in get_keys_with_prefix(path):
        data = get_unzipped_text_reader(key)
        for row in csv.DictReader(data, delimiter='\t', escapechar='\\', quotechar='"'):
            yield row


def object_exists(key):
    client = get_client()
    bucket = app.config['LOCH_S3_BUCKET']
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        # Log an error only if we get something other than the usual response for a nonexistent object.
        if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 404:
            app.logger.error(f'Unexpected error response on S3 existence check: bucket={bucket}, key={key}, error={e}')
        return False
    except (ConnectionError, ValueError) as e:
        app.logger.error(f'Error on S3 existence check: bucket={bucket}, key={key}, error={e}')
        return False


def upload_data(data, s3_key, bucket=None):
    if bucket is None:
        bucket = app.config['LOCH_S3_BUCKET']
    try:
        client = get_client()
        client.put_object(Bucket=bucket, Key=s3_key, Body=data, ServerSideEncryption=app.config['LOCH_S3_ENCRYPTION'])
    except (ClientError, ConnectionError, ValueError) as e:
        app.logger.error(f'Error on S3 upload: bucket={bucket}, key={s3_key}, error={e}')
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


def upload_from_url(url, s3_key, on_stream_opened=None):
    bucket = app.config['LOCH_S3_BUCKET']
    s3_url = build_s3_url(s3_key)
    with requests.get(url, stream=True) as response:
        if response.status_code != 200:
            app.logger.error(
                f'Received unexpected status code, aborting S3 upload '
                f'(status={response.status_code}, body={response.text}, key={s3_key} url={url})')
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
            # smart_open needs to be told to ignore the .gz extension, or it will smartly attempt to double-compress it.
            with smart_open.open(
                s3_url,
                'wb',
                ignore_ext=True,
                transport_params=dict(session=session, multipart_upload_kwargs=s3_upload_args),
            ) as s3_out:
                for chunk in response.iter_content(chunk_size=1024):
                    s3_out.write(chunk)
        except (ClientError, ConnectionError, ValueError) as e:
            app.logger.error(f'Error on S3 upload: source_url={url}, bucket={bucket}, key={s3_key}, error={e}')
            raise e
    s3_response = get_client().head_object(Bucket=bucket, Key=s3_key)
    if s3_response:
        app.logger.info(f'S3 upload complete: source_url={url}, bucket={bucket}, key={s3_key}')
        return s3_response


def upload_tsv_rows(rows, s3_key):
    data = b'\n'.join(rows)
    return upload_data(data, s3_key)
