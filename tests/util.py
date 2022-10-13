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

import base64
from contextlib import contextmanager
import json
import logging

import boto3
import moto
from nessie.externals import rds


@contextmanager
def capture_app_logs(app):
    """Temporarily add pytest's LogCaptureHandler to the Flask app logger.

    This makes app logs avilable to the caplog fixture for testing. Due to the way that caplog is set up, this
    logic regrettably can't go into a fixture itself.
    """
    capture_handler = next((h for h in logging.getLogger().handlers if 'LogCaptureHandler' in str(type(h))), None)
    app.logger.addHandler(capture_handler)
    yield
    app.logger.removeHandler(capture_handler)


@contextmanager
def mock_s3(app, bucket=None):
    with moto.mock_s3(), moto.mock_sts():
        region_name = app.config['LOCH_S3_REGION']
        s3 = boto3.resource('s3', region_name=region_name)
        s3.create_bucket(
            Bucket=bucket or app.config['LOCH_S3_BUCKET'],
            CreateBucketConfiguration={'LocationConstraint': region_name},
        )
        yield s3


@contextmanager
def override_config(app, key, value):
    """Temporarily override an app config value."""
    old_value = app.config[key]
    app.config[key] = value
    yield
    app.config[key] = old_value


def assert_background_job_status(prefix):
    from flask import current_app as app
    schema = app.config['RDS_SCHEMA_METADATA']
    background_job_status_results = rds.fetch(f'SELECT * FROM {schema}.background_job_status')
    assert len(background_job_status_results) == 1
    assert background_job_status_results[0]['job_id'].startswith(f'{prefix}_')
    assert background_job_status_results[0]['status'] == 'succeeded'
    assert background_job_status_results[0]['created_at']
    assert background_job_status_results[0]['updated_at'] > background_job_status_results[0]['created_at']


def credentials(app):
    return app.config['API_USERNAME'], app.config['API_PASSWORD']


def post_basic_auth(client, path, credentials, data=None):
    auth_string = bytes(credentials[0] + ':' + credentials[1], 'utf-8')
    encoded_credentials = base64.b64encode(auth_string).decode('utf-8')
    return client.post(path, data=json.dumps(data), headers={'Authorization': 'Basic ' + encoded_credentials})


def get_basic_auth(client, path, credentials):
    auth_string = bytes(credentials[0] + ':' + credentials[1], 'utf-8')
    encoded_credentials = base64.b64encode(auth_string).decode('utf-8')
    return client.get(path, headers={'Authorization': 'Basic ' + encoded_credentials})


def delete_basic_auth(client, path, credentials, data=None):
    auth_string = bytes(credentials[0] + ':' + credentials[1], 'utf-8')
    encoded_credentials = base64.b64encode(auth_string).decode('utf-8')
    return client.delete(path, data=json.dumps(data), headers={'Authorization': 'Basic ' + encoded_credentials})
