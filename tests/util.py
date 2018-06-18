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
from contextlib import contextmanager
import json
import logging

import boto3
import moto
import responses


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
def mock_s3(app):
    # Allow calls to live external URLs during tests; currently our tests are using shakespeare.mit.edu.
    # TODO See if we can get httpretty and/or responses to sit nicely beside moto in non-testext mode.
    responses.add_passthru('http://')
    with moto.mock_s3():
        s3 = boto3.resource('s3', app.config['LOCH_S3_REGION'])
        s3.create_bucket(Bucket=app.config['LOCH_S3_BUCKET'])
        yield s3


@contextmanager
def override_config(app, key, value):
    """Temporarily override an app config value."""
    old_value = app.config[key]
    app.config[key] = value
    yield
    app.config[key] = old_value


def credentials(app):
    return (app.config['WORKER_USERNAME'], app.config['WORKER_PASSWORD'])


def post_basic_auth(client, path, credentials, data=None):
    auth_string = bytes(credentials[0] + ':' + credentials[1], 'utf-8')
    encoded_credentials = base64.b64encode(auth_string).decode('utf-8')
    return client.post(path, data=json.dumps(data), headers={'Authorization': 'Basic ' + encoded_credentials})
