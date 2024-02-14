"""
Copyright ©2024. The Regents of the University of California (Regents). All Rights Reserved.

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
from datetime import datetime
import hashlib
import hmac
from urllib.parse import urlparse, urlunparse

from flask import current_app as app
from nessie.lib import http
from nessie.lib.mockingbird import fixture

"""Client code to connect to the Canvas Data API."""


@fixture('canvas_data_file_sync')
def get_snapshots(mock=None):
    url = build_url('account/self/file/sync')
    with mock(url):
        response = request(url)
        if response:
            return response.json()


@fixture('canvas_data_schema_latest')
def get_canvas_data_schema(mock=None):
    url = build_url('schema/latest')
    with mock(url):
        response = request(url)
        if response:
            return response.json()


def request(url):
    timestamp = datetime.utcnow().isoformat()[:-3] + 'Z'
    signature = generate_hmac_signature(url, timestamp)
    authorization = 'HMACAuth ' + app.config['CANVAS_DATA_API_KEY'] + ':' + signature
    headers = {
        'Authorization': authorization,
        'Date': timestamp,
    }
    return http.request(url, headers=headers)


def build_url(path):
    return urlunparse(['https', app.config['CANVAS_DATA_HOST'], f'/api/{path}', '', '', ''])


def generate_hmac_signature(url, timestamp, method='GET', content_type='', content_md5=''):
    parsed = urlparse(url)
    secret = app.config['CANVAS_DATA_API_SECRET']
    message = '\n'.join([
        method,
        parsed.netloc,
        content_type,
        content_md5,
        parsed.path,
        parsed.query,
        timestamp,
        secret,
    ])
    m = hmac.new(secret.encode('utf-8'), digestmod=hashlib.sha256)
    m.update(message.encode('utf-8'))
    return base64.b64encode(m.digest()).decode('utf-8')
