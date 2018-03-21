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


"""Client code allowing a master instance to dispatch requests to workers."""

import base64
from urllib.parse import urlunparse

from flask import current_app as app
from nessie.lib import http
from nessie.lib.mockingbird import fixture


@fixture('dispatch_{command}')
def dispatch(command, data=None, mock=None):
    url = build_url(command)
    with mock(url, method='post'):
        response = request(url, data)
        if response:
            return response.json()


def build_url(command):
    return urlunparse(['https', app.config['WORKER_HOST'], f'/api/{command}', '', '', ''])


def request(url, data=None):
    credentials = app.config['WORKER_USERNAME'] + ':' + app.config['WORKER_PASSWORD']
    authorization = 'Basic ' + base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    headers = {
        'Authorization': authorization,
        'Content-Type': 'application/json',
    }
    return http.request(url, headers=headers, method='post', data=data)
