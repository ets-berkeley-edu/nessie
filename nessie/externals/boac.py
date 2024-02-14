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

from contextlib import contextmanager

from flask import current_app as app
from nessie.lib import http
import requests

"""BOAC auth API."""


@contextmanager
def export_notes_metadata(boa_credentials):
    url = f"{boa_credentials['API_BASE_URL']}/reports/boa_notes/metadata"
    header = _auth_header(boa_credentials['API_KEY'])
    with requests.get(url, headers=header, stream=True) as response:
        yield response


def kickoff_refresh():
    successful = True
    for boac in app.config['BOAC_REFRESHERS']:
        successful = _authorized_request(f"{boac['API_BASE_URL']}/admin/cachejob/refresh", boac['API_KEY']) and successful
    return successful


def _authorized_request(url, api_key):
    return http.request(url, _auth_header(api_key))


def _auth_header(api_key):
    # The more typical underscored "app_key" header will be stripped out by the AWS load balancer.
    # A hyphened "app-key" header passes through.
    return {'app-key': api_key}
