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


from flask import current_app as app
from nessie.lib import http
from nessie.lib.mockingbird import fixture


def get_asc_feed():
    response = _get_asc_feed_response()
    if not response or not hasattr(response, 'json'):
        error = f'ASC API unexpected response: {response}'
        app.logger.error(error)
        return {'error': error}
    # The API responds with a hash whose values correspond to the rows of a CSV or TSV.
    asc_hash = response.json()
    return [r for r in asc_hash.values()]


@fixture('asc_athletes')
def _get_asc_feed_response(mock=None):
    url = app.config['ASC_ATHLETES_API_URL']
    with mock(url):
        headers = {
            'Accept': 'application/json',
        }
        auth_params = {
            'ETSkey': app.config['ASC_ATHLETES_API_KEY'],
        }
        return http.request(url, headers, auth_params=auth_params)
