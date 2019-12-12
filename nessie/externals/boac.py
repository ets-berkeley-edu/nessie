"""
Copyright ©2020. The Regents of the University of California (Regents). All Rights Reserved.

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

"""BOAC auth API."""


def get_manually_added_advisees():
    response = authorized_request(app.config['BOA_ADVISEES_API_URL'], app.config['BOA_ADVISEES_API_KEY'])
    if not response or not hasattr(response, 'json'):
        error = f'BOA manually added advisees API unexpected response: {response}'
        app.logger.error(error)
        return {'error': error}
    return {'feed': response.json()}


def kickoff_refresh():
    successful = True
    for boac in app.config['BOAC_REFRESHERS']:
        successful = authorized_request(boac['URL'], boac['API_KEY']) and successful
    return successful


def authorized_request(url, api_key):
    # The more typical underscored "app_key" header will be stripped out by the AWS load balancer.
    # A hyphened "app-key" header passes through.
    auth_headers = {'app-key': api_key}
    return http.request(url, auth_headers)
