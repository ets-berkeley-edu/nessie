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

from flask import current_app as app
from nessie.lib import http
import requests


"""Calendly API."""

RESULT_SET_LIMIT_PER_REQUEST = 100


def get_events(min_start_time, max_start_time):
    def _get_events(_page_token=None):
        # Calendly API docs: https://developer.calendly.com/api-docs/2d5ed9bbd2952-list-events
        url = http.build_url(
            f"{app.config['CALENDLY_BASE_URL']}/scheduled_events",
            {
                'count': RESULT_SET_LIMIT_PER_REQUEST,
                'max_start_time': max_start_time,
                'min_start_time': min_start_time,
                'organization': app.config['CALENDLY_ORGANIZATION_UUID'],
                'page_token': _page_token,
                'sort': 'start_time',
                'status': 'active',
            },
        )
        _feed = get_authorized_response(url).json()
        _events = _feed['collection']
        _next_page_token = _feed['pagination']['next_page_token']
        return _events, _next_page_token

    events, next_page_token = _get_events()
    while next_page_token is not None:
        more_events, next_page_token = _get_events(_page_token=next_page_token)
        events.extend(more_events)
    return events


def get_authorized_response(url, mock=None):
    with mock(url):
        auth_token = app.config['CALENDLY_AUTH_TOKEN']
        return requests.get(  # noqa: S113
            headers={'Authorization': f'Basic {auth_token}'},
            url=url,
        )
