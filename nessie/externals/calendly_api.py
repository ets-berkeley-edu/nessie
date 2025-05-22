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

from nessie.jobs.background_job import BackgroundJobError
from nessie.lib import http
from nessie.lib.mockingbird import fixture
import pytz
import requests


"""Calendly API."""

RESULT_SET_LIMIT_PER_REQUEST = 100


def get_scheduled_events(min_start_time, max_start_time):
    organization = _get_organization_object()
    organization_uri = organization['resource']['uri']
    events, next_page_token = _get_scheduled_events(
        min_start_time=min_start_time,
        max_start_time=max_start_time,
        organization_uri=organization_uri,
    )
    while next_page_token is not None:
        more_events, next_page_token = _get_scheduled_events(
            min_start_time=min_start_time,
            max_start_time=max_start_time,
            organization_uri=organization_uri,
            page_token=next_page_token,
        )
        events.extend(more_events)
    return events


def _get_scheduled_events(
        min_start_time,
        max_start_time,
        organization_uri,
        page_token=None,
):
    # Calendly API docs: https://developer.calendly.com/api-docs/2d5ed9bbd2952-list-events
    query = {
        'count': RESULT_SET_LIMIT_PER_REQUEST,
        'max_start_time': _to_calendly_date_format(max_start_time),
        'min_start_time': _to_calendly_date_format(min_start_time),
        'organization': organization_uri,
        'sort': 'start_time',
        'status': 'active',
    }
    if page_token:
        query['page_token'] = page_token
    feed = _make_calendly_api_request(path='/scheduled_events', query=query).json()
    return feed['collection'], feed['pagination']['next_page_token']


def _get_organization_object():
    # Calendly API docs: https://developer.calendly.com/api-docs/9738aea27ba80-get-organization
    uuid = app.config['CALENDLY_ORGANIZATION_UUID']
    return _make_calendly_api_request(path=f'/organizations/{uuid}').json()


def _make_calendly_api_request(path, query=None):
    base_url = app.config['CALENDLY_BASE_API_URL']
    url = http.build_url(url=f'{base_url}{path}', query=query)
    app.logger.info(f'Calendly API request: {url}')
    return _get_authorized_response(url)


@fixture('calendly_scheduled_events')
def _get_authorized_response(url, mock=None):
    with mock(url):
        response = requests.get(  # noqa: S113
            headers={
                'Authorization': f"Bearer {app.config['CALENDLY_AUTH_TOKEN']}",
                'Content-Type': 'application/json',
            },
            url=url,
        )
        if response.status_code != 200:
            raise BackgroundJobError(f'Failed GET {url} (status_code: {response.status_code}) due to {response.text}')
        return response


def _to_calendly_date_format(value):
    return value.astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
