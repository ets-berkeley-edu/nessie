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

from flask import current_app as app
from nessie.lib import http
from nessie.lib.mockingbird import fixture
from requests.auth import HTTPBasicAuth


"""YouCanBookMe API."""


def get_bookings_for_date(date):
    bookings = []
    date_str = date.strftime('%Y-%m-%d')
    url = http.build_url(
        f"{app.config['YCBM_BASE_URL']}/bookings",
        {
            'fields': ','.join([
                'id',
                'title',
                'startsAt',
                'endsAt',
                'answers',
                'answers.code',
                'answers.string',
                'cancelled',
                'calendars',
                'calendars.calendarId',
                'calendars.targetCalendar',
                'cancellationReason',
                'teamMember',
                'teamMember.name',
                'teamMember.id',
                'teamMember.email',
            ]),
            'jumpToDate': date_str,
        },
    )
    response = get_authorized_response(url)
    # YCBM API pagination seems a little wobbly as far as how it orders bookings, so we cast a wide net and request more pages
    # as long as any bookings on the current page have the date of interest. This conservative approach means we'll end up with
    # extra appointments outside our date range, so higher-level code should use booking ids to screen out duplicates.
    while response and hasattr(response, 'json'):
        feed = response.json()
        if not len(feed) or next((b for b in feed if b.get('startsAt', '').startswith(date_str)), None) is None:
            break
        bookings += feed
        next_url = response.links.get('next', {}).get('url')
        if not next_url:
            break
        response = get_authorized_response(next_url)
    return bookings


@fixture('ycbm_bookings')
def get_authorized_response(url, mock=None):
    with mock(url):
        return http.request(url, auth=HTTPBasicAuth(app.config['YCBM_API_USERNAME'], app.config['YCBM_API_PASSWORD']))
