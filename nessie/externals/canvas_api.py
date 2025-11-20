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


import csv
import io
import random
from time import sleep

from canvasapi import Canvas
from flask import current_app as app

from nessie.lib import http
from nessie.lib.mockingbird import fixture

"""Client code to connect to the Canvas API."""


@fixture('canvas_course_enrollments_{course_id}')
def get_course_enrollments(course_id, mock=None):
    path = f'/api/v1/courses/{course_id}/enrollments'
    return paged_request(
        path=path,
        mock=mock,
        query={
            'type[]': 'StudentEnrollment',
            # By default, Canvas will not return any students at all for completed course sites.
            'state[]': ['active', 'completed', 'inactive'],
        },
    )


@fixture('canvas_course_site_{course_id}')
def get_course_site(course_id, mock=None):
    path = f'/api/v1/courses/{course_id}'
    return request(
        path=path,
        mock=mock,
    )


@fixture('canvas_section_{sis_section_id}')
def get_section_by_sis_id(sis_section_id, mock=None):
    path = f'/api/v1/sections/sis_section_id:{sis_section_id}'
    return request(
        path=path,
        mock=mock,
    )


BACKGROUND_STATUS_CHECK_INTERVAL = 20
MAX_REPORT_RETRIEVAL_ATTEMPTS = 180


def get_csv_report(report_type, download_path=None, term_id=None):
    canvas = _get_canvas_client()
    account = canvas.get_account(app.config['CANVAS_BERKELEY_ACCOUNT_ID'])
    parameters = {report_type: 1}
    if term_id:
        parameters['enrollment_term'] = f'sis_term_id:TERM:{term_id}'

    r = account.create_report('provisioning_csv', parameters=parameters)
    if not r:
        app.logger.error(f'Failed to request CSV {report_type} report')
        return None

    app.logger.info(f'Requested CSV {report_type} report: {r}')
    attempts = 0

    while attempts < MAX_REPORT_RETRIEVAL_ATTEMPTS:
        report = account.get_report('provisioning_csv', r.id)

        if report.status == 'complete':
            file = canvas.get_file(report.attachment['id'])
            if not download_path:
                return csv.DictReader(io.StringIO(file.get_contents()))
            else:
                # We use this lower-level workaround to canvasapi's File.download in order to avoid memory-intensive
                # logging on a large file response.
                file_response = canvas._Canvas__requester._get_request(file.url, {})
                with open(download_path, 'wb') as f:
                    f.write(file_response.content)
                return True

        elif report.status == 'error':
            app.logger.error(f'Failed to generate CSV {report_type} report: {report}')
            return None

        else:
            attempts += 1
            sleep(BACKGROUND_STATUS_CHECK_INTERVAL)

    app.logger.error(f'Failed to retrieve CSV {report_type} report after {MAX_REPORT_RETRIEVAL_ATTEMPTS} attempts')


def build_url(path, query=None):
    working_url = app.config['CANVAS_HTTP_URL'] + path
    return http.build_url(working_url, query)


def authorized_request(url):
    auth_headers = {'Authorization': f'Bearer {_get_token()}'}
    return http.request(url, auth_headers)


def request(path, mock, query=None):
    url = build_url(path, query)
    with mock(url):
        response = authorized_request(url)
        if response:
            return response.json()


def paged_request(path, mock, query=None):
    if query is None:
        query = {}
    query['per_page'] = 100
    url = build_url(
        path,
        query,
    )
    results = []
    while url:
        with mock(url):
            response = authorized_request(url)
            if not response:
                return None
            results.extend(response.json())
            url = http.get_next_page(response)
    return results


def _get_canvas_client():
    return Canvas(
        base_url=app.config['CANVAS_HTTP_URL'],
        access_token=_get_token(),
    )


def _get_token():
    return random.choice(app.config['CANVAS_HTTP_TOKENS'])
