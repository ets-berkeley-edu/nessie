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

from datetime import datetime, timedelta

from flask import current_app as app
from nessie.lib import http
from nessie.lib.mockingbird import fixture

"""Official access to student data."""


def get_v1_student(sid):
    response = _get_v1_student(sid)
    if response and hasattr(response, 'json'):
        unwrapped = response.json().get('apiResponse', {}).get('response', {}).get('any', {}).get('students', [])
        if unwrapped:
            unwrapped = unwrapped[0]
        return unwrapped
    else:
        return


def get_v2_student(sid, term_id=None, as_of=None):
    response = _get_v2_single_student(sid, term_id, as_of)
    if response and hasattr(response, 'json'):
        return response.json().get('apiResponse', {}).get('response', {})
    else:
        return


@fixture('sis_student_api_v1_{sid}')
def _get_v1_student(sid, mock=None):
    url = http.build_url(app.config['STUDENT_V1_API_URL'] + '/' + str(sid) + '/all')
    with mock(url):
        return authorized_request_v1(url)


def get_v2_by_sids_list(up_to_100_sids, term_id=None, as_of=None, with_registration=False, with_contacts=True):
    response = _get_v2_by_sids_list(up_to_100_sids, term_id, as_of, with_registration, with_contacts)
    if response and hasattr(response, 'json'):
        unwrapped = response.json().get('apiResponse', {}).get('response', {}).get('students', [])
        if len(unwrapped) < len(up_to_100_sids):
            app.logger.warn(f'{len(up_to_100_sids)} SIDs requested; {len(unwrapped)} students returned')
        return unwrapped
    else:
        app.logger.error(f'Got error response: {response}')
        return False


@fixture('sis_student_list_api_v2')
def _get_v2_by_sids_list(up_to_100_sids, term_id, as_of, with_registration, with_contacts, mock=None):
    """Collect SIS Student Profile data for up to 100 SIDs at a time.

    The SIS 'list' request does not return studentAttributes, ethnicities, or gender. (In other words, 'inc-attr',
    'inc-dmgr', and 'inc-gndr' options are ignored.) As a practical matter, registrations should only be requested
    in combination with a 'term-id' parameter. No registrations outside that term will be returned, and will have
    to be filled in by some other API call.
    """
    id_list = ','.join(up_to_100_sids)
    params = {
        'id-list': id_list,
        'affiliation-status': 'ALL',
        'inc-acad': True,
        'inc-cntc': with_contacts,
        'inc-completed-programs': True,
        'inc-inactive-programs': True,
    }
    if term_id:
        params['term-id'] = term_id
    if as_of:
        params['as-of-date'] = as_of
    if with_registration:
        params['inc-regs'] = True
    url = http.build_url(app.config['STUDENT_API_URL'] + '/list', params)
    with mock(url):
        return authorized_request_v2(url)


def _get_v2_single_student(sid, term_id=None, as_of=None):
    params = {
        'affiliation-status': 'ALL',
        'inc-acad': True,
        'inc-attr': True,
        'inc-cntc': True,
        'inc-completed-programs': True,
        'inc-inactive-programs': True,
        'inc-dmgr': True,
        'inc-gndr': True,
        'inc-regs': True,
    }
    # If 'term-id' is not specified, the 'inc-regs' parameter will pull in all registrations.
    # This will slow responses down considerably.
    if term_id:
        params['term-id'] = term_id
    if as_of:
        # In format '2018-12-01'.
        params['as-of-date'] = as_of
    url = http.build_url(app.config['STUDENT_API_URL'] + f'/{sid}', params)
    return authorized_request_v2(url)


def get_term_gpas_registration_demog(sid, with_demog=True):
    # Unlike the V1 Students API, V2 will not returns 'registrations' data for the upcoming term unless
    # we request an 'as-of-date' in the future.
    near_future = (datetime.now() + timedelta(days=60)).strftime('%Y-%m-%d')
    response = _get_v2_registrations_demog(sid, as_of=near_future, with_demog=with_demog)
    feed = response and hasattr(response, 'json') and response.json().get('apiResponse', {}).get('response', {})
    if not feed:
        return
    registrations = feed.get('registrations')
    term_gpas = {}
    last_registration = {}
    if registrations:
        for registration in registrations:
            # Ignore terms in which the student took no classes with units (or, for current/future terms, will
            # take classes with units).
            term_units = registration.get('termUnits', [])
            total_units = next((u for u in term_units if u['type']['code'] == 'Total'), None)
            if not total_units or not (total_units.get('unitsTaken') or total_units.get('unitsEnrolled')):
                continue

            # We prefer the most recent completed registration. But if the only registration data
            # is for an in-progress or future term, use it as a fallback.
            is_pending = (not total_units.get('unitsTaken')) and total_units.get('unitsEnrolled')
            if is_pending and last_registration:
                continue

            # At present, terms spent as an Extension student are not included in Term GPAs (but see BOAC-2266).
            # However, if there are no other types of registration, the Extension term is used for academicCareer.
            if registration.get('academicCareer', {}).get('code') == 'UCBX':
                if last_registration and (last_registration.get('academicCareer', {}).get('code') != 'UCBX'):
                    continue

            # The most recent registration will be at the end of the list.
            last_registration = registration

            # Future and current registrations lack interesting GPAs.
            if is_pending:
                continue

            # Specifically for Term GPAs, ignore terms in which the student was not an undergraduate.
            if registration.get('academicCareer', {}).get('code') != 'UGRD':
                continue

            term_id = registration.get('term', {}).get('id')
            gpa = registration.get('termGPA', {}).get('average')
            if term_id and gpa is not None:
                units_taken_for_gpa = next(
                    (tu.get('unitsTaken') for tu in term_units if tu['type']['code'] == 'For GPA'),
                    None,
                )
                units_taken_total = next(
                    (tu.get('unitsTaken') for tu in term_units if tu['type']['code'] == 'Total'),
                    None,
                )
                term_gpas[term_id] = {
                    'gpa': gpa,
                    'unitsTaken': units_taken_total,
                    'unitsTakenForGpa': units_taken_for_gpa,
                }
    summary = {
        'last_registration': last_registration,
        'term_gpas': term_gpas,
    }
    if with_demog:
        demographics = {k: feed[k] for k in feed.keys() & {'ethnicities', 'foreignCountries', 'gender', 'residency', 'usaCountry'}}
        summary['demographics'] = demographics
    return summary


@fixture('sis_student_registrations_demog_api_{sid}')
def _get_v2_registrations_demog(sid, as_of=None, with_demog=True, mock=None):
    params = {
        'affiliation-status': 'ALL',
        'inc-regs': True,
    }
    if as_of:
        params['as-of-date'] = as_of
    if with_demog:
        params.update({
            'inc-dmgr': True,
            'inc-gndr': True,
        })
    url = http.build_url(app.config['STUDENT_API_URL'] + f'/{sid}', params)
    with mock(url):
        return authorized_request_v2(url)


def authorized_request_v2(url):
    if app.config['STUDENT_API_USER']:
        return basic_auth(url)
    auth_headers = {
        'app_id': app.config['STUDENT_API_ID'],
        'app_key': app.config['STUDENT_API_KEY'],
        'Accept': 'application/json',
    }
    response = http.request(url, auth_headers)
    # We are occasionally receiving bad JSON payloads from the API and would like to know why.
    try:
        response and hasattr(response, 'json') and response.json()
    except ValueError as e:
        if hasattr(response, 'content'):
            desc = response.content
        else:
            desc = response
        app.logger.error(f'Error on API call {url}, response={desc}, error={e}')
        return
    return response


def authorized_request_v1(url):
    auth_headers = {
        'app_id': app.config['STUDENT_API_ID'],
        'app_key': app.config['STUDENT_API_KEY'],
        'Accept': 'application/json',
    }
    return http.request(url, auth_headers)


def basic_auth(url):
    headers = {
        'Accept': 'application/json',
    }
    return http.request(url, headers, auth=(app.config['STUDENT_API_USER'], app.config['STUDENT_API_PWD']))
