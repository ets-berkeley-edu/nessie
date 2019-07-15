"""
Copyright ©2019. The Regents of the University of California (Regents). All Rights Reserved.

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

import json
import re

from flask import current_app as app
from nessie.lib.berkeley import degree_program_url_for_major, term_name_for_sis_id
from nessie.lib.util import vacuum_whitespace


def parse_merged_sis_profile(sis_student_api_feed, degree_progress_api_feed, last_registration_feed):
    sis_student_api_feed = sis_student_api_feed and json.loads(sis_student_api_feed, strict=False)
    if not sis_student_api_feed:
        return False

    sis_profile = {}

    # We sometimes get malformed feed structures from the Hub, most often in the form of
    # duplicate wrapped dictionaries (BOAC-362, NS-202, NS-203). Retrieve as much as we
    # can, separately handling exceptions in different parts of the feed.
    for merge_method in [
        merge_sis_profile_academic_status,
        merge_sis_profile_emails,
        merge_sis_profile_names,
        merge_sis_profile_phones,
        merge_holds,
    ]:
        try:
            merge_method(sis_student_api_feed, sis_profile)
        except AttributeError as e:
            app.logger.error(f'Hub Student API returned malformed response in {sis_student_api_feed}')
            app.logger.error(e)

    merge_registration(sis_student_api_feed, last_registration_feed, sis_profile)
    if sis_profile.get('academicCareer') == 'UGRD':
        sis_profile['degreeProgress'] = degree_progress_api_feed and json.loads(degree_progress_api_feed)

    return sis_profile


def merge_holds(sis_student_api_feed, sis_profile):
    sis_profile['holds'] = sis_student_api_feed.get('holds', [])


def merge_sis_profile_academic_status(sis_student_api_feed, sis_profile):
    # The Hub may return multiple academic statuses. We'll select the first status with a well-formed academic
    # career that is not a concurrent enrollment.
    academic_status = None
    career_code = None
    for status in sis_student_api_feed.get('academicStatuses', []):
        career_code = status.get('studentCareer', {}).get('academicCareer', {}).get('code')
        if career_code and career_code != 'UCBX':
            academic_status = status
            break
        elif career_code == 'UCBX':
            academic_status = status
            next
    sis_profile['academicCareer'] = career_code
    if not academic_status:
        return

    cumulative_units = None
    cumulative_units_taken_for_gpa = None

    for units in academic_status.get('cumulativeUnits', []):
        code = units.get('type', {}).get('code')
        if code == 'Total':
            cumulative_units = units.get('unitsCumulative')
        elif code == 'For GPA':
            cumulative_units_taken_for_gpa = units.get('unitsTaken')

    sis_profile['cumulativeUnits'] = cumulative_units

    cumulative_gpa = academic_status.get('cumulativeGPA', {}).get('average')
    if cumulative_gpa == 0 and not cumulative_units_taken_for_gpa:
        sis_profile['cumulativeGPA'] = None
    else:
        sis_profile['cumulativeGPA'] = cumulative_gpa

    sis_profile['termsInAttendance'] = academic_status.get('termsInAttendance')

    merge_sis_profile_matriculation(academic_status, sis_profile)
    merge_sis_profile_plans(academic_status, sis_profile)


def merge_registration(sis_student_api_feed, last_registration_feed, sis_profile):
    registration = next((r for r in sis_student_api_feed.get('registrations', [])), None)
    # If the student is not officially registered in the current term, the basic V2 Students API will not include a
    # 'registrations' element. Instead, we find the most recent registration-hosted data through the fuller
    # V2 registrations feed.
    if not registration:
        registration = last_registration_feed and json.loads(last_registration_feed)
    sis_profile['currentRegistration'] = registration
    if not registration:
        return

    if not sis_profile['academicCareer']:
        sis_profile['academicCareer'] = registration.get('academicCareer', {}).get('code')

    # The old 'academicLevel' element has become at least two 'academicLevels': one for the beginning-of-term, one
    # for the end-of-term. The beginning-of-term level should match what V1 gave us.
    levels = registration.get('academicLevels', [])
    sis_profile['level'] = next((l['level']['description'] for l in levels if l['type']['code'] == 'BOT'), None)
    for units in registration.get('termUnits', []):
        if units.get('type', {}).get('description') == 'Total':
            sis_profile['currentTerm'] = {
                'unitsMaxOverride': units.get('unitsMax'),
                'unitsMinOverride': units.get('unitsMin'),
            }
            break
    # TODO Should we also check for ['academicStanding']['status'] == {'code': 'DIS', 'description': 'Dismissed'}?
    withdrawal_cancel = registration.get('withdrawalCancel', {})
    if withdrawal_cancel:
        sis_profile['withdrawalCancel'] = {
            'description': withdrawal_cancel.get('type', {}).get('description'),
            'reason': withdrawal_cancel.get('reason', {}).get('code'),
            'date': withdrawal_cancel.get('date'),
        }


def merge_sis_profile_emails(sis_student_api_feed, sis_profile):
    primary_email = None
    campus_email = None
    for email in sis_student_api_feed.get('emails', []):
        if email.get('primary'):
            primary_email = email.get('emailAddress')
            break
        elif email.get('type', {}).get('code') == 'CAMP':
            campus_email = email.get('emailAddress')
    sis_profile['emailAddress'] = primary_email or campus_email


def merge_sis_profile_matriculation(academic_status, sis_profile):
    matriculation = academic_status.get('studentCareer', {}).get('matriculation')
    if matriculation:
        matriculation_term_name = matriculation.get('term', {}).get('name')
        if matriculation_term_name and re.match('\A2\d{3} (?:Spring|Summer|Fall)\Z', matriculation_term_name):
            # "2015 Fall" to "Fall 2015"
            sis_profile['matriculation'] = ' '.join(reversed(matriculation_term_name.split()))
        if matriculation.get('type', {}).get('code') == 'TRN':
            sis_profile['transfer'] = True
    if not sis_profile.get('transfer'):
        sis_profile['transfer'] = False


def merge_sis_profile_names(sis_student_api_feed, sis_profile):
    for name in sis_student_api_feed.get('names', []):
        code = name.get('type', {}).get('code')
        if code == 'PRF':
            sis_profile['preferredName'] = vacuum_whitespace(name.get('formattedName'))
        elif code == 'PRI':
            sis_profile['primaryName'] = vacuum_whitespace(name.get('formattedName'))
        if 'primaryName' in sis_profile and 'preferredName' in sis_profile:
            break


def merge_sis_profile_phones(sis_student_api_feed, sis_profile):
    phones_by_code = {
        phone.get('type', {}).get('code'): phone.get('number')
        for phone in sis_student_api_feed.get('phones', [])
    }
    sis_profile['phoneNumber'] = phones_by_code.get('CELL') or phones_by_code.get('LOCL') or phones_by_code.get('HOME')


def merge_sis_profile_plans(academic_status, sis_profile):
    sis_profile['plans'] = []
    for student_plan in academic_status.get('studentPlans', []):
        academic_plan = student_plan.get('academicPlan', {})
        # SIS majors come in five flavors.
        if academic_plan.get('type', {}).get('code') not in ['MAJ', 'SS', 'SP', 'HS', 'CRT']:
            continue
        plan = academic_plan.get('plan', {})
        major = plan.get('description')
        plan_feed = {
            'degreeProgramUrl': degree_program_url_for_major(major),
            'description': major,
        }
        # Find the latest expected graduation term from any plan.
        expected_graduation_term = student_plan.get('expectedGraduationTerm', {}).get('id')
        if expected_graduation_term and expected_graduation_term > sis_profile.get('expectedGraduationTerm', {}).get('id', '0'):
            sis_profile['expectedGraduationTerm'] = {
                'id': expected_graduation_term,
                'name': term_name_for_sis_id(expected_graduation_term),
            }
        # Add program unless plan code indicates undeclared.
        if plan.get('code') != '25000U':
            program = student_plan.get('academicPlan', {}).get('academicProgram', {}).get('program', {})
            plan_feed['program'] = program.get('description')
        # Add plan unless it's a duplicate.
        if not next((p for p in sis_profile['plans'] if p.get('description') == plan_feed.get('description')), None):
            sis_profile['plans'].append(plan_feed)
