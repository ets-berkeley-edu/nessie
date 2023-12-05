"""
Copyright ©2022. The Regents of the University of California (Regents). All Rights Reserved.

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

from nessie.lib import berkeley
from nessie.lib.util import to_boolean, to_float


def empty_term_feed(term_id, term_name):
    return {
        'termId': term_id,
        'termName': term_name,
        'enrollments': [],
        'enrolledUnits': 0,
    }


def append_drops(term_feed, drops):
    term_feed['droppedSections'] = []
    for row in drops:
        term_feed['droppedSections'].append({
            'component': row['sis_instruction_format'],
            'displayName': row['sis_course_name'],
            'dropDate': str(row['drop_date']) if row['drop_date'] else None,
            'instructionMode': row['sis_instruction_mode'],
            'sectionNumber': row['sis_section_num'],
            'withdrawAfterDeadline': (row['grade'] == 'W'),
        })


def append_term_gpa(term_feed, term_gpa_rows):
    if term_feed and term_feed.get('enrolledUnits'):
        # In the case of multiple results per term id and SID, SQL ordering ensures that a UGRD career, if present,
        # will get the last word.
        for row in term_gpa_rows:
            term_feed['termGpa'] = {
                'gpa': float(row['gpa']),
                'unitsTakenForGpa': float(row['units_taken_for_gpa']),
            }


def merge_canvas_site_memberships(term_feed, canvas_site_rows):
    for site_row in canvas_site_rows:
        enrollments_matched = set()
        canvas_site_feed = json.loads(site_row['feed'])
        section_ids = site_row['sis_section_ids'] or []
        if section_ids:
            section_ids = [int(s) for s in section_ids.split(',')]
        for section_id in section_ids:
            # There is no particularly intuitive unique identifier for a 'class enrollment', and so we resort to
            # list position.
            for index, enrollment in enumerate(term_feed['enrollments']):
                for sis_section in enrollment['sections']:
                    if section_id == sis_section.get('ccn'):
                        sis_section['canvasCourseIds'] = sis_section.get('canvasCourseIds', [])
                        sis_section['canvasCourseIds'].append(canvas_site_feed['canvasCourseId'])
                        # Do not add the same site multiple times to the same enrollment.
                        if index not in enrollments_matched:
                            enrollments_matched.add(index)
                            enrollment['canvasSites'].append(canvas_site_feed)


def check_for_multiple_primary_sections(enrollment, class_name, enrollments_by_class, section_feed):
    # If we have seen this class name before, in most cases we'll just append the new section feed to the
    # existing class feed. However, because multiple concurrent primary-section enrollments aren't distinguished by
    # class name, we need to do an extra check for that case.
    # In the rare case of multiple-primary enrollments with associated secondary sections, secondary section
    # handling will be unpredictable.
    existing_primary = next((sec for sec in enrollments_by_class[class_name]['sections'] if is_enrolled_primary_section(sec)), None)
    # If we do indeed have two primary sections under the same class name, disambiguate them.
    if existing_primary:
        # First, revise the existing class feed to include section number.
        component = existing_primary['component']
        section = existing_primary['sectionNumber']
        disambiguated_class_name = f'{class_name} {component} {section}'
        enrollments_by_class[class_name]['displayName'] = disambiguated_class_name
        enrollments_by_class[disambiguated_class_name] = enrollments_by_class[class_name]
        del enrollments_by_class[class_name]
        # Now create a new class feed, also with section number, for our new primary section.
        component = section_feed['component']
        section = section_feed['sectionNumber']
        class_name = f'{class_name} {component} {section}'
        enrollments_by_class[class_name] = sis_enrollment_class_feed(enrollment)
        enrollments_by_class[class_name]['displayName'] = class_name
    return class_name


def is_enrolled_primary_section(section_feed):
    return section_feed['primary'] and section_feed['enrollmentStatus'] == 'E'


def merge_enrollment(enrollments, term_id, term_name):
    enrollments_by_class = {}
    term_section_ids = {}
    incompletes = []
    enrolled_units = 0
    max_term_units_allowed = None
    min_term_units_allowed = None

    for enrollment in enrollments:
        # Skip this class section if we've seen it already.
        section_id = int(enrollment.get('sis_section_id'))
        if section_id in term_section_ids:
            continue
        term_section_ids[section_id] = True

        section_feed = {
            'ccn': section_id,
            'component': enrollment['sis_instruction_format'],
            'courseRequirements': enrollment['course_requirements'] and json.loads(enrollment['course_requirements']),
            'enrollmentStatus': enrollment['sis_enrollment_status'],
            'grade': enrollment['grade'],
            'gradingBasis': berkeley.translate_grading_basis(enrollment['grading_basis']),
            'instructionMode': enrollment['sis_instruction_mode'],
            'midtermGrade': (enrollment['grade_midterm'] or None),
            'primary': to_boolean(enrollment['sis_primary']),
            'sectionNumber': enrollment['sis_section_num'],
            'units': to_float(enrollment['units']),
        }

        if len(enrollment['incomplete_status_code'] or ''):
            section_feed.update({
                'incompleteComments': enrollment['incomplete_comments'],
                'incompleteFrozenFlag': enrollment['incomplete_frozen_flag'],
                'incompleteLapseGradeDate': enrollment['incomplete_lapse_grade_date'],
                'incompleteLapseToGrade': enrollment['incomplete_lapse_to_grade'],
                'incompleteStatusCode': enrollment['incomplete_status_code'],
                'incompleteStatusDescription': enrollment['incomplete_status_description'],
            })
            incompletes.append({
                'status': enrollment['incomplete_status_code'],
                'frozen': enrollment['incomplete_frozen_flag'],
                'lapseDate': enrollment['incomplete_lapse_grade_date'],
                'grade': enrollment['grade'],
            })

        # The SIS enrollments API gives us no better unique identifier than the course display name.
        class_name = enrollment['sis_course_name']

        # If we haven't seen this class name before and this is a primary section, we create a new feed entry for it.
        if class_name not in enrollments_by_class:
            # If there is no primary section, then the student probably withdrew from the class, leaving the non-primary
            # enrollments as noise.
            if not section_feed['primary']:
                continue
            enrollments_by_class[class_name] = sis_enrollment_class_feed(enrollment)

        if is_enrolled_primary_section(section_feed):
            class_name = check_for_multiple_primary_sections(enrollment, class_name, enrollments_by_class, section_feed)

        enrollments_by_class[class_name]['sections'].append(section_feed)
        if is_enrolled_primary_section(section_feed):
            enrolled_units += section_feed['units']
        # Since only one enrolled primary section is allowed per class, it's safe to associate units and grade
        # information with the class as well as the section. If a primary section is waitlisted, do the same
        # association unless we've already done it with a different section (this case may not arise in practice).
        if is_enrolled_primary_section(section_feed) or (
                section_feed['primary'] and 'units' not in enrollments_by_class[class_name]):
            enrollments_by_class[class_name]['courseRequirements'] = section_feed['courseRequirements']
            enrollments_by_class[class_name]['grade'] = section_feed['grade']
            enrollments_by_class[class_name]['midtermGrade'] = section_feed['midtermGrade']
            enrollments_by_class[class_name]['gradingBasis'] = section_feed['gradingBasis']
            enrollments_by_class[class_name]['units'] = section_feed['units']
        if max_term_units_allowed is None:
            max_term_units_allowed = to_float(enrollment['max_term_units_allowed'])
            min_term_units_allowed = to_float(enrollment['min_term_units_allowed'])

    enrollments_feed = sorted(enrollments_by_class.values(), key=lambda x: x['displayName'])
    # Whenever we have floating arithmetic, we can expect floating errors.
    enrolled_units = round(enrolled_units, 2)
    sort_sections(enrollments_feed)
    term_feed = {
        'termId': term_id,
        'termName': term_name,
        'enrollments': enrollments_feed,
        'enrolledUnits': to_float(enrolled_units),
        'maxTermUnitsAllowed': max_term_units_allowed,
        'minTermUnitsAllowed': min_term_units_allowed,
    }
    return term_feed, incompletes


def sis_enrollment_class_feed(enrollment):
    return {
        'academicCareer': enrollment['academic_career'],
        'displayName': enrollment['sis_course_name'],
        'title': enrollment['sis_course_title'],
        'canvasSites': [],
        'sections': [],
    }


def sort_sections(enrollments_feed):
    # Sort by 1) enrollment status, 2) units descending, 3) section number.
    def section_key(sec):
        enrollment_status_keys = {
            'E': 0,
            'W': 1,
            'D': 2,
        }
        units_key = -1 * sec['units']
        return (
            enrollment_status_keys.get(sec['enrollmentStatus']),
            units_key,
            sec['sectionNumber'],
        )
    for enrollment in enrollments_feed:
        enrollment['sections'].sort(key=section_key)
