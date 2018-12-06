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

from itertools import groupby
import operator

from nessie.lib import berkeley, queries


def get_merged_enrollment_terms(uid, sid, term_ids, canvas_courses_feed, canvas_site_map):
    enrollment_results = queries.get_sis_enrollments(uid) or []
    enrollments_by_term = {}
    for key, group in groupby(enrollment_results, key=operator.itemgetter('sis_term_id')):
        enrollments_by_term[str(key)] = list(group)
    term_feeds = {}
    for term_id in term_ids:
        enrollments = enrollments_by_term.get(term_id, [])
        term_name = berkeley.term_name_for_sis_id(term_id)
        term_feed = merge_enrollment(enrollments, term_id, term_name)
        term_feed['droppedSections'] = get_dropped_sections(sid, term_id)
        for site in canvas_courses_feed:
            merge_canvas_course_site(term_feed, site, canvas_site_map)
        sort_canvas_course_sites(term_feed)
        term_feeds[term_id] = term_feed
    return term_feeds


def get_canvas_courses_feed(uid):
    courses = queries.get_student_canvas_courses(uid) or []
    if not courses:
        return []
    return [canvas_course_feed(course) for course in courses]


def merge_canvas_site_map(canvas_site_map, canvas_courses_feed):
    unmapped_canvas_course_ids = []
    for course in canvas_courses_feed:
        str_id = str(course['canvasCourseId'])
        if str_id not in canvas_site_map:
            unmapped_canvas_course_ids.append(str_id)
            canvas_site_map[str_id] = {
                'enrollments': [],
                'sis_sections': [],
            }
    if not unmapped_canvas_course_ids:
        return
    score_results = queries.get_canvas_course_scores(unmapped_canvas_course_ids)
    for key, group in groupby(score_results, key=operator.itemgetter('course_id')):
        canvas_site_map[str(key)]['enrollments'] = list(group)
    section_results = queries.get_sis_sections_for_canvas_courses(unmapped_canvas_course_ids)
    for key, group in groupby(section_results, key=operator.itemgetter('canvas_course_id')):
        canvas_site_map[str(key)]['sis_sections'] = list(group)


def get_dropped_sections(sid, term_id):
    drops = []
    for row in queries.get_enrollment_drops(sid, term_id):
        drops.append({
            'displayName': row['sis_course_name'],
            'component': row['sis_instruction_format'],
            'sectionNumber': row['sis_section_num'],
            'withdrawAfterDeadline': (row['grade'] == 'W'),
        })
    return drops


def merge_enrollment(enrollments, term_id, term_name):
    enrollments_by_class = {}
    term_section_ids = {}
    enrolled_units = 0
    for enrollment in enrollments:
        # Skip this class section if we've seen it already.
        section_id = enrollment.get('sis_section_id')
        if section_id in term_section_ids:
            continue
        term_section_ids[section_id] = True

        section_feed = {
            'ccn': enrollment['sis_section_id'],
            'component': enrollment['sis_instruction_format'],
            'sectionNumber': enrollment['sis_section_num'],
            'enrollmentStatus': enrollment['sis_enrollment_status'],
            'units': enrollment['units'],
            'gradingBasis': berkeley.translate_grading_basis(enrollment['grading_basis']),
            'grade': enrollment['grade'],
            'midtermGrade': enrollment['grade_midterm'],
            'primary': enrollment['sis_primary'],
        }

        # The SIS enrollments API gives us no better unique identifier than the course display name.
        class_name = enrollment['sis_course_name']
        # If we haven't seen this class name before, we create a new feed entry for it.
        if class_name not in enrollments_by_class:
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
            enrollments_by_class[class_name]['grade'] = section_feed['grade']
            enrollments_by_class[class_name]['midtermGrade'] = section_feed['midtermGrade']
            enrollments_by_class[class_name]['gradingBasis'] = section_feed['gradingBasis']
            enrollments_by_class[class_name]['units'] = section_feed['units']

    enrollments_feed = sorted(enrollments_by_class.values(), key=lambda x: x['displayName'])
    sort_sections(enrollments_feed)
    term_feed = {
        'termId': term_id,
        'termName': term_name,
        'enrollments': enrollments_feed,
        'enrolledUnits': enrolled_units,
        'unmatchedCanvasSites': [],
    }
    return term_feed


def check_for_multiple_primary_sections(enrollment, class_name, enrollments_by_class, section_feed):
    # If we have seen this class name before, in most cases we'll just append the new section feed to the
    # existing class feed. However, because multiple concurrent primary-section enrollments aren't distinguished by
    # class name, we need to do an extra check for that case.
    # TODO In the rare case of multiple-primary enrollments with associated secondary sections, secondary section
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


def canvas_course_feed(course):
    return {
        'canvasCourseId': course.get('canvas_course_id'),
        'courseName': course.get('canvas_course_name'),
        'courseCode': course.get('canvas_course_code'),
        'courseTerm': course.get('canvas_course_term'),
    }


def is_enrolled_primary_section(section_feed):
    return section_feed['primary'] and section_feed['enrollmentStatus'] == 'E'


def merge_canvas_course_site(term_feed, site, canvas_site_map):
    if site['courseTerm'] != term_feed['termName']:
        return
    enrollments_matched = set()
    canvas_sections = canvas_site_map.get(str(site['canvasCourseId']), {}).get('sis_sections')
    if not canvas_sections:
        return
    for canvas_section in canvas_sections:
        canvas_ccn = canvas_section['sis_section_id']
        if not canvas_ccn:
            continue
        # There is no particularly intuitive unique identifier for a 'class enrollment', and so we resort to
        # list position.
        for index, enrollment in enumerate(term_feed['enrollments']):
            for sis_section in enrollment['sections']:
                if canvas_ccn == sis_section.get('ccn'):
                    sis_section['canvasCourseIds'] = sis_section.get('canvasCourseIds', [])
                    sis_section['canvasCourseIds'].append(site['canvasCourseId'])
                    # Do not add the same site multiple times to the same enrollment.
                    if index not in enrollments_matched:
                        enrollments_matched.add(index)
                        enrollment['canvasSites'].append(site)
    if not enrollments_matched:
        term_feed['unmatchedCanvasSites'].append(site)


def sis_enrollment_class_feed(enrollment):
    return {
        'displayName': enrollment['sis_course_name'],
        'title': enrollment['sis_course_title'],
        'canvasSites': [],
        'sections': [],
    }


def sort_canvas_course_sites(term_feed):
    for enrollment in term_feed['enrollments']:
        enrollment['canvasSites'] = sorted(enrollment['canvasSites'], key=lambda x: x['canvasCourseId'])
        for section in enrollment['sections']:
            section['canvasCourseIds'] = sorted(section.get('canvasCourseIds', []))
    term_feed['unmatchedCanvasSites'] = sorted(term_feed['unmatchedCanvasSites'], key=lambda x: x['canvasCourseId'])


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
