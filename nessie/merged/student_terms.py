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

from itertools import groupby
import operator

from flask import current_app as app
from nessie.externals import s3
from nessie.lib import berkeley, queries


def upload_student_term_maps(advisees_by_sid):
    (enrollment_terms_map, canvas_site_map) = generate_student_term_maps(advisees_by_sid)
    feed_path = app.config['LOCH_S3_BOAC_ANALYTICS_DATA_PATH'] + '/feeds/'
    for term_id, enrollment_term_map in enrollment_terms_map.items():
        s3.upload_json(enrollment_term_map, feed_path + f'enrollment_term_map_{term_id}.json')
    for term_id, canvas_site in canvas_site_map.items():
        s3.upload_json(canvas_site, feed_path + f'canvas_site_map_{term_id}.json')


def generate_student_term_maps(advisees_by_sid):
    # Our mission is to produce 1) a dictionary of enrollment terms indexed by term_id and sid; 2) a dictionary
    # of Canvas sites indexed by Canvas course id.
    enrollment_terms_map = get_sis_enrollments()
    merge_dropped_classes(enrollment_terms_map)
    merge_term_gpas(enrollment_terms_map)
    # Track the results of course-site-level queries to avoid requerying.
    (canvas_site_map, advisee_site_map) = get_canvas_site_maps()
    merge_memberships_into_site_map(canvas_site_map)
    merge_canvas_data(canvas_site_map, advisee_site_map, enrollment_terms_map, advisees_by_sid)
    return enrollment_terms_map, canvas_site_map


def get_sis_enrollments():
    sis_enrollments = queries.get_all_advisee_sis_enrollments()
    return map_sis_enrollments(sis_enrollments)


def map_sis_enrollments(sis_enrollments):
    student_enrollments_map = {}
    for key, all_sids_grp in groupby(sis_enrollments, operator.itemgetter('sis_term_id')):
        term_id = str(key)
        term_name = berkeley.term_name_for_sis_id(term_id)
        student_enrollments_map[term_id] = {}
        for sid, all_enrs_grp in groupby(all_sids_grp, operator.itemgetter('sid')):
            term_enrollments = merge_enrollment(all_enrs_grp, term_id, term_name)
            student_enrollments_map[term_id][sid] = term_enrollments
    return student_enrollments_map


def merge_dropped_classes(all_advisees_terms_map):
    all_drops = queries.get_all_advisee_enrollment_drops()
    for key, sids_grp in groupby(all_drops, key=operator.itemgetter('sis_term_id')):
        term_id = str(key)
        for sid, enrs_grp in groupby(sids_grp, operator.itemgetter('sid')):
            student_term = all_advisees_terms_map.get(term_id, {}).get(sid)
            # When a student has begun enrolling for classes but then decides not to attend (or to withdraw),
            # the SIS DB will contain nothing but "dropped" sections. CalCentral does not show such terms
            # as part of the student's academic history.
            if student_term:
                drops = []
                for row in list(enrs_grp):
                    drops.append({
                        'displayName': row['sis_course_name'],
                        'component': row['sis_instruction_format'],
                        'sectionNumber': row['sis_section_num'],
                        'withdrawAfterDeadline': (row['grade'] == 'W'),
                    })
                student_term['droppedSections'] = drops
    return all_advisees_terms_map


def merge_term_gpas(all_advisees_terms_map):
    all_gpas = queries.get_all_advisee_term_gpas() or []
    for key, term_gpa_rows in groupby(all_gpas, operator.itemgetter('term_id')):
        term_id = str(key)
        for term_gpa_row in term_gpa_rows:
            sid = term_gpa_row['sid']
            student_term = all_advisees_terms_map.get(term_id, {}).get(sid)
            if student_term:
                student_term['termGpa'] = {
                    'gpa': float(term_gpa_row['gpa']),
                    'unitsTakenForGpa': float(term_gpa_row['units_taken_for_gpa']),
                }
    return all_advisees_terms_map


def get_canvas_site_maps():
    canvas_site_map = {}
    advisee_site_map = {}
    canvas_sites = queries.get_advisee_enrolled_canvas_sites()
    for canvas_course_term, canvas_sites_grp in groupby(canvas_sites, operator.itemgetter('canvas_course_term')):
        sis_term_id = berkeley.sis_term_id_for_name(canvas_course_term)
        canvas_site_map[sis_term_id] = {}
        for row in canvas_sites_grp:
            canvas_site_id = row['canvas_course_id']
            sis_sections = row.get('sis_section_ids', [])
            if sis_sections:
                # The SIS-derived feeds tend to deliver section IDs as integers rather than strings.
                sis_sections = [int(s) for s in sis_sections.split(',')]
            canvas_site_map[sis_term_id][canvas_site_id] = {
                'canvasCourseId': row['canvas_course_id'],
                'courseName': row.get('canvas_course_name'),
                'courseCode': row.get('canvas_course_code'),
                'courseTerm': canvas_course_term,
                'enrollments': [],
                'adviseeEnrollments': [],
                'sis_sections': sis_sections,
            }
            if not advisee_site_map.get(sis_term_id):
                advisee_site_map[sis_term_id] = {}
            sids = row.get('advisee_sids', [])
            if sids:
                sids = sids.split(',')
                for sid in sids:
                    if not advisee_site_map[sis_term_id].get(sid):
                        advisee_site_map[sis_term_id][sid] = []
                    advisee_site_map[sis_term_id][sid].append({
                        'canvas_course_id': canvas_site_id,
                    })
    return canvas_site_map, advisee_site_map


def merge_memberships_into_site_map(site_map):
    # Collect the bCourses enrollments of interest.
    canvas_enrollments = queries.get_all_enrollments_in_advisee_canvas_sites()
    for key, group in groupby(canvas_enrollments, key=operator.itemgetter('canvas_course_id')):
        canvas_site_id = key
        enrollments = list(group)
        sis_term_id = berkeley.sis_term_id_for_name(enrollments[0].get('canvas_course_term'))
        site = site_map.get(sis_term_id, {}).get(canvas_site_id)
        if site:
            site['enrollments'] = enrollments
        else:
            app.logger.warn(f'Did not find canvas_course_id {canvas_site_id} in site map for term {sis_term_id}')
    return site_map


def merge_canvas_data(canvas_site_map, advisee_site_map, all_advisees_terms_map, advisees_by_sid):
    for (term_id, all_sids) in advisee_site_map.items():
        canvas_sites_for_term = canvas_site_map.get(term_id, {})
        for (sid, sites) in all_sids.items():
            canvas_user_id = advisees_by_sid.get(sid, {}).get('canvas_user_id')
            term_feed = all_advisees_terms_map.get(term_id, {}).get(sid)
            if not term_feed:
                continue
            for membership in sites:
                merge_canvas_site_membership(membership, sid, canvas_user_id, term_feed, canvas_sites_for_term)
    return all_advisees_terms_map


def merge_canvas_site_membership(membership, sid, canvas_user_id, term_feed, canvas_sites_for_term):
    enrollments_matched = set()
    canvas_course_id = membership['canvas_course_id']
    canvas_site = canvas_sites_for_term.get(canvas_course_id)
    if not canvas_site:
        app.logger.warn(f'canvas_course_id {canvas_course_id} found in SID {sid} memberships but not in site map for term')
        return
    canvas_sections = canvas_site['sis_sections']
    if not canvas_sections:
        return
    canvas_site_element = {
        'canvasCourseId': canvas_site['canvasCourseId'],
        'courseName': canvas_site['courseName'],
        'courseCode': canvas_site['courseCode'],
        'courseTerm': canvas_site['courseTerm'],
        'analytics': {},
    }
    if canvas_user_id:
        canvas_site['adviseeEnrollments'].append(canvas_user_id)
    for canvas_ccn in canvas_sections:
        # There is no particularly intuitive unique identifier for a 'class enrollment', and so we resort to
        # list position.
        for index, enrollment in enumerate(term_feed['enrollments']):
            for sis_section in enrollment['sections']:
                if canvas_ccn == sis_section.get('ccn'):
                    sis_section['canvasCourseIds'] = sis_section.get('canvasCourseIds', [])
                    sis_section['canvasCourseIds'].append(canvas_course_id)
                    # Do not add the same site multiple times to the same enrollment.
                    if index not in enrollments_matched:
                        enrollments_matched.add(index)
                        enrollment['canvasSites'].append(canvas_site_element)
    if not enrollments_matched:
        term_feed['unmatchedCanvasSites'].append(canvas_site_element)


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
            enrollments_by_class[class_name]['grade'] = section_feed['grade']
            enrollments_by_class[class_name]['midtermGrade'] = section_feed['midtermGrade']
            enrollments_by_class[class_name]['gradingBasis'] = section_feed['gradingBasis']
            enrollments_by_class[class_name]['units'] = section_feed['units']

    enrollments_feed = sorted(enrollments_by_class.values(), key=lambda x: x['displayName'])
    # Whenever we have floating arithmetic, we can expect floating errors.
    enrolled_units = round(enrolled_units, 2)
    sort_sections(enrollments_feed)
    term_feed = {
        'termId': term_id,
        'termName': term_name,
        'enrollments': enrollments_feed,
        'enrolledUnits': enrolled_units,
        'unmatchedCanvasSites': [],
    }
    return term_feed


def sis_enrollment_class_feed(enrollment):
    return {
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
