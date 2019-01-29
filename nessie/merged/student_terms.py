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
from nessie.lib import berkeley, queries
from nessie.lib.analytics import mean_course_analytics_for_user


def generate_enrollment_terms_map(advisee_sids_map):
    # Our mission is to produce student_enrollment_terms rows indexed by sid and term_id, and so that should be our
    # final sort order.
    enrollment_terms_map = get_sis_enrollments()
    merge_dropped_classes(enrollment_terms_map)
    merge_term_gpas(enrollment_terms_map)
    # Track the results of course-site-level queries to avoid requerying.
    (canvas_site_map, advisee_site_map) = get_canvas_site_map()
    merge_memberships_into_site_map(canvas_site_map)
    merge_canvas_data(canvas_site_map, advisee_site_map, enrollment_terms_map)
    merge_all_analytics_data(advisee_sids_map, canvas_site_map, enrollment_terms_map)
    return enrollment_terms_map


def get_sis_enrollments():
    student_enrollments_map = {}
    sis_enrollments = queries.get_all_advisee_sis_enrollments()
    for sid, all_terms_grp in groupby(sis_enrollments, operator.itemgetter('sid')):
        student_enrollments_map[sid] = {}
        for key, all_enrs_grp in groupby(all_terms_grp, operator.itemgetter('sis_term_id')):
            term_id = str(key)
            term_name = berkeley.term_name_for_sis_id(term_id)
            term_enrollments = merge_enrollment(all_enrs_grp, term_id, term_name)
            student_enrollments_map[sid][term_id] = term_enrollments
    return student_enrollments_map


def merge_dropped_classes(all_advisees_terms_map):
    all_drops = queries.get_all_advisee_enrollment_drops()
    for sid, all_terms_grp in groupby(all_drops, operator.itemgetter('sid')):
        for key, enrs_grp in groupby(all_terms_grp, key=operator.itemgetter('sis_term_id')):
            term_id = str(key)
            student_term = all_advisees_terms_map.get(sid, {}).get(term_id)
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
    for sid, all_terms_grp in groupby(all_gpas, operator.itemgetter('sid')):
        for term_gpa in all_terms_grp:
            term_id = term_gpa['term_id']
            student_term = all_advisees_terms_map.get(sid, {}).get(term_id)
            if student_term:
                student_term['termGpa'] = {
                    'gpa': float(term_gpa['gpa']),
                    'unitsTakenForGpa': float(term_gpa['units_taken_for_gpa']),
                }
    return all_advisees_terms_map


def get_canvas_site_map():
    canvas_site_map = {}
    advisee_site_map = {}
    canvas_sites = queries.get_advisee_enrolled_canvas_sites()
    for row in canvas_sites:
        canvas_site_id = row['canvas_course_id']
        canvas_course_term = row.get('canvas_course_term')
        sis_sections = row.get('sis_section_ids', [])
        if sis_sections:
            # The SIS-derived feeds tend to deliver section IDs as integers rather than strings.
            sis_sections = [int(s) for s in sis_sections.split(',')]
        canvas_site_map[canvas_site_id] = {
            'canvasCourseId': row['canvas_course_id'],
            'courseName': row.get('canvas_course_name'),
            'courseCode': row.get('canvas_course_code'),
            'courseTerm': canvas_course_term,
            'enrollments': [],
            'sis_sections': sis_sections,
        }
        sis_term_id = berkeley.sis_term_id_for_name(canvas_course_term)
        sids = row.get('advisee_sids', [])
        if sids:
            sids = sids.split(',')
            for sid in sids:
                if not advisee_site_map.get(sid):
                    advisee_site_map[sid] = {}
                if not advisee_site_map[sid].get(sis_term_id):
                    advisee_site_map[sid][sis_term_id] = []
                advisee_site_map[sid][sis_term_id].append({
                    'canvas_course_id': canvas_site_id,
                })
    return canvas_site_map, advisee_site_map


def merge_memberships_into_site_map(site_map):
    # Collect the bCourses enrollments of interest.
    canvas_enrollments = queries.get_all_enrollments_in_advisee_canvas_sites()
    for key, group in groupby(canvas_enrollments, key=operator.itemgetter('canvas_course_id')):
        canvas_site_id = key
        site = site_map.get(canvas_site_id)
        if site:
            site['enrollments'] = list(group)
        else:
            app.logger.warn(f'Did not find canvas_course_id {canvas_site_id} in site map')
    return site_map


def merge_canvas_data(canvas_site_map, advisee_site_map, all_advisees_terms_map):
    for (sid, all_terms) in advisee_site_map.items():
        for (term_id, sites) in all_terms.items():
            term_feed = all_advisees_terms_map.get(sid, {}).get(term_id)
            if not term_feed:
                continue
            for membership in sites:
                enrollments_matched = set()
                canvas_course_id = membership['canvas_course_id']
                canvas_site = canvas_site_map.get(canvas_course_id)
                if not canvas_site:
                    app.logger.warn(f'canvas_course_id {canvas_course_id} found in SID {sid} memberships but not in site map')
                    continue
                canvas_sections = canvas_site['sis_sections']
                if not canvas_sections:
                    continue
                canvas_site_element = {
                    'canvasCourseId': canvas_site['canvasCourseId'],
                    'courseName': canvas_site['courseName'],
                    'courseCode': canvas_site['courseCode'],
                    'courseTerm': canvas_site['courseTerm'],
                    'analytics': {},
                }
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
            # Sort sites by Canvas course ID? Shouldn't be necessary, given the original query order.
    return all_advisees_terms_map


def merge_all_analytics_data(advisee_sids_map, canvas_site_map, all_advisees_terms_map):
    sids_without_merged_analytics = {sid for (sid, val) in advisee_sids_map.items() if val.get('canvas_user_id')}

    # First, handle those advisees who are graced with billions and billions of assignment submission stats.
    all_counts = queries.get_advisee_submissions_comparisons()
    for (canvas_user_id, sid), sites_grp in groupby(all_counts, key=operator.itemgetter('reference_user_id', 'sid')):
        app.logger.debug(f'Merging analytics for SID {sid}')
        sids_without_merged_analytics.discard(sid)
        advisee_terms_map = all_advisees_terms_map.get(sid)
        if not advisee_terms_map:
            # Nothing to merge.
            continue
        relative_submission_counts = {}
        for canvas_course_id, subs_grp in groupby(sites_grp, key=operator.itemgetter('canvas_course_id')):
            relative_submission_counts[canvas_course_id] = list(subs_grp)
        merge_advisee_analytics(advisee_terms_map, canvas_user_id, relative_submission_counts, canvas_site_map)

    # Then take care of the few students who have never seen a Canvas assignment.
    for sid in sids_without_merged_analytics:
        app.logger.debug(f'Merging analytics for SID {sid}')
        canvas_user_id = advisee_sids_map[sid].get('canvas_user_id')
        advisee_terms_map = all_advisees_terms_map.get(sid)
        if not advisee_terms_map:
            # Nothing to merge.
            continue
        merge_advisee_analytics(advisee_terms_map, canvas_user_id, {}, canvas_site_map)

    return all_advisees_terms_map


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


def merge_advisee_analytics(terms_map, canvas_user_id, relative_submission_counts, canvas_site_map):
    for (term_id, term_feed) in terms_map.items():
        canvas_courses = []
        for enrollment in term_feed.get('enrollments', []):
            canvas_courses += enrollment['canvasSites']
        canvas_courses += term_feed.get('unmatchedCanvasSites', [])
        # Decorate the Canvas courses list with per-course statistics and return summary statistics.
        term_feed['analytics'] = mean_course_analytics_for_user(
            canvas_courses,
            canvas_user_id,
            relative_submission_counts,
            canvas_site_map,
        )
    return terms_map


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
