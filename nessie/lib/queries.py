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

from flask import current_app as app
from nessie.externals import redshift
from nessie.lib.berkeley import term_name_for_sis_id
from nessie.lib.mockingdata import fixture

# Lazy init to support testing.
data_loch_db = None


def asc_schema():
    return app.config['REDSHIFT_SCHEMA_ASC']


def boac_schema():
    return app.config['REDSHIFT_SCHEMA_BOAC']


def calnet_schema():
    return app.config['REDSHIFT_SCHEMA_CALNET']


def coe_schema():
    return app.config['REDSHIFT_SCHEMA_COE']


def intermediate_schema():
    return app.config['REDSHIFT_SCHEMA_INTERMEDIATE']


def metadata_schema():
    return app.config['REDSHIFT_SCHEMA_METADATA']


def student_schema():
    return app.config['REDSHIFT_SCHEMA_STUDENT']


def get_all_student_ids():
    sql = f"""SELECT sid FROM {asc_schema()}.students
        UNION SELECT sid FROM {coe_schema()}.students"""
    return redshift.fetch(sql)


def get_calnet_profiles(sids=None):
    if sids:
        where_clause = f"WHERE sid=ANY('{{{','.join(sids)}}}')"
    else:
        where_clause = ''
    sql = f"""SELECT ldap_uid, sid, first_name, last_name
              FROM {calnet_schema()}.persons
              {where_clause}
        """
    return redshift.fetch(sql)


@fixture('query_canvas_course_scores.csv')
def get_canvas_course_scores(course_ids):
    sql = f"""SELECT
                course_id,
                canvas_user_id,
                current_score,
                EXTRACT(EPOCH FROM last_activity_at) AS last_activity_at,
                sis_enrollment_status
              FROM {boac_schema()}.course_enrollments
              WHERE course_id=ANY('{{{','.join(course_ids)}}}')
              ORDER BY course_id, canvas_user_id
        """
    return redshift.fetch(sql)


def get_enrolled_canvas_sites_for_term(term_id):
    sql = f"""SELECT DISTINCT enr.canvas_course_id
              FROM {intermediate_schema()}.active_student_enrollments enr
              JOIN {intermediate_schema()}.course_sections cs
                ON cs.canvas_course_id = enr.canvas_course_id
                AND cs.canvas_course_term = '{term_name_for_sis_id(term_id)}'
                AND enr.uid IN (SELECT uid FROM {student_schema()}.student_academic_status)
              ORDER BY canvas_course_id
        """
    return redshift.fetch(sql)


def get_enrolled_primary_sections_for_term(term_id):
    sql = f"""SELECT
                sec.sis_section_id,
                sec.sis_course_name,
                TRANSLATE(sec.sis_course_name, '&-, ', '') AS sis_course_name_compressed,
                sec.sis_course_title,
                sec.sis_instruction_format,
                sec.sis_section_num,
                LISTAGG(DISTINCT sec.instructor_name, ', ') WITHIN GROUP (ORDER BY sec.instructor_name) AS instructors
              FROM {intermediate_schema()}.sis_enrollments enr
              JOIN {intermediate_schema()}.sis_sections sec
                ON enr.sis_term_id = {term_id}
                AND sec.sis_term_id = {term_id}
                AND sec.is_primary = TRUE
                AND enr.sis_section_id = sec.sis_section_id
                AND enr.ldap_uid IN (SELECT uid FROM {student_schema()}.student_academic_status)
              GROUP BY sec.sis_section_id, sis_course_name, sis_course_title, sis_instruction_format, sis_section_num
              ORDER BY sec.sis_section_id
        """
    return redshift.fetch(sql)


@fixture('query_enrollment_drops_{csid}.csv')
def get_enrollment_drops(csid):
    sql = f"""SELECT dr.*
              FROM {intermediate_schema()}.sis_dropped_classes AS dr
              WHERE dr.sid = '{csid}'
              ORDER BY dr.sis_term_id DESC, dr.sis_course_name
            """
    return redshift.fetch(sql)


def get_sis_api_degree_progress(csid):
    sql = f"""SELECT feed from {student_schema()}.sis_api_degree_progress WHERE sid='{csid}'"""
    return redshift.fetch(sql)


def get_sis_api_profile(csid):
    sql = f"""SELECT feed from {student_schema()}.sis_api_profiles WHERE sid='{csid}'"""
    return redshift.fetch(sql)


@fixture('query_sis_enrollments_{uid}.csv')
def get_sis_enrollments(uid):
    sql = f"""SELECT
                  enr.grade, enr.grade_midterm, enr.units, enr.grading_basis, enr.sis_enrollment_status, enr.sis_term_id, enr.ldap_uid,
                  enr.sis_course_title, enr.sis_course_name,
                  enr.sis_section_id, enr.sis_primary, enr.sis_instruction_format, enr.sis_section_num
              FROM {intermediate_schema()}.sis_enrollments enr
              WHERE enr.ldap_uid = {uid}
              ORDER BY enr.sis_term_id DESC, enr.sis_course_name, enr.sis_primary DESC, enr.sis_instruction_format, enr.sis_section_num
        """
    return redshift.fetch(sql)


@fixture('query_sis_section_{term_id}_{sis_section_id}.csv')
def get_sis_section(term_id, sis_section_id):
    sql = f"""SELECT
                  sc.sis_term_id, sc.sis_section_id, sc.sis_course_title, sc.sis_course_name,
                  sc.is_primary, sc.sis_instruction_format, sc.sis_section_num, sc.allowed_units,
                  sc.instructor_uid, sc.instructor_name, sc.instructor_role_code,
                  sc.meeting_location, sc.meeting_days,
                  sc.meeting_start_time, sc.meeting_end_time, sc.meeting_start_date, sc.meeting_end_date
              FROM {intermediate_schema()}.sis_sections sc
              WHERE sc.sis_section_id = {sis_section_id}
                  AND sc.sis_term_id = {term_id}
              ORDER BY sc.meeting_days, sc.meeting_start_time, sc.meeting_end_time, sc.instructor_name
        """
    return redshift.fetch(sql)


@fixture('query_sis_sections_for_canvas_courses.csv')
def get_sis_sections_for_canvas_courses(canvas_course_ids):
    # The GROUP BY clause eliminates duplicates when multiple site sections include the same SIS class section.
    sql = f"""SELECT canvas_course_id, sis_section_id
        FROM {intermediate_schema()}.course_sections
        WHERE canvas_course_id = ANY('{{{','.join(canvas_course_ids)}}}')
        GROUP BY canvas_course_id, sis_section_id
        ORDER BY canvas_course_id
        """
    return redshift.fetch(sql)


@fixture('query_student_canvas_courses_{uid}.csv')
def get_student_canvas_courses(uid):
    sql = f"""SELECT DISTINCT enr.canvas_course_id, enr.canvas_course_name, enr.canvas_course_code, enr.canvas_course_term
        FROM {intermediate_schema()}.active_student_enrollments enr
        WHERE enr.uid = {uid}
        """
    return redshift.fetch(sql)


@fixture('query_submissions_turned_in_relative_to_user_{user_id}.csv')
def get_submissions_turned_in_relative_to_user(user_id):
    sql = f"""SELECT course_id, canvas_user_id, submissions_turned_in
              FROM {boac_schema()}.assignment_submissions_relative
              WHERE reference_user_id = {user_id}"""
    return redshift.fetch(sql)


def get_successfully_backfilled_students():
    sql = f"""SELECT sid
        FROM {metadata_schema()}.merged_feed_status
        WHERE term_id = 'all' AND status = 'success'"""
    return redshift.fetch(sql)


def get_term_gpas(sid):
    sql = f"""SELECT term_id, gpa, units_taken_for_gpa
              FROM {student_schema()}.student_term_gpas
              WHERE sid = {sid}
        """
    return redshift.fetch(sql)


@fixture('query_user_for_uid_{uid}.csv')
def get_user_for_uid(uid):
    sql = f"""SELECT canvas_id, name, uid
        FROM {intermediate_schema()}.users
        WHERE uid = {uid}
        """
    return redshift.fetch(sql)
