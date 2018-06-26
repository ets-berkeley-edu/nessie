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
from nessie.lib.mockingdata import fixture

# Lazy init to support testing.
data_loch_db = None


def boac_schema():
    return app.config['REDSHIFT_SCHEMA_BOAC']


def intermediate_schema():
    return app.config['REDSHIFT_SCHEMA_INTERMEDIATE']


@fixture('query_canvas_course_scores_{course_id}.csv')
def get_canvas_course_scores(course_id):
    sql = f"""SELECT
                canvas_user_id,
                current_score,
                EXTRACT(EPOCH FROM last_activity_at) AS last_activity_at,
                sis_enrollment_status
              FROM {boac_schema()}.course_enrollments
              WHERE course_id={course_id}
              ORDER BY canvas_user_id
        """
    return redshift.fetch(sql)


@fixture('query_sis_enrollments_{uid}_{term_id}.csv')
def get_sis_enrollments(uid, term_id):
    sql = f"""SELECT
                  enr.grade, enr.units, enr.grading_basis, enr.sis_enrollment_status, enr.sis_term_id, enr.ldap_uid,
                  crs.sis_course_title, crs.sis_course_name,
                  crs.sis_section_id, crs.sis_primary, crs.sis_instruction_format, crs.sis_section_num
              FROM {intermediate_schema()}.sis_enrollments enr
              JOIN {intermediate_schema()}.course_sections crs
                  ON crs.sis_section_id = enr.sis_section_id
                  AND crs.sis_term_id = enr.sis_term_id
              WHERE enr.ldap_uid = {uid}
                  AND enr.sis_enrollment_status != 'D'
                  AND enr.sis_term_id = {term_id}
              ORDER BY crs.sis_course_name, crs.sis_primary DESC, crs.sis_instruction_format, crs.sis_section_num
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


@fixture('query_sis_sections_in_canvas_course_{canvas_course_id}.csv')
def get_sis_sections_in_canvas_course(canvas_course_id):
    # The GROUP BY clause eliminates duplicates when multiple site sections include the same SIS class section.
    sql = f"""SELECT sis_section_id
        FROM {intermediate_schema()}.course_sections
        WHERE canvas_course_id={canvas_course_id}
        GROUP BY sis_section_id
        """
    return redshift.fetch(sql)


@fixture('query_student_canvas_courses_{uid}.csv')
def get_student_canvas_courses(uid):
    sql = f"""SELECT DISTINCT enr.canvas_course_id, cs.canvas_course_name, cs.canvas_course_code, cs.canvas_course_term
        FROM {intermediate_schema()}.active_student_enrollments enr
        JOIN {intermediate_schema()}.course_sections cs
            ON cs.canvas_course_id = enr.canvas_course_id
        WHERE enr.uid = {uid}
        """
    return redshift.fetch(sql)


@fixture('query_submissions_turned_in_relative_to_user_{course_id}_{user_id}.csv')
def get_submissions_turned_in_relative_to_user(course_id, user_id):
    sql = f"""SELECT canvas_user_id,
        COUNT(CASE WHEN
          assignment_status IN ('graded', 'late', 'on_time', 'submitted')
        THEN 1 ELSE NULL END) AS submissions_turned_in
        FROM {boac_schema()}.assignment_submissions_scores
        WHERE assignment_id IN
        (
          SELECT DISTINCT assignment_id FROM {boac_schema()}.assignment_submissions_scores
          WHERE canvas_user_id = {user_id} AND course_id = {course_id}
        )
        GROUP BY canvas_user_id
        HAVING count(*) = (
          SELECT count(*) FROM {boac_schema()}.assignment_submissions_scores
          WHERE canvas_user_id = {user_id} AND course_id = {course_id}
        )
        """
    return redshift.fetch(sql)


@fixture('query_user_for_uid_{uid}.csv')
def get_user_for_uid(uid):
    sql = f"""SELECT canvas_id, name, uid
        FROM {intermediate_schema()}.users
        WHERE uid = {uid}
        """
    return redshift.fetch(sql)
