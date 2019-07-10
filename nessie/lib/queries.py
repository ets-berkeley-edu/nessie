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

from flask import current_app as app
from nessie.externals import rds, redshift, s3
from nessie.lib.berkeley import canvas_terms, reverse_term_ids, term_name_for_sis_id
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


def l_s_schema():
    return app.config['REDSHIFT_SCHEMA_L_S']


def metadata_schema():
    return app.config['RDS_SCHEMA_METADATA']


def physics_schema():
    return app.config['REDSHIFT_SCHEMA_PHYSICS']


def student_schema():
    return app.config['REDSHIFT_SCHEMA_STUDENT']


def get_all_student_ids():
    l_s_clause = f'UNION SELECT sid FROM {l_s_schema()}.students' if app.config['L_AND_S_ENABLED'] else ''
    sql = f"""SELECT sid FROM {asc_schema()}.students
        UNION SELECT sid FROM {coe_schema()}.students
        {l_s_clause}
        UNION SELECT sid FROM {physics_schema()}.students"""
    return redshift.fetch(sql)


def get_advisee_ids(csids=None):
    csid_filter = 'WHERE sid = ANY(%s)' if csids is not None else ''
    sql = f"""SELECT ldap_uid, sid
              FROM {calnet_schema()}.persons
              {csid_filter}
              ORDER BY sid"""
    return redshift.fetch(sql, params=(csids,))


@fixture('query_advisee_student_profile_feeds.csv')
def get_advisee_student_profile_feeds():
    sql = f"""SELECT DISTINCT ldap.ldap_uid, ldap.sid, ldap.first_name, ldap.last_name,
                us.canvas_id AS canvas_user_id, us.name AS canvas_user_name,
                sis.feed AS sis_profile_feed,
                deg.feed AS degree_progress_feed,
                demog.feed AS demographics_feed
                reg.feed AS last_registration_feed
              FROM {calnet_schema()}.persons ldap
              LEFT JOIN {intermediate_schema()}.users us
                ON us.uid = ldap.ldap_uid
              LEFT JOIN {student_schema()}.sis_api_profiles sis
                ON sis.sid = ldap.sid
              LEFT JOIN {student_schema()}.sis_api_degree_progress deg
                ON deg.sid = ldap.sid
              LEFT JOIN {boac_schema()}.student_demographics demog
                ON demog.sid = ldap.sid
              LEFT JOIN {student_schema()}.student_last_registrations reg
                ON reg.sid = ldap.sid
              ORDER BY ldap.sid
        """
    return redshift.fetch(sql)


@fixture('query_advisee_enrolled_canvas_sites.csv')
def get_advisee_enrolled_canvas_sites():
    sql = f"""SELECT enr.canvas_course_id, enr.canvas_course_name, enr.canvas_course_code, enr.canvas_course_term,
          LISTAGG(DISTINCT ldap.sid, ',') AS advisee_sids,
          LISTAGG(DISTINCT cs.sis_section_id, ',') AS sis_section_ids
        FROM {intermediate_schema()}.active_student_enrollments enr
        JOIN {calnet_schema()}.persons ldap
          ON enr.uid = ldap.ldap_uid
        JOIN {intermediate_schema()}.course_sections cs
          ON cs.canvas_course_id = enr.canvas_course_id
        WHERE enr.canvas_course_term=ANY('{{{','.join(canvas_terms())}}}')
        GROUP BY enr.canvas_course_id, enr.canvas_course_name, enr.canvas_course_code, enr.canvas_course_term
        ORDER BY enr.canvas_course_term, enr.canvas_course_id
        """
    return redshift.fetch(sql)


def get_advisee_sids_with_photos():
    sql = f"""SELECT sid
        FROM {metadata_schema()}.photo_import_status
        WHERE status = 'success'"""
    return rds.fetch(sql)


@fixture('query_advisee_sis_demographics.csv')
def get_advisee_sis_demographics():
    sql = f"""SELECT d.sid, d.gender_of_record, d.gender_identity, d.visas, d.ethnicities, d.countries
              FROM {intermediate_schema()}.advisee_sis_demographics d
              ORDER BY d.sid
    """
    return redshift.fetch(sql)


@fixture('query_advisee_submissions_comparisons_{term_id}.csv')
def get_advisee_submissions_sorted(term_id):
    columns = ['reference_user_id', 'canvas_course_id', 'canvas_user_id', 'submissions_turned_in']
    key = f"{app.config['LOCH_S3_BOAC_ANALYTICS_DATA_PATH']}/assignment_submissions_relative/{term_id}/sub_000.gz"
    return s3.get_retriable_csv_stream(columns, key, retries=3)


@fixture('query_enrollments_in_advisee_canvas_sites.csv')
def get_all_enrollments_in_advisee_canvas_sites():
    sql = f"""SELECT
                mem.course_id as canvas_course_id,
                mem.course_term as canvas_course_term,
                mem.uid,
                mem.canvas_user_id,
                mem.current_score,
                EXTRACT(EPOCH FROM mem.last_activity_at) AS last_activity_at,
                mem.sis_enrollment_status
              FROM {boac_schema()}.course_enrollments mem
              WHERE EXISTS (
                SELECT 1 FROM {boac_schema()}.course_enrollments memsub
                  JOIN {calnet_schema()}.persons ldap
                    ON memsub.uid = ldap.ldap_uid
                  WHERE memsub.course_id = mem.course_id
              )
              AND EXISTS (
                SELECT 1 FROM {intermediate_schema()}.course_sections cs
                WHERE cs.canvas_course_id = mem.course_id
                  AND cs.canvas_course_term=ANY('{{{','.join(canvas_terms())}}}')
              )
              ORDER BY mem.course_id, mem.canvas_user_id
        """
    return redshift.fetch(sql)


@fixture('query_advisee_sis_enrollments.csv')
def get_all_advisee_sis_enrollments():
    sql = f"""SELECT
                  enr.grade, enr.grade_midterm, enr.units, enr.grading_basis, enr.sis_enrollment_status, enr.sis_term_id,
                  enr.ldap_uid, ldap.sid,
                  enr.sis_course_title, enr.sis_course_name,
                  enr.sis_section_id, enr.sis_primary, enr.sis_instruction_format, enr.sis_section_num
              FROM {intermediate_schema()}.sis_enrollments enr
              JOIN {calnet_schema()}.persons ldap
                ON enr.ldap_uid = ldap.ldap_uid
              WHERE enr.sis_term_id=ANY('{{{','.join(reverse_term_ids(include_future_terms=True, include_legacy_terms=True))}}}')
              ORDER BY enr.sis_term_id DESC, ldap.sid, enr.sis_course_name, enr.sis_primary DESC, enr.sis_instruction_format, enr.sis_section_num
        """
    return redshift.fetch(sql)


@fixture('query_advisee_enrollment_drops.csv')
def get_all_advisee_enrollment_drops():
    sql = f"""SELECT dr.*
              FROM {intermediate_schema()}.sis_dropped_classes AS dr
              JOIN {calnet_schema()}.persons ldap
                ON dr.sid = ldap.sid
              WHERE dr.sis_term_id=ANY('{{{','.join(reverse_term_ids(include_legacy_terms=True))}}}')
              ORDER BY dr.sis_term_id DESC, dr.sid, dr.sis_course_name
            """
    return redshift.fetch(sql)


def get_all_advisee_term_gpas():
    sql = f"""SELECT gp.sid, gp.term_id, gp.gpa, gp.units_taken_for_gpa
              FROM {student_schema()}.student_term_gpas gp
              JOIN {calnet_schema()}.persons ldap
                ON gp.sid = ldap.sid
              WHERE gp.term_id=ANY('{{{','.join(reverse_term_ids(include_legacy_terms=True))}}}')
              ORDER BY gp.term_id, gp.sid DESC
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


def get_enrolled_primary_sections(term_id=None):
    term_clause = f'AND sec.sis_term_id = {term_id}' if term_id else ''
    sql = f"""SELECT
                sec.sis_term_id,
                sec.sis_section_id,
                sec.sis_course_name,
                TRANSLATE(sec.sis_course_name, '&-, ', '') AS sis_course_name_compressed,
                sec.sis_course_title,
                sec.sis_instruction_format,
                sec.sis_section_num,
                LISTAGG(DISTINCT sec.instructor_name, ', ') WITHIN GROUP (ORDER BY sec.instructor_name) AS instructors
              FROM {intermediate_schema()}.sis_enrollments enr
              JOIN {intermediate_schema()}.sis_sections sec
                ON enr.sis_term_id = sec.sis_term_id
                {term_clause}
                AND sec.is_primary = TRUE
                AND enr.sis_section_id = sec.sis_section_id
                AND enr.ldap_uid IN (SELECT uid FROM {student_schema()}.student_academic_status)
              GROUP BY
                sec.sis_term_id, sec.sis_section_id, sec.sis_course_name,
                sec.sis_course_title, sec.sis_instruction_format, sec.sis_section_num
              ORDER BY sec.sis_section_id
        """
    return redshift.fetch(sql)


def get_successfully_backfilled_students():
    sql = f"""SELECT sid
        FROM {metadata_schema()}.merged_feed_status
        WHERE status = 'success'"""
    return rds.fetch(sql)
