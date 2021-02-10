"""
Copyright ©2021. The Regents of the University of California (Regents). All Rights Reserved.

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


def advisee_schema():
    return app.config['REDSHIFT_SCHEMA_ADVISEE']


def advisor_schema():
    return app.config['REDSHIFT_SCHEMA_ADVISOR']


def advisor_schema_internal():
    return app.config['REDSHIFT_SCHEMA_ADVISOR_INTERNAL']


def asc_schema():
    return app.config['REDSHIFT_SCHEMA_ASC']


def boac_schema():
    return app.config['REDSHIFT_SCHEMA_BOAC']


def calnet_schema():
    return app.config['REDSHIFT_SCHEMA_CALNET']


def coe_schema():
    return app.config['REDSHIFT_SCHEMA_COE']


def edl_external_schema():
    return app.config['REDSHIFT_SCHEMA_EDL_EXTERNAL']


def edl_feature_flag():
    return app.config['FEATURE_FLAG_ENTERPRISE_DATA_LAKE']


def edl_schema():
    return app.config['REDSHIFT_SCHEMA_EDL']


def intermediate_schema():
    return app.config['REDSHIFT_SCHEMA_INTERMEDIATE']


def metadata_schema():
    return app.config['RDS_SCHEMA_METADATA']


def sis_schema():
    return app.config['REDSHIFT_SCHEMA_SIS']


def student_schema():
    return app.config['REDSHIFT_SCHEMA_STUDENT']


def undergrads_schema():
    return app.config['REDSHIFT_SCHEMA_UNDERGRADS']


def get_all_student_ids():
    sql = f"""SELECT sid FROM {asc_schema()}.students
        UNION SELECT sid FROM {coe_schema()}.students
        UNION SELECT sid FROM {undergrads_schema()}.students
        UNION SELECT sid FROM {advisee_schema()}.non_current_students"""
    return redshift.fetch(sql)


def get_advisee_ids(csids=None):
    csid_filter = 'WHERE sid = ANY(%s)' if csids is not None else ''
    sql = f"""SELECT ldap_uid, sid
              FROM {calnet_schema()}.advisees
              {csid_filter}
              ORDER BY sid"""
    return redshift.fetch(sql, params=(csids,))


def get_advisee_advisor_mappings():
    sql = f"""SELECT DISTINCT
            advs.student_sid AS student_sid,
            advs.advisor_type AS advisor_role,
            advs.academic_program AS program,
            advs.academic_plan AS plan,
            advs.advisor_sid,
            aa.ldap_uid AS advisor_uid,
            aa.first_name AS advisor_first_name,
            aa.last_name AS advisor_last_name,
            aa.title AS advisor_title,
            aa.campus_email AS advisor_campus_email,
            aa.email AS advisor_email
        FROM {calnet_schema()}.advisees ldap
        JOIN {advisor_schema_internal()}.advisor_students advs
          ON ldap.sid = advs.student_sid
        LEFT JOIN {advisor_schema_internal()}.advisor_attributes aa
          ON advs.advisor_sid = aa.csid
        ORDER BY advs.student_sid, advs.advisor_type, advs.academic_plan, aa.first_name, aa.last_name
        """
    return redshift.fetch(sql)


@fixture('query_advisee_student_profile_feeds.csv')
def get_advisee_student_profile_elements():
    sis_api_profile_table = 'sis_api_profiles_v1' if app.config['STUDENT_V1_API_PREFERRED'] else 'sis_api_profiles'
    degree_progress_table = (f'{edl_schema()}.student_degree_progress' if edl_feature_flag() else f'{student_schema()}.sis_api_degree_progress')
    sql = f"""SELECT DISTINCT ldap.ldap_uid, ldap.sid, ldap.first_name, ldap.last_name,
                us.canvas_id AS canvas_user_id, us.name AS canvas_user_name,
                sis.feed AS sis_profile_feed,
                deg.feed AS degree_progress_feed,
                demog.feed AS demographics_feed,
                reg.feed AS last_registration_feed,
                (
                  SELECT LISTAGG(im.plan_code || ' :: ' || coalesce(apo.acadplan_descr, ''), ' || ')
                  FROM {sis_schema()}.intended_majors im
                  LEFT JOIN {advisor_schema()}.academic_plan_owners apo ON im.plan_code = apo.acadplan_code
                  WHERE im.sid = ldap.sid
                ) AS intended_majors
              FROM {calnet_schema()}.advisees ldap
              LEFT JOIN {intermediate_schema()}.users us
                ON us.uid = ldap.ldap_uid
              LEFT JOIN {student_schema()}.{sis_api_profile_table} sis
                ON sis.sid = ldap.sid
              LEFT JOIN {degree_progress_table} deg
                ON deg.sid = ldap.sid
              LEFT JOIN {student_schema()}.student_api_demographics demog
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
        JOIN {calnet_schema()}.advisees ldap
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
                  JOIN {calnet_schema()}.advisees ldap
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
    # The calnet advisees table is used as a convenient union of all BOA advisees,
    sql = f"""SELECT
                  enr.grade, enr.grade_midterm, enr.units, enr.grading_basis, enr.sis_enrollment_status, enr.sis_term_id,
                  enr.ldap_uid, enr.sid,
                  enr.sis_course_title, enr.sis_course_name, enr.sis_section_id,
                  enr.sis_primary, enr.sis_instruction_mode, enr.sis_instruction_format, enr.sis_section_num
              FROM {intermediate_schema()}.sis_enrollments enr
              JOIN {calnet_schema()}.advisees ldap
                ON enr.ldap_uid = ldap.ldap_uid
              WHERE enr.sis_term_id=ANY('{{{','.join(reverse_term_ids(include_future_terms=True, include_legacy_terms=True))}}}')
              ORDER BY enr.sis_term_id DESC, ldap.sid, enr.sis_course_name, enr.sis_primary DESC, enr.sis_instruction_format, enr.sis_section_num
        """
    return redshift.fetch(sql)


@fixture('query_advisee_enrollment_drops.csv')
def get_all_advisee_enrollment_drops():
    sql = f"""SELECT dr.*
              FROM {intermediate_schema()}.sis_dropped_classes AS dr
              JOIN {calnet_schema()}.advisees ldap
                ON dr.sid = ldap.sid
              WHERE dr.sis_term_id=ANY('{{{','.join(reverse_term_ids(include_legacy_terms=True))}}}')
              ORDER BY dr.sis_term_id DESC, dr.sid, dr.sis_course_name
            """
    return redshift.fetch(sql)


def get_all_advisee_term_gpas():
    sql = f"""SELECT gp.sid, gp.term_id, gp.gpa, gp.units_taken_for_gpa
              FROM {student_schema()}.student_term_gpas gp
              JOIN {calnet_schema()}.advisees ldap
                ON gp.sid = ldap.sid
              WHERE gp.term_id=ANY('{{{','.join(reverse_term_ids(include_legacy_terms=True))}}}')
              ORDER BY gp.term_id, gp.sid DESC
        """
    return redshift.fetch(sql)


def get_all_instructor_uids():
    sql = f"""SELECT DISTINCT instructor_uid
              FROM {sis_schema()}.courses
              WHERE instructor_uid IS NOT NULL AND instructor_uid != ''
        """
    return redshift.fetch(sql)


def get_edl_student_registrations(sids, order_by='student_id'):
    sql = f"""SELECT
                academic_career_cd,
                academic_level_beginning_of_term_cd,
                academic_level_beginning_of_term_desc,
                academic_level_end_of_term_cd,
                academic_level_end_of_term_desc,
                current_term_gpa_nbr,
                eligibility_status_desc,
                eligible_to_enroll_flag,
                expected_graduation_term,
                intends_to_graduate_flag,
                load_dt,
                maximum_term_enrollment_units_limit,
                minimum_term_enrollment_units_limit,
                registered_flag,
                registrn_eligibility_status_cd,
                semester_year_term_cd AS term_id,
                student_id,
                term_enrolled_units,
                terms_in_attendance,
                total_cumulative_gpa_nbr,
                total_units_completed_qty
              FROM {edl_external_schema()}.student_registration_term_data
              WHERE student_id=ANY('{{{','.join(sids)}}}')
              ORDER BY {order_by}
        """
    return redshift.fetch(sql)


def get_enrolled_canvas_sites_for_term(term_id):
    sql = f"""SELECT DISTINCT enr.canvas_course_id
              FROM {intermediate_schema()}.active_student_enrollments enr
              JOIN {intermediate_schema()}.course_sections cs
                ON cs.canvas_course_id = enr.canvas_course_id
                AND cs.canvas_course_term = '{term_name_for_sis_id(term_id)}'
                AND enr.uid IN (SELECT uid FROM {student_schema()}.student_profile_index)
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
                AND enr.ldap_uid IN (SELECT uid FROM {student_schema()}.student_profile_index)
              GROUP BY
                sec.sis_term_id, sec.sis_section_id, sec.sis_course_name,
                sec.sis_course_title, sec.sis_instruction_format, sec.sis_section_num
              ORDER BY sec.sis_section_id
        """
    return redshift.fetch(sql)


def get_fetched_non_advisees():
    sql = f"""SELECT DISTINCT(hist.sid) AS sid
              FROM {student_schema()}.sis_api_profiles_hist_enr hist
              LEFT JOIN {asc_schema()}.students ascs ON ascs.sid = hist.sid
              LEFT JOIN {coe_schema()}.students coe ON coe.sid = hist.sid
              LEFT JOIN {undergrads_schema()}.students ug ON ug.sid = hist.sid
              WHERE ascs.sid IS NULL AND coe.sid IS NULL AND ug.sid IS NULL
        """
    return redshift.fetch(sql)


def get_unfetched_non_advisees():
    sql = f"""SELECT DISTINCT(attrs.sid)
              FROM (
                SELECT sis_id AS sid FROM {sis_schema()}.enrollments WHERE sis_id IS NOT NULL
                UNION SELECT sid FROM {sis_schema()}.basic_attributes WHERE sid IS NOT NULL
              ) attrs
              LEFT JOIN {asc_schema()}.students ascs ON ascs.sid = attrs.sid
              LEFT JOIN {coe_schema()}.students coe ON coe.sid = attrs.sid
              LEFT JOIN {undergrads_schema()}.students ug ON ug.sid = attrs.sid
              LEFT JOIN {student_schema()}.sis_api_profiles_hist_enr hist ON hist.sid = attrs.sid
              WHERE ascs.sid IS NULL AND coe.sid IS NULL AND ug.sid IS NULL AND hist.sid IS NULL
                AND char_length(attrs.sid) < 12
        """
    return redshift.fetch(sql)


def get_non_advisees_without_registration_imports():
    sql = f"""SELECT DISTINCT(attrs.sid)
              FROM (
                SELECT sis_id AS sid FROM {sis_schema()}.enrollments WHERE sis_id IS NOT NULL
                UNION SELECT sid FROM {sis_schema()}.basic_attributes WHERE sid IS NOT NULL
              ) attrs
              LEFT JOIN {asc_schema()}.students ascs ON ascs.sid = attrs.sid
              LEFT JOIN {coe_schema()}.students coe ON coe.sid = attrs.sid
              LEFT JOIN {undergrads_schema()}.students ug ON ug.sid = attrs.sid
              LEFT JOIN {edl_schema() if edl_feature_flag() else student_schema()}.hist_enr_last_registrations hist ON hist.sid = attrs.sid
              WHERE ascs.sid IS NULL AND coe.sid IS NULL AND ug.sid IS NULL AND hist.sid IS NULL
                AND char_length(attrs.sid) < 12
        """
    return redshift.fetch(sql)


def get_non_advisee_api_feeds(sids):
    sql = f"""SELECT DISTINCT sis.sid, sis.uid,
                sis.feed AS sis_feed,
                reg.feed AS last_registration_feed
              FROM {student_schema()}.sis_api_profiles_hist_enr sis
              LEFT JOIN {student_schema()}.hist_enr_last_registrations reg
                ON reg.sid = sis.sid
              WHERE sis.sid=ANY(%s)
              ORDER BY sis.sid
        """
    return redshift.fetch(sql, params=(sids,))


def get_non_advisee_sis_enrollments(sids, term_id):
    sql = f"""SELECT
                  enr.grade, enr.grade_midterm, enr.units, enr.grading_basis, enr.sis_enrollment_status, enr.sis_term_id,
                  enr.ldap_uid, enr.sid,
                  enr.sis_course_title, enr.sis_course_name, enr.sis_section_id,
                  enr.sis_primary, enr.sis_instruction_mode, enr.sis_instruction_format, enr.sis_section_num
              FROM {intermediate_schema()}.sis_enrollments enr
              WHERE enr.sid=ANY(%s)
                AND enr.sis_term_id='{term_id}'
              ORDER BY enr.sis_term_id DESC, enr.sid, enr.sis_course_name, enr.sis_primary DESC, enr.sis_instruction_format, enr.sis_section_num
        """
    return redshift.fetch(sql, params=(sids,))


def get_non_advisee_enrollment_drops(sids, term_id):
    sql = f"""SELECT dr.*
              FROM {intermediate_schema()}.sis_dropped_classes AS dr
              WHERE dr.sid = ANY(%s)
                AND dr.sis_term_id = '{term_id}'
              ORDER BY dr.sid, dr.sis_course_name
        """
    return redshift.fetch(sql, params=(sids,))


def get_non_advisee_term_gpas(sids, term_id):
    sql = f"""SELECT gp.sid, gp.term_id, gp.gpa, gp.units_taken_for_gpa
              FROM {student_schema()}.hist_enr_term_gpas gp
              WHERE gp.sid = ANY(%s)
                AND gp.term_id = '{term_id}'
              ORDER BY gp.sid
        """
    return redshift.fetch(sql, params=(sids,))


def get_sids_with_registration_imports():
    sql = f"""SELECT sid
        FROM {metadata_schema()}.registration_import_status
        WHERE status = 'success'"""
    return rds.fetch(sql)


def get_active_sids_with_oldest_registration_imports(limit):
    active_sids = [r['sid'] for r in get_all_student_ids()]
    sql = f"""SELECT sid FROM {metadata_schema()}.registration_import_status
        WHERE sid = ANY(%s)
        AND status = 'success'
        ORDER BY updated_at LIMIT %s"""
    return rds.fetch(sql, params=(active_sids, limit))
