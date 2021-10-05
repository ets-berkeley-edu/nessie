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


def edl_external_schema_staging():
    return app.config['REDSHIFT_SCHEMA_EDL_EXTERNAL_STAGING']


def edl_schema():
    return app.config['REDSHIFT_SCHEMA_EDL']


def intermediate_schema():
    return app.config['REDSHIFT_SCHEMA_INTERMEDIATE']


def metadata_schema():
    return app.config['RDS_SCHEMA_METADATA']


def sis_schema():
    return app.config['REDSHIFT_SCHEMA_SIS']


def sis_schema_internal():
    return app.config['REDSHIFT_SCHEMA_SIS_INTERNAL']


def student_schema():
    return app.config['REDSHIFT_SCHEMA_STUDENT']


# TODO: Remove this method when the EDL cutover is complete.
def student_schema_table(key):
    return {
        'degree_progress': 'student_degree_progress' if app.config['FEATURE_FLAG_EDL_DEGREE_PROGRESS'] else 'sis_api_degree_progress',
        'student_demographics': 'student_demographics' if app.config['FEATURE_FLAG_EDL_DEMOGRAPHICS'] else 'student_api_demographics',
    }.get(key, key)


def undergrads_schema():
    return app.config['REDSHIFT_SCHEMA_UNDERGRADS']


def get_all_student_ids():
    if app.config['FEATURE_FLAG_EDL_SIS_VIEWS']:
        sql = f"""SELECT DISTINCT student_id AS sid
            FROM {edl_external_schema()}.student_academic_plan_data
            WHERE
                academic_career_cd='UGRD'
                AND academic_program_status_cd='AC'
                AND academic_plan_type_cd != 'MIN'
            UNION SELECT sid FROM {asc_schema()}.students
            UNION SELECT sid FROM {coe_schema()}.students
            UNION SELECT sid FROM {advisee_schema()}.non_current_students
        """
    else:
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
    degree_progress_schema = edl_schema() if app.config['FEATURE_FLAG_EDL_DEGREE_PROGRESS'] else app.config['REDSHIFT_SCHEMA_STUDENT']
    demographics_schema = edl_schema() if app.config['FEATURE_FLAG_EDL_DEMOGRAPHICS'] else app.config['REDSHIFT_SCHEMA_STUDENT']
    registration_schema = edl_schema() if app.config['FEATURE_FLAG_EDL_REGISTRATIONS'] else app.config['REDSHIFT_SCHEMA_STUDENT']

    use_edl_sis = app.config['FEATURE_FLAG_EDL_SIS_VIEWS']
    if use_edl_sis:
        sql_intended_majors = f"""
            SELECT LISTAGG(im.plan_code || ' :: ' || coalesce(saphd.academic_plan_nm, ''), ' || ')
            FROM {edl_schema()}.intended_majors im
            LEFT JOIN {edl_external_schema()}.student_academic_plan_hierarchy_data saphd ON im.plan_code = saphd.academic_plan_cd
            WHERE im.sid = ldap.sid
        """
    else:
        sql_intended_majors = f"""
            SELECT LISTAGG(im.plan_code || ' :: ' || coalesce (apo.acadplan_descr, ''), ' || ')
            FROM {sis_schema()}.intended_majors im
            LEFT JOIN {advisor_schema()}.academic_plan_owners apo ON im.plan_code = apo.acadplan_code
            WHERE im.sid = ldap.sid
        """

    if app.config['FEATURE_FLAG_EDL_STUDENT_PROFILES']:
        profile_table = f'{edl_schema()}.student_profiles'
    else:
        profile_table = f'{student_schema()}.sis_api_profiles'

    sql = f"""SELECT DISTINCT ldap.ldap_uid, ldap.sid, ldap.first_name, ldap.last_name,
                us.canvas_id AS canvas_user_id, us.name AS canvas_user_name,
                sis.feed AS sis_profile_feed,
                deg.feed AS degree_progress_feed,
                demog.feed AS demographics_feed,
                reg.feed AS last_registration_feed,
                ({sql_intended_majors}) AS intended_majors
              FROM {calnet_schema()}.advisees ldap
              LEFT JOIN {intermediate_schema()}.users us
                ON us.uid = ldap.ldap_uid
              LEFT JOIN {profile_table} sis
                ON sis.sid = ldap.sid
              LEFT JOIN {degree_progress_schema}.{student_schema_table('degree_progress')} deg
                ON deg.sid = ldap.sid
              LEFT JOIN {demographics_schema}.{student_schema_table('student_demographics')} demog
                ON demog.sid = ldap.sid
              LEFT JOIN {registration_schema}.student_last_registrations reg
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
    if app.config['FEATURE_FLAG_EDL_SIS_VIEWS']:
        select = 'SELECT dr.*, LEFT(e.drop_date, 10) AS drop_date'
        drop_date_join = f"""LEFT JOIN {edl_schema()}.enrollments e
            ON ldap.ldap_uid = e.ldap_uid
            AND dr.sis_term_id = e.term_id
            AND dr.sis_section_id = e.section_id"""
    else:
        select = 'SELECT dr.*, drp.date AS drop_date'
        drop_date_join = f"""LEFT JOIN {sis_schema_internal()}.drop_dates drp
            ON ldap.ldap_uid = drp.ldap_uid
            AND dr.sis_term_id = drp.sis_term_id
            AND dr.sis_section_id = drp.sis_section_id"""
    sql = f"""{select}
              FROM {intermediate_schema()}.sis_dropped_classes AS dr
              JOIN {calnet_schema()}.advisees ldap
                ON dr.sid = ldap.sid
              {drop_date_join}
              WHERE dr.sis_term_id=ANY('{{{','.join(reverse_term_ids(include_legacy_terms=True))}}}')
              ORDER BY dr.sis_term_id DESC, dr.sid, dr.sis_course_name
            """
    return redshift.fetch(sql)


def get_all_advisee_term_gpas():
    if app.config['FEATURE_FLAG_EDL_REGISTRATIONS']:
        term_gpa_table = f'{edl_schema()}.term_gpa'
    else:
        term_gpa_table = f'{student_schema()}.student_term_gpas'
    sql = f"""SELECT gp.sid, gp.term_id, gp.gpa, gp.units_taken_for_gpa
              FROM {term_gpa_table} gp
              JOIN {calnet_schema()}.advisees ldap
                ON gp.sid = ldap.sid
              WHERE gp.term_id=ANY('{{{','.join(reverse_term_ids(include_legacy_terms=True))}}}')
              ORDER BY gp.term_id, gp.sid DESC
        """
    return redshift.fetch(sql)


def get_all_instructor_uids():
    course_schema = edl_schema() if app.config['FEATURE_FLAG_EDL_SIS_VIEWS'] else sis_schema()
    sql = f"""SELECT DISTINCT instructor_uid
              FROM {course_schema}.courses
              WHERE instructor_uid IS NOT NULL AND instructor_uid != ''
        """
    return redshift.fetch(sql)


def stream_edl_demographics():
    sql = f"""SELECT
                i.sid, i.gender, e.ethnicity, e.ethnic_group, c.citizenship_country, v.visa_status, v.visa_type
              FROM {edl_schema()}.student_profile_index i
              LEFT JOIN {edl_schema()}.student_ethnicities e ON i.sid = e.sid
              LEFT JOIN {edl_schema()}.student_citizenships c ON i.sid = c.sid
              LEFT JOIN {edl_schema()}.student_visas v ON i.sid = v.sid
              ORDER by i.sid, i.gender, e.ethnicity, e.ethnic_group, c.citizenship_country, v.visa_status, v.visa_type"""
    return redshift.fetch(sql, stream_results=True)


def stream_edl_degrees():
    sql = f"""SELECT sadd.student_id AS sid,
        sadd.academic_career_cd,
        sadd.academic_degree_status_desc,
        sadd.academic_group_desc,
        sadd.academic_plan_nm,
        sadd.academic_plan_transcr_desc,
        sadd.academic_plan_type_cd,
        sadd.degree_conferred_dt,
        sadd.degree_desc
        FROM {edl_external_schema()}.student_awarded_degree_data sadd
        ORDER BY sadd.student_id, sadd.degree_conferred_dt, sadd.degree_desc"""
    return redshift.fetch(sql, stream_results=True)


def stream_edl_holds():
    sql = f"""SELECT ssid.student_id AS sid,
        ssid.service_indicator_start_dt,
        ssid.service_indicator_reason_desc,
        ssid.service_indicator_long_desc
        FROM {edl_external_schema()}.student_service_indicator_data ssid
        WHERE ssid.positive_service_indicator_flag = 'N' and ssid.deleted_flag = 'N'
        ORDER BY ssid.student_id, ssid.service_indicator_start_dt"""
    return redshift.fetch(sql, stream_results=True)


def stream_edl_plans():
    sql = f"""SELECT
        DISTINCT sapd.student_id AS sid,
        sapd.academic_career_cd,
        sapd.academic_plan_type_cd,
        sapd.academic_plan_nm,
        sapd.academic_program_cd,
        sapd.academic_program_effective_dt,
        sapd.academic_program_nm,
        sapd.academic_program_shrt_nm,
        sapd.academic_program_status_desc,
        sapd.academic_subplan_nm,
        sapd.current_admit_term,
        sapd.degree_expected_year_term_cd,
        sapd.transfer_student,
        cpap.admit_term AS matriculation_term_cd
        FROM {edl_external_schema()}.student_academic_plan_data sapd
        JOIN {edl_external_schema_staging()}.cs_ps_acad_prog cpap
          ON sapd.student_id = cpap.emplid
          AND sapd.academic_program_cd = cpap.acad_prog
          AND cpap.prog_action IN ('DATA', 'MATR')
        ORDER BY sapd.student_id, sapd.academic_career_cd, sapd.academic_program_cd"""
    return redshift.fetch(sql, stream_results=True)


def stream_edl_profiles():
    sql = f"""SELECT spd.student_id AS sid,
          spd.campus_email_address_nm,
          spd.preferred_email_address_nm,
          spd.person_display_nm,
          spd.person_preferred_display_nm,
          cppp.phone_type,
          cppp.phone
        FROM {edl_external_schema()}.student_personal_data spd
        -- One phone number per student; prefer CELL, then LOCL.
        LEFT JOIN (
          SELECT emplid, phone, phone_type,
          row_number() OVER (
            PARTITION BY emplid
            ORDER BY CASE phone_type WHEN 'CELL' THEN 0 WHEN 'LOCL' THEN 1 ELSE 2 END
          ) AS seqnum
          FROM cs_staging_ext_dev.cs_ps_personal_phone
        ) cppp
        ON spd.student_id = cppp.emplid and seqnum = 1
        ORDER BY spd.student_id"""
    return redshift.fetch(sql, stream_results=True)


def stream_edl_profile_terms():
    sql = f"""SELECT
              r.student_id AS sid,
              r.semester_year_term_cd AS term_id,
              r.academic_career_cd,
              r.expected_graduation_term,
              r.term_berkeley_completed_gpa_units,
              r.terms_in_attendance,
              r.total_cumulative_gpa_nbr,
              r.total_units_completed_qty
            FROM {edl_external_schema()}.student_registration_term_data r
            ORDER BY r.student_id, r.semester_year_term_cd"""
    return redshift.fetch(sql, stream_results=True)


def stream_edl_registrations():
    sql = f"""SELECT
                r.student_id AS sid,
                r.semester_year_term_cd AS term_id,
                r.academic_career_cd,
                r.academic_level_beginning_of_term_cd,
                r.academic_level_beginning_of_term_desc,
                r.academic_level_end_of_term_cd,
                r.academic_level_end_of_term_desc,
                r.maximum_term_enrollment_units_limit,
                r.minimum_term_enrollment_units_limit,
                r.term_enrolled_units,
                r.term_berkeley_completed_total_units,
                s.withdraw_code,
                s.withdraw_date,
                s.withdraw_reason
              FROM {edl_external_schema()}.student_registration_term_data r
              JOIN {edl_external_schema_staging()}.cs_ps_stdnt_car_term s
                ON r.student_id = s.emplid AND r.semester_year_term_cd = s.strm
              WHERE (r.term_enrolled_units IS NOT NULL AND r.term_enrolled_units > 0)
                OR (r.term_berkeley_completed_total_units IS NOT NULL AND r.term_berkeley_completed_total_units > 0)
              ORDER BY r.student_id, r.semester_year_term_cd
        """
    return redshift.fetch(sql, stream_results=True)


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
    if app.config['FEATURE_FLAG_EDL_SIS_VIEWS']:
        where_clause = f"""
            LEFT JOIN {edl_external_schema()}.student_academic_plan_data ug ON ug.student_id = hist.sid
              AND ug.academic_career_cd = 'UGRD'
              AND ug.academic_program_status_cd = 'AC'
              AND ug.academic_plan_type_cd != 'MIN'
            WHERE ascs.sid IS NULL AND coe.sid IS NULL AND ug.student_id IS NULL
        """
    else:
        where_clause = f"""
            LEFT JOIN {undergrads_schema()}.students ug ON ug.sid = hist.sid
            WHERE ascs.sid IS NULL AND coe.sid IS NULL AND ug.sid IS NULL
        """

    if app.config['FEATURE_FLAG_EDL_STUDENT_PROFILES']:
        profile_table = f'{edl_schema()}.student_profiles'
        where_clause = f"""
            LEFT JOIN (
                 SELECT sid, ldap_uid FROM edl_sis_data.basic_attributes attrs
                 UNION
                 SELECT sis_id AS sid, ldap_uid FROM edl_sis_data.enrollments
                 GROUP BY sid, ldap_uid
            ) attrs
            ON attrs.sid = hist.sid
            {where_clause}
            AND attrs.ldap_uid IS NOT NULL and attrs.ldap_uid != ''
        """
    else:
        profile_table = f'{student_schema()}.sis_api_profiles_hist_enr'

    sql = f"""SELECT DISTINCT(hist.sid) AS sid
              FROM {profile_table} hist
              LEFT JOIN {asc_schema()}.students ascs ON ascs.sid = hist.sid
              LEFT JOIN {coe_schema()}.students coe ON coe.sid = hist.sid
              {where_clause}
        """
    return redshift.fetch(sql)


def get_unfetched_non_advisees():
    if app.config['FEATURE_FLAG_EDL_SIS_VIEWS']:
        attrs_schema = edl_schema()
        ug_join = f"""LEFT JOIN {edl_external_schema()}.student_academic_plan_data ug ON ug.student_id = hist.sid
              AND ug.academic_career_cd = 'UGRD'
              AND ug.academic_program_status_cd = 'AC'
              AND ug.academic_plan_type_cd != 'MIN'"""
        ug_null = 'AND ug.student_id IS NULL'
    else:
        attrs_schema = sis_schema()
        ug_join = f'LEFT JOIN {undergrads_schema()}.students ug ON ug.sid = attrs.sid'
        ug_null = 'AND ug.sid IS NULL'
    sql = f"""SELECT DISTINCT attrs.sid
              FROM {attrs_schema}.basic_attributes attrs
              LEFT JOIN {asc_schema()}.students ascs ON ascs.sid = attrs.sid
              LEFT JOIN {coe_schema()}.students coe ON coe.sid = attrs.sid
              LEFT JOIN {student_schema()}.sis_api_profiles_hist_enr hist ON hist.sid = attrs.sid
              {ug_join}
              WHERE ascs.sid IS NULL AND coe.sid IS NULL AND hist.sid IS NULL {ug_null}
                AND (
                  attrs.affiliations LIKE '%STUDENT-TYPE%'
                  OR attrs.affiliations LIKE '%SIS-EXTENDED%'
                  OR attrs.affiliations LIKE '%FORMER-STUDENT%'
                )
                AND attrs.person_type = 'S' AND char_length(attrs.sid) < 12
        """
    return redshift.fetch(sql)


def get_non_advisees_without_registration_imports():
    if app.config['FEATURE_FLAG_EDL_SIS_VIEWS']:
        attrs_schema = edl_schema()
        ug_join = f"""LEFT JOIN {edl_external_schema()}.student_academic_plan_data ug ON ug.student_id = hist.sid
              AND ug.academic_career_cd = 'UGRD'
              AND ug.academic_program_status_cd = 'AC'
              AND ug.academic_plan_type_cd != 'MIN'"""
        ug_null = 'AND ug.student_id IS NULL'
    else:
        attrs_schema = sis_schema()
        ug_join = f'LEFT JOIN {undergrads_schema()}.students ug ON ug.sid = attrs.sid'
        ug_null = 'AND ug.sid IS NULL'
    attrs_schema = edl_schema() if app.config['FEATURE_FLAG_EDL_SIS_VIEWS'] else sis_schema()
    sql = f"""SELECT DISTINCT attrs.sid
              FROM {attrs_schema}.basic_attributes attrs
              LEFT JOIN {asc_schema()}.students ascs ON ascs.sid = attrs.sid
              LEFT JOIN {coe_schema()}.students coe ON coe.sid = attrs.sid
              LEFT JOIN {student_schema()}.hist_enr_last_registrations hist ON hist.sid = attrs.sid
              {ug_join}
              WHERE ascs.sid IS NULL AND coe.sid IS NULL AND hist.sid IS NULL {ug_null}
                AND (
                  attrs.affiliations LIKE '%STUDENT-TYPE%'
                  OR attrs.affiliations LIKE '%SIS-EXTENDED%'
                  OR attrs.affiliations LIKE '%FORMER-STUDENT%'
                )
                AND attrs.person_type = 'S' AND char_length(attrs.sid) < 12
        """
    return redshift.fetch(sql)


def get_non_advisee_api_feeds(sids):
    if app.config['FEATURE_FLAG_EDL_REGISTRATIONS']:
        registration_table = f'{edl_schema()}.student_last_registrations'
    else:
        registration_table = f'{student_schema()}.hist_enr_last_registrations'
    if app.config['FEATURE_FLAG_EDL_STUDENT_PROFILES']:
        uid_select = 'attrs.ldap_uid AS uid'
        attrs_join = """
          LEFT JOIN (
            SELECT a.sid, MAX(a.ldap_uid) AS ldap_uid FROM (
              SELECT sid, ldap_uid FROM edl_sis_data.basic_attributes attrs
                WHERE (
                  attrs.affiliations LIKE '%%STUDENT-TYPE%%'
                  OR attrs.affiliations LIKE '%%SIS-EXTENDED%%'
                  OR attrs.affiliations LIKE '%%FORMER-STUDENT%%'
                )
                AND attrs.person_type = 'S' AND char_length(attrs.sid) < 12
              UNION
              SELECT sis_id AS sid, ldap_uid FROM edl_sis_data.enrollments
              GROUP BY sid, ldap_uid
            ) a
            GROUP BY sid
          ) attrs
          ON attrs.sid = sis.sid"""
        profile_table = f'{edl_schema()}.student_profiles'
    else:
        uid_select = 'sis.uid'
        attrs_join = ''
        profile_table = f'{student_schema()}.sis_api_profiles_hist_enr'
    sql = f"""SELECT DISTINCT sis.sid,
                {uid_select},
                sis.feed AS sis_feed,
                reg.feed AS last_registration_feed
              FROM {profile_table} sis
              {attrs_join}
              LEFT JOIN {registration_table} reg
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
    sql = f"""SELECT dr.*, dd.date AS drop_date
              FROM {intermediate_schema()}.sis_dropped_classes AS dr
              LEFT JOIN {sis_schema_internal()}.drop_dates dd
                ON dr.ldap_uid = dd.ldap_uid
                AND dr.sis_term_id = dd.sis_term_id
                AND dr.sis_section_id = dd.sis_section_id
              WHERE dr.sid = ANY(%s)
                AND dr.sis_term_id = '{term_id}'
              ORDER BY dr.sid, dr.sis_course_name
        """
    return redshift.fetch(sql, params=(sids,))


def get_non_advisee_term_gpas(sids, term_id):
    if app.config['FEATURE_FLAG_EDL_REGISTRATIONS']:
        term_gpa_table = f'{edl_schema()}.term_gpa'
    else:
        term_gpa_table = f'{student_schema()}.hist_enr_term_gpas'
    sql = f"""SELECT gp.sid, gp.term_id, gp.gpa, gp.units_taken_for_gpa
              FROM {term_gpa_table} gp
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
