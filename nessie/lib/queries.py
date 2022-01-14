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
from nessie.externals import rds, redshift
from nessie.lib.berkeley import term_name_for_sis_id
from nessie.lib.mockingdata import fixture


def advisor_schema():
    return app.config['REDSHIFT_SCHEMA_ADVISOR']


def advisor_schema_internal():
    return app.config['REDSHIFT_SCHEMA_ADVISOR_INTERNAL']


def asc_schema():
    return app.config['REDSHIFT_SCHEMA_ASC']


def boac_schema():
    return app.config['REDSHIFT_SCHEMA_BOAC']


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


def student_schema():
    return app.config['REDSHIFT_SCHEMA_STUDENT']


def get_active_student_ids():
    sql = f"""
      SELECT sid, MAX(ldap_uid) AS ldap_uid
      FROM {edl_schema()}.basic_attributes attrs
      WHERE ldap_uid IS NOT NULL
      AND sid IN (
          SELECT DISTINCT student_id AS sid
          FROM {edl_external_schema()}.student_academic_plan_data
          WHERE
            academic_career_cd IN ('UGRD', 'GRAD', 'UCBX')
            AND academic_program_status_cd='AC'
          UNION SELECT sid FROM {asc_schema()}.students
          UNION SELECT sid FROM {coe_schema()}.students
        )
      GROUP BY sid
    """
    return redshift.fetch(sql)


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
        FROM {advisor_schema_internal()}.advisor_students advs
        LEFT JOIN {advisor_schema_internal()}.advisor_attributes aa
          ON advs.advisor_sid = aa.csid
        ORDER BY advs.student_sid, advs.advisor_type, advs.academic_plan, aa.first_name, aa.last_name
        """
    return redshift.fetch(sql)


@fixture('query_all_student_profile_feeds.csv')
def get_all_student_profile_elements():
    sql = f"""SELECT DISTINCT attrs.sid, attrs.ldap_uid,
            us.canvas_id AS canvas_user_id, us.name AS canvas_user_name,
            sis.feed AS sis_profile_feed,
            deg.feed AS degree_progress_feed,
            demog.feed AS demographics_feed,
            attrs.first_name,
            attrs.last_name,
            reg.feed AS last_registration_feed,
            (
                SELECT LISTAGG(im.plan_code || ' :: ' || coalesce(saphd.academic_plan_nm, ''), ' || ')
                FROM {edl_schema()}.intended_majors im
                LEFT JOIN {edl_external_schema()}.student_academic_plan_hierarchy_data saphd
                    ON im.plan_code = saphd.academic_plan_cd
                WHERE im.sid = attrs.sid
            ) AS intended_majors
        FROM (
            SELECT a.sid, MAX(a.ldap_uid) AS ldap_uid, MAX(a.first_name) AS first_name, MAX(a.last_name) AS last_name FROM (
              SELECT sid, ldap_uid, first_name, last_name FROM {edl_schema()}.basic_attributes attrs
                WHERE (
                  attrs.affiliations LIKE '%%STUDENT-TYPE%%'
                  OR attrs.affiliations LIKE '%%SIS-EXTENDED%%'
                  OR attrs.affiliations LIKE '%%FORMER-STUDENT%%'
                  OR attrs.affiliations LIKE '%%ADVCON-STUDENT%%'
                )
                AND char_length(attrs.sid) < 12 AND attrs.ldap_uid IS NOT NULL
              UNION
              SELECT enr.sis_id AS sid, enr.ldap_uid, NULL AS first_name, NULL AS last_name
              FROM {edl_schema()}.enrollments enr WHERE enr.ldap_uid IS NOT NULL
              GROUP BY sid, ldap_uid
            ) a
            GROUP BY sid
        ) attrs
        LEFT JOIN {edl_schema()}.student_profiles sis
            ON sis.sid = attrs.sid
        LEFT JOIN {intermediate_schema()}.users us
            ON us.sis_user_id = attrs.sid
        LEFT JOIN {edl_schema()}.student_degree_progress deg
            ON deg.sid = attrs.sid
        LEFT JOIN {edl_schema()}.student_demographics demog
            ON demog.sid = attrs.sid
        LEFT JOIN {edl_schema()}.student_last_registrations reg
            ON reg.sid = attrs.sid
        ORDER BY attrs.sid
        """
    return redshift.fetch(sql)


def get_sids_with_photos():
    sql = f"""SELECT sid
        FROM {metadata_schema()}.photo_import_status
        WHERE status = 'success'"""
    return rds.fetch(sql)


def get_advisor_sids():
    sql = f"""SELECT advisor_sid AS sid
        FROM {advisor_schema_internal()}.advisor_students
        UNION SELECT advisor_id AS sid
        FROM {advisor_schema()}.instructor_advisor"""
    return redshift.fetch(sql)


@fixture('query_advisee_enrolled_canvas_sites_{term_id}.csv')
def stream_canvas_sites(term_id):
    sql = f"""SELECT enr.canvas_course_id, enr.canvas_course_name, enr.canvas_course_code, enr.canvas_course_term,
          LISTAGG(DISTINCT cs.sis_section_id, ',') AS sis_section_ids
        FROM {intermediate_schema()}.active_student_enrollments enr
        JOIN {intermediate_schema()}.course_sections cs
          ON cs.canvas_course_id = enr.canvas_course_id
          AND cs.sis_section_id IS NOT NULL
        WHERE enr.term_id='{term_id}'
        GROUP BY enr.canvas_course_id, enr.canvas_course_name, enr.canvas_course_code, enr.canvas_course_term
        ORDER BY enr.canvas_course_id
        """
    return redshift.fetch(sql, stream_results=True)


@fixture('query_enrollments_in_advisee_canvas_sites_{term_id}.csv')
def stream_canvas_enrollments(term_id):
    sql = f"""SELECT DISTINCT
                ce.course_id as canvas_course_id,
                ce.canvas_user_id,
                ce.uid,
                attrs.sid,
                ce.current_score,
                EXTRACT(EPOCH FROM ce.last_activity_at) AS last_activity_at,
                ce.sis_enrollment_status,
                ce.sis_section_ids
              FROM {boac_schema()}.course_enrollments ce
              JOIN {intermediate_schema()}.course_sections cs
                ON cs.canvas_course_id = ce.course_id
                AND cs.sis_section_id IS NOT NULL
              LEFT JOIN {edl_schema()}.basic_attributes attrs
                ON ce.uid = attrs.ldap_uid
              WHERE ce.term_id='{term_id}'
              ORDER BY ce.course_id, ce.canvas_user_id
        """
    return redshift.fetch(sql, stream_results=True)


@fixture('query_advisee_submissions_comparisons_{term_id}.csv')
def stream_canvas_assignment_submissions(term_id):
    sql = f"""SELECT
            ac1.course_id AS canvas_course_id,
            ac1.canvas_user_id AS reference_user_id,
            ac2.canvas_user_id AS canvas_user_id,
            COUNT(
                CASE WHEN ac2.assignment_status IN (\'graded\', \'late\', \'on_time\', \'submitted\')
                THEN 1 ELSE NULL END
            ) AS submissions_turned_in
        FROM {boac_schema()}.assignment_submissions_scores ac1
        JOIN {boac_schema()}.assignment_submissions_scores ac2
            ON ac1.term_id='{term_id}'
            AND ac1.assignment_id = ac2.assignment_id
            AND ac1.course_id = ac2.course_id
        GROUP BY canvas_course_id, reference_user_id, ac2.canvas_user_id
        HAVING count(*) = (
            SELECT count(*) FROM {boac_schema()}.assignment_submissions_scores
            WHERE canvas_user_id = reference_user_id AND course_id = ac1.course_id
        )
        ORDER BY canvas_course_id, reference_user_id, ac2.canvas_user_id"""
    return redshift.fetch(sql, stream_results=True)


def get_all_instructor_uids():
    sql = f"""SELECT DISTINCT instructor_uid
              FROM {edl_schema()}.courses
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
        ORDER BY sadd.student_id, sadd.degree_conferred_dt DESC, sadd.degree_desc"""
    return redshift.fetch(sql, stream_results=True)


def stream_edl_holds():
    sql = f"""SELECT ssid.student_id AS sid,
        ssid.service_indicator_start_dt,
        ssid.service_indicator_reason_desc,
        ssid.service_indicator_long_desc
        FROM {edl_external_schema()}.student_service_indicator_data ssid
        WHERE
          ssid.positive_service_indicator_flag = 'N'
          AND ssid.deleted_flag = 'N'
          AND NOT ssid.service_indicator_cd = ANY('{{A01, ARV, C01, CDP, CDR, R05, R14, RCL, S00, S03, S08, X00}}')
        ORDER BY ssid.student_id, ssid.service_indicator_start_dt"""
    return redshift.fetch(sql, stream_results=True)


def stream_edl_plans():
    sql = f"""SELECT
        DISTINCT sapd.student_id AS sid,
        attrs.affiliations AS ldap_affiliations,
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
        sapd.matriculation_term_cd
        FROM {edl_external_schema()}.student_academic_plan_data sapd
        LEFT OUTER JOIN {edl_schema()}.basic_attributes attrs
          ON sapd.student_id = attrs.sid
        ORDER BY
          sapd.student_id,
          CASE sapd.academic_career_cd
            WHEN 'UGRD' THEN 1
            WHEN 'GRAD' THEN 2
            WHEN 'UCBX' THEN 3
            ELSE 4
          END,
          sapd.career_program_sequence_nbr,
          sapd.academic_program_cd"""
    return redshift.fetch(sql, stream_results=True)


def stream_edl_profiles():
    sql = f"""SELECT spd.student_id AS sid,
          spd.campus_email_address_nm,
          spd.preferred_email_address_nm,
          spd.person_first_nm,
          spd.person_middle_nm,
          spd.person_last_nm,
          spd.person_preferred_first_nm,
          spd.person_preferred_middle_nm,
          spd.person_preferred_last_nm,
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
          FROM {edl_external_schema_staging()}.cs_ps_personal_phone
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
              r.total_units_completed_qty,
              a.acad_standing_status,
              a.action_date,
              t.gpa
            FROM {edl_external_schema()}.student_registration_term_data r
            LEFT JOIN {edl_schema()}.academic_standing a
              ON r.student_id = a.sid
             AND r.semester_year_term_cd = a.term_id
            LEFT JOIN {edl_schema()}.term_gpa t
              ON r.student_id = t.sid
             AND r.semester_year_term_cd = t.term_id
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


@fixture('query_advisee_sis_enrollments.csv')
def stream_sis_enrollments(sids=None):
    sql = f"""SELECT
                  enr.grade, enr.grade_midterm, enr.units, enr.grading_basis, enr.sis_enrollment_status, enr.sis_term_id,
                  enr.ldap_uid, enr.sid,
                  enr.sis_course_title, enr.sis_course_name, enr.sis_section_id,
                  enr.sis_primary, enr.sis_instruction_mode, enr.sis_instruction_format, enr.sis_section_num,
                  NULL::date AS drop_date, NULL::boolean AS dropped,
                  r.maximum_term_enrollment_units_limit AS max_term_units_allowed,
                  r.minimum_term_enrollment_units_limit AS min_term_units_allowed
              FROM {intermediate_schema()}.sis_enrollments enr
              LEFT JOIN {edl_external_schema()}.student_registration_term_data r
                  ON enr.sis_term_id = r.semester_year_term_cd AND enr.sid = r.student_id
              {'WHERE enr.sid = ANY(%s)' if sids else ''}
              UNION
              SELECT
                dr.grade, dr.grade_midterm, NULL::int as units, NULL::varchar as grading_basis, dr.sis_enrollment_status, dr.sis_term_id,
                dr.ldap_uid, dr.sid,
                dr.sis_course_title, dr.sis_course_name, dr.sis_section_id,
                NULL::boolean as sis_primary, dr.sis_instruction_mode, dr.sis_instruction_format, dr.sis_section_num,
                LEFT(e.drop_date, 10)::date AS drop_date, TRUE as dropped,
                r.maximum_term_enrollment_units_limit AS max_term_units_allowed,
                r.minimum_term_enrollment_units_limit AS min_term_units_allowed
              FROM {intermediate_schema()}.sis_dropped_classes AS dr
              LEFT JOIN {edl_external_schema()}.student_registration_term_data r
                  ON dr.sis_term_id = r.semester_year_term_cd AND dr.sid = r.student_id
              LEFT JOIN {edl_schema()}.enrollments e
                ON dr.ldap_uid = e.ldap_uid
                AND dr.sis_term_id = e.term_id
                AND dr.sis_section_id = e.section_id
              {'WHERE dr.sid = ANY(%s)' if sids else ''}
              ORDER BY sis_term_id DESC, sid, dropped NULLS FIRST, sis_course_name, sis_primary DESC, sis_instruction_format, sis_section_num
        """
    params = (sids, sids) if sids else None
    return redshift.fetch(sql, params=params, stream_results=True)


def stream_term_gpas(sids=None):
    if sids:
        sid_filter = 'WHERE gp.sid = ANY(%s)'
    else:
        sid_filter = ''
    sql = f"""SELECT gp.sid, gp.term_id, gp.gpa, gp.units_taken_for_gpa
              FROM {edl_schema()}.term_gpa gp
              {sid_filter}
              ORDER BY gp.term_id DESC, gp.sid, CASE gp.career WHEN 'UGRD' THEN 1 ELSE 0 END
        """
    params = (sids,) if sids else None
    return redshift.fetch(sql, params=params, stream_results=True)


def stream_canvas_memberships():
    sql = f"""SELECT term_id, sid, sis_section_ids, feed
        FROM {student_schema()}.student_canvas_site_memberships
        ORDER BY term_id DESC, sid"""
    return redshift.fetch(sql, stream_results=True)
