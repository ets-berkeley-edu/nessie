/**
 * Copyright ©2021. The Regents of the University of California (Regents). All Rights Reserved.
 *
 * Permission to use, copy, modify, and distribute this software and its documentation
 * for educational, research, and not-for-profit purposes, without fee and without a
 * signed licensing agreement, is hereby granted, provided that the above copyright
 * notice, this paragraph and the following two paragraphs appear in all copies,
 * modifications, and distributions.
 *
 * Contact The Office of Technology Licensing, UC Berkeley, 2150 Shattuck Avenue,
 * Suite 510, Berkeley, CA 94720-1620, (510) 643-7201, otl@berkeley.edu,
 * http://ipira.berkeley.edu/industry-info for commercial licensing opportunities.
 *
 * IN NO EVENT SHALL REGENTS BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 * THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF REGENTS HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
 * SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED
 * "AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
 * ENHANCEMENTS, OR MODIFICATIONS.
 */

--------------------------------------------------------------------
-- External schema
--------------------------------------------------------------------

DROP SCHEMA IF EXISTS {redshift_schema_edl_external_staging};
CREATE EXTERNAL SCHEMA {redshift_schema_edl_external_staging}
FROM data catalog
DATABASE 'cs_staging'
IAM_ROLE '{redshift_iam_role},{edl_iam_role}';

DROP SCHEMA IF EXISTS {redshift_schema_edl_external};
CREATE EXTERNAL SCHEMA {redshift_schema_edl_external}
FROM data catalog
DATABASE 'cs_analytics'
IAM_ROLE '{redshift_iam_role},{edl_iam_role}';

--------------------------------------------------------------------
-- Internal schema
--------------------------------------------------------------------

CREATE OR REPLACE FUNCTION {redshift_schema_edl}.to_utc_iso_string(date_string VARCHAR)
RETURNS VARCHAR
STABLE
AS $$
  from datetime import datetime
  import pytz

  d = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
  d = pytz.timezone('America/Los_Angeles').localize(d)
  return d.astimezone(pytz.utc).isoformat()
$$ language plpythonu;

GRANT EXECUTE
ON function {redshift_schema_edl}.to_utc_iso_string(VARCHAR)
TO GROUP {redshift_app_boa_user}_group;

DROP SCHEMA IF EXISTS {redshift_schema_edl} CASCADE;
CREATE SCHEMA {redshift_schema_edl};
GRANT USAGE ON SCHEMA {redshift_schema_edl} TO GROUP {redshift_dblink_group};
ALTER DEFAULT PRIVILEGES IN SCHEMA {redshift_schema_edl} GRANT SELECT ON TABLES TO GROUP {redshift_dblink_group};

--------------------------------------------------------------------
-- Internal tables
--------------------------------------------------------------------

CREATE TABLE {redshift_schema_edl}.academic_standing
SORTKEY (sid)
AS (
    SELECT
        student_id AS sid,
        semester_year_term_cd AS term_id,
        academic_standing_cd AS acad_standing_action,
        academic_standing_category_cd AS acad_standing_status,
        action_dt AS action_date
    FROM {redshift_schema_edl_external}.student_academic_standing_data
);

CREATE TABLE {redshift_schema_edl}.advising_notes
SORTKEY (id)
AS (
    SELECT DISTINCT
      student_id || '-' || note_id AS id,
      student_id AS sid,
      note_id AS student_note_nr,
      advisor_id AS advisor_sid,
      appointment_id,
      note_type_desc AS note_category,
      note_subtype_desc AS note_subcategory,
      note_text AS note_body,
      created_by_uid AS created_by,
      DATE_TRUNC('second', create_tmsp AT TIME ZONE 'America/Los_Angeles') AS created_at,
      updated_by_uid AS updated_by,
      DATE_TRUNC('second', update_tmsp AT TIME ZONE 'America/Los_Angeles') AS updated_at,
      load_dt AS edl_load_date
    FROM
      {redshift_schema_edl_external}.student_advising_notes_data
);

CREATE TABLE {redshift_schema_edl}.advising_note_attachments
INTERLEAVED SORTKEY (advising_note_id, sis_file_name)
AS (
    SELECT
      A.student_id || '-' || N.note_id AS advising_note_id,
      A.student_id AS sid,
      A.note_id AS student_note_nr,
      N.created_by_uid AS created_by,
      A.note_attachment_upload_filename AS user_file_name,
      A.note_attachment_derived_filename AS sis_file_name,
      A.load_dt AS edl_load_date,
      FALSE AS is_historical
    FROM
        {redshift_schema_edl_external}.student_advising_note_attachments_data A
    JOIN
        {redshift_schema_edl_external}.student_advising_notes_data N
    ON A.student_id = N.student_id
    AND A.note_id = N.note_id
);

CREATE TABLE {redshift_schema_edl}.advising_note_topics
SORTKEY (advising_note_id)
AS (
  SELECT
    student_id || '-' || note_id AS advising_note_id,
    student_id AS sid,
    note_id AS student_note_nr,
    note_topic_cd AS note_topic
  FROM {redshift_schema_edl_external}.student_advising_notes_data
);

CREATE TABLE {redshift_schema_edl}.courses
SORTKEY (section_id)
AS (
  SELECT
    class.class_number::int AS section_id,
    class.semester_year_term_cd AS term_id,
    class.instructional_format_nm AS instruction_format,
    class.class_section_cd AS section_num,
    class.course_id_display_desc AS course_display_name,
    class.course_title_long AS course_title,
    DECODE(class.graded_section_flg,
      'N', '0',
      'Y', '1'
    )::integer::boolean AS is_primary,
    class.enrollment_total_nbr AS enrollment_count,
    class.units_maximum_nbr AS allowed_units,
    class.instructional_format_nm AS instruction_mode,
    instr.instructor_calnet_uid AS instructor_uid,
    instr.instructor_preferred_display_nm AS instructor_name,
    instr.instructor_function_cd AS instructor_role_code,
    meet.room_desc AS meeting_location,
    -- TODO: While using a feature flag to toggle between sources, this crude but functional SQL snarl translates EDL
    -- weekday patterns to the format we get from SISEDO.
    replace(
      replace(
      replace(
      replace(
      replace(
      replace(
      replace(meet.meeting_days_cd, 'S', 'SA'),
      'U', 'SU'),
      'M', 'MO'),
      'T', 'TU'),
      'W', 'WE'),
      'R', 'TH'),
      'F', 'FR'
    ) AS meeting_days,
    -- Throw away meaningless date and second-precision data from EDL meeting times.
    left(right(meet.meeting_start_time, 8), 5) AS meeting_start_time,
    left(right(meet.meeting_end_time, 8), 5) AS meeting_end_time,
    meet.meeting_start_date AS meeting_start_date,
    meet.meeting_end_date AS meeting_end_date
  FROM {redshift_schema_edl_external}.student_class_data class
  LEFT JOIN {redshift_schema_edl_external}.student_class_instructor_data instr
    ON class.class_number = instr.class_number
    AND class.semester_year_term_cd = instr.semester_year_term_cd
  LEFT JOIN {redshift_schema_edl_external}.student_class_meeting_pattern_data meet
    ON class.class_number = meet.class_number
    AND class.semester_year_term_cd = meet.semester_year_term_cd
    AND (meet.class_meeting_number = instr.class_meeting_number OR instr.class_meeting_number IS NULL)
);

CREATE TABLE {redshift_schema_edl}.demographics
(
    sid VARCHAR NOT NULL,
    gender VARCHAR,
    minority BOOLEAN
)
DISTKEY (sid)
SORTKEY (sid);

CREATE TABLE {redshift_schema_edl}.enrollments
SORTKEY (sis_id)
AS (
  SELECT
    sed.class_number::int AS section_id,
    sed.semester_year_term_cd AS term_id,
    spd.calnet_uid AS ldap_uid,
    sed.student_id AS sis_id,
    sed.enrollment_action_cd AS enrollment_status,
    sed.wait_list_position_cd AS waitlist_position,
    sed.enrollment_drop_dt AS drop_date,
    sed.units AS units,
    sed.grd AS grade,
    sed.final_grade_points AS grade_points,
    sed.grading_basis_enrollment_cd AS grading_basis,
    sed.midterm_course_grade_input_cd AS grade_midterm
  FROM {redshift_schema_edl_external}.student_enrollment_data sed
  LEFT JOIN {redshift_schema_edl_external}.student_personal_data spd
    ON spd.student_id = sed.student_id
);

CREATE TABLE {redshift_schema_edl}.ethnicities
(
    sid VARCHAR NOT NULL,
    ethnicity VARCHAR
)
DISTKEY (sid)
SORTKEY (sid, ethnicity);

CREATE TABLE {redshift_schema_edl}.intended_majors
DISTKEY (sid)
SORTKEY (sid, plan_code)
AS (
    SELECT
      student_id AS sid,
      intended_academic_plan_cd_1 AS plan_code,
      academic_program_status_cd AS academic_program_status_code,
      academic_program_effective_dt AS effective_date
      FROM {redshift_schema_edl_external}.student_academic_plan_data
      WHERE intended_academic_plan_cd_1 IS NOT NULL
      GROUP BY student_id, intended_academic_plan_cd_1, academic_program_status_cd, academic_program_effective_dt
    UNION
    SELECT
      student_id AS sid,
      intended_academic_plan_cd_2 AS plan_code,
      academic_program_status_cd AS academic_program_status_code,
      academic_program_effective_dt AS effective_date
      FROM {redshift_schema_edl_external}.student_academic_plan_data
      WHERE intended_academic_plan_cd_2 IS NOT NULL
      GROUP BY student_id, intended_academic_plan_cd_2, academic_program_status_cd, academic_program_effective_dt
);

CREATE TABLE {redshift_schema_edl}.minors
(
    sid VARCHAR NOT NULL,
    minor VARCHAR NOT NULL
)
DISTKEY (sid)
SORTKEY (sid, minor);

CREATE TABLE {redshift_schema_edl}.visas
(
    sid VARCHAR NOT NULL,
    visa_status VARCHAR,
    visa_type VARCHAR
)
DISTKEY (sid)
SORTKEY (sid);

CREATE TABLE {redshift_schema_edl}.student_academic_plan_index
SORTKEY (sid)
AS (
    SELECT
      student_id AS sid,
      academic_program_nm AS program,
      academic_plan_nm AS plan,
      academic_plan_type_cd AS plan_type,
      academic_subplan_nm AS subplan,
      academic_program_status_desc AS status,
      load_dt AS edl_load_date
    FROM {redshift_schema_edl_external}.student_academic_plan_data
    WHERE academic_plan_type_cd IN ('MAJ', 'SS', 'SP', 'HS', 'CRT', 'MIN')
);

CREATE TABLE {redshift_schema_edl}.student_academic_plans
(
    sid VARCHAR NOT NULL,
    feed VARCHAR(max) NOT NULL
)
DISTKEY (sid)
SORTKEY (sid);

CREATE TABLE {redshift_schema_edl}.student_citizenships
SORTKEY(sid)
AS (
    SELECT
      student_id AS sid,
      citizenship_country_desc AS citizenship_country,
      load_dt AS edl_load_date
    FROM {redshift_schema_edl_external}.student_citizenship_data
);

CREATE TABLE {redshift_schema_edl}.student_degree_progress_index
SORTKEY (sid)
AS (
    SELECT
      student_id AS sid,
      reporting_dt AS report_date,
      CASE
        WHEN requirement_cd = '000000001' THEN 'entryLevelWriting'
        WHEN requirement_cd = '000000002' THEN 'americanHistory'
        WHEN requirement_cd = '000000003' THEN 'americanCultures'
        WHEN requirement_cd = '000000018' THEN 'americanInstitutions'
        ELSE NULL
      END AS requirement,
      requirement_desc,
      CASE
        WHEN requirement_status_cd = 'COMP' AND in_progress_grade_flg = 'Y' THEN 'In Progress'
        WHEN requirement_status_cd = 'COMP' AND in_progress_grade_flg = 'N' THEN 'Satisfied'
        ELSE 'Not Satisfied'
      END AS status,
      load_dt AS edl_load_date
    FROM {redshift_schema_edl_external}.student_academic_progress_data
    WHERE requirement_group_cd = '000131'
    AND requirement_cd in ('000000001', '000000002', '000000003', '000000018')
);

CREATE TABLE {redshift_schema_edl}.student_degree_progress
(
    sid VARCHAR NOT NULL,
    feed VARCHAR(max) NOT NULL
)
DISTKEY (sid)
SORTKEY (sid);

CREATE TABLE {redshift_schema_edl}.student_demographics
(
    sid VARCHAR NOT NULL,
    feed VARCHAR(max) NOT NULL
)
DISTKEY(sid)
SORTKEY(sid);

CREATE TABLE {redshift_schema_edl}.student_ethnicities
DISTKEY (sid)
SORTKEY (sid, ethnicity)
AS (
  SELECT
    student_id AS sid,
    ethnic_desc AS ethnicity,
    ethnic_rollup_desc AS ethnic_group,
    load_dt AS edl_load_date
  FROM {redshift_schema_edl_external}.student_ethnicity_data
);

CREATE TABLE {redshift_schema_edl}.student_late_drop_eforms
DISTKEY (eform_id)
SORTKEY (sid, term_id)
AS (
  SELECT
    student_id || '-eform:' || g3form_id || '-' || replace(g3form_last_update_tmsp, ' ', '_') AS id,
    academic_career_cd AS career_code,
    class_number::int AS section_id,
    class_section_cd AS section_num,
    course_id_display_desc AS course_display_name,
    course_title_nm AS course_title,
    g3form_id::int AS eform_id,
    TO_TIMESTAMP({redshift_schema_edl}.to_utc_iso_string(g3form_last_update_tmsp), 'YYYY-MM-DD"T"HH.MI.SS%z') AS updated_at,
    TO_TIMESTAMP({redshift_schema_edl}.to_utc_iso_string(g3form_origination_dt), 'YYYY-MM-DD"T"HH.MI.SS%z') AS created_at,
    g3form_status_desc AS eform_status,
    g3form_type_cd AS eform_type,
    grading_basis_enrollment_cd AS grading_basis_code,
    grading_basis_enrollment_desc AS grading_basis_description,
    load_dt AS edl_load_date,
    semester_year_term_cd AS term_id,
    student_id AS sid,
    person_display_nm AS student_name,
    requested_action_desc AS requested_action,
    requested_grading_basis_cd AS requested_grading_basis_code,
    requested_grading_basis_desc AS requested_grading_basis_description,
    units_taken
  FROM {redshift_schema_edl_external}.student_late_drop_eform_data
);

-- TODO: EDL equivalent of 'sis_profiles'?
CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.sis_profiles
(
    sid VARCHAR NOT NULL,
    feed VARCHAR(max) NOT NULL
)
DISTKEY(sid)
SORTKEY(sid);

-- TODO: EDL equivalent of 'sis_profiles_hist_enr'?
CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.sis_profiles_hist_enr
(
    sid VARCHAR NOT NULL,
    uid VARCHAR,
    feed VARCHAR(max) NOT NULL
)
DISTKEY(sid)
SORTKEY(sid);

CREATE TABLE {redshift_schema_edl}.student_majors
DISTKEY (sid)
SORTKEY (college, major)
AS (
    SELECT
      student_id AS sid,
      academic_plan_nm AS major,
      academic_program_nm AS college,
      load_dt AS edl_load_date
    FROM {redshift_schema_edl_external}.student_academic_plan_data
    WHERE academic_plan_type_cd in ('MAJ', 'SS', 'SP', 'HS', 'CRT')
    AND academic_program_status_cd = 'AC'
);

CREATE TABLE {redshift_schema_edl}.student_minors
DISTKEY (sid)
SORTKEY (sid, minor)
AS (
    SELECT
      student_id AS sid,
      academic_plan_nm AS minor,
      load_dt AS edl_load_date
    FROM {redshift_schema_edl_external}.student_academic_plan_data
    WHERE academic_plan_type_cd = 'MIN'
    AND academic_program_status_cd = 'AC'
);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.student_last_registrations
(
    sid VARCHAR NOT NULL,
    feed VARCHAR(max) NOT NULL
)
DISTKEY(sid)
SORTKEY(sid);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.student_profiles
(
    sid VARCHAR NOT NULL,
    profile VARCHAR(max) NOT NULL
)
DISTKEY (sid)
SORTKEY (sid);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.student_term_gpas
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    gpa DECIMAL(5,3),
    units_taken_for_gpa DECIMAL(4,1)
)
DISTKEY (sid)
SORTKEY (sid, term_id);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.hist_enr_last_registrations
(
    sid VARCHAR NOT NULL,
    feed VARCHAR(max) NOT NULL
)
DISTKEY(sid)
SORTKEY(sid);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.hist_enr_term_gpas
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    gpa DECIMAL(5,3),
    units_taken_for_gpa DECIMAL(4,1)
)
DISTKEY (sid)
SORTKEY (sid, term_id);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.student_enrollment_terms
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    enrollment_term VARCHAR(max) NOT NULL
)
DISTKEY (sid)
SORTKEY (sid, term_id);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.student_enrollment_terms_hist_enr
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    enrollment_term VARCHAR(max) NOT NULL
)
DISTKEY (sid)
SORTKEY (sid, term_id);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.student_holds
(
    sid VARCHAR NOT NULL,
    feed VARCHAR(max) NOT NULL
)
DISTKEY (sid)
SORTKEY (sid);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_names_hist_enr
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL
)
DISTKEY (sid)
SORTKEY (sid);

CREATE TABLE {redshift_schema_edl}.student_profile_index
DISTKEY (units)
INTERLEAVED SORTKEY (sid, last_name, level, gpa, units, uid, first_name)
AS (
    SELECT
      reg.student_id AS sid,
      s.campus_id AS uid,
      p.person_preferred_first_nm AS first_name,
      p.person_preferred_last_nm AS last_name,
      p.gender_cd AS gender,
      NULL AS level,
      reg.total_cumulative_gpa_nbr AS gpa,
      reg.total_units_completed_qty AS units,
      NULL AS transfer,
      reg.expected_graduation_term AS expected_grad_term,
      reg.terms_in_attendance,
      reg.load_dt AS edl_load_date
    FROM {redshift_schema_edl_external}.student_registration_term_data reg
    JOIN {redshift_schema_edl_external}.student_personal_data p
    ON reg.student_id = p.student_id
    JOIN {redshift_schema_edl_external_staging}.cs_ps_person_sa s
    ON p.student_id = s.emplid
    AND s.campus_id <> ''
    WHERE reg.semester_year_term_cd = (
        SELECT MAX(semester_year_term_cd)
        FROM {redshift_schema_edl_external}.student_registration_term_data max_reg
        WHERE max_reg.student_id = reg.student_id
    )
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_profile_index_hist_enr
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    level VARCHAR,
    gpa DECIMAL(5,3),
    units DECIMAL (6,3),
    transfer BOOLEAN,
    expected_grad_term VARCHAR(4),
    terms_in_attendance INT
)
DISTKEY (units)
INTERLEAVED SORTKEY (sid, last_name, level, gpa, units, uid, first_name);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_profiles_hist_enr
(
    sid VARCHAR NOT NULL,
    uid VARCHAR,
    profile VARCHAR(max) NOT NULL
)
DISTKEY (sid)
SORTKEY (sid);

-- Equivalent to external table {redshift_schema_sis}.term_gpa. Distinct from student_term_gpas and hist_enr_term_gpas
-- above, which mimic API-sourced data.

CREATE TABLE {redshift_schema_edl}.term_gpa
SORTKEY(sid)
AS (
    SELECT
      reg.student_id AS sid,
      reg.semester_year_term_cd AS term_id,
      reg.term_berkeley_completed_total_units AS units_total,
      reg.term_berkeley_completed_gpa_units AS units_taken_for_gpa,
      reg.current_term_gpa_nbr AS gpa
    FROM {redshift_schema_edl_external}.student_registration_term_data reg
);

CREATE TABLE {redshift_schema_edl}.student_visas
SORTKEY(sid)
AS (
    SELECT
      student_id AS sid,
      visa_workpermit_status_cd AS visa_status,
      visa_permit_type_cd AS visa_type,
      load_dt AS edl_load_date
    FROM {redshift_schema_edl_external}.student_visa_permit_data
    WHERE country_cd = 'USA'
    AND visa_permit_type_cd IS NOT NULL
);

DROP FUNCTION {redshift_schema_edl}.to_utc_iso_string(VARCHAR);
