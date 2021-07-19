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

DROP SCHEMA IF EXISTS {redshift_schema_edl_external};
CREATE EXTERNAL SCHEMA {redshift_schema_edl_external}
FROM data catalog
DATABASE 'cs_analytics'
IAM_ROLE '{redshift_iam_role},{edl_iam_role}';

--------------------------------------------------------------------
-- Internal schema
--------------------------------------------------------------------

DROP SCHEMA IF EXISTS {redshift_schema_edl} CASCADE;
CREATE SCHEMA {redshift_schema_edl};
GRANT USAGE ON SCHEMA {redshift_schema_edl} TO GROUP {redshift_dblink_group};
ALTER DEFAULT PRIVILEGES IN SCHEMA {redshift_schema_edl} GRANT SELECT ON TABLES TO GROUP {redshift_dblink_group};

--------------------------------------------------------------------
-- Internal tables
--------------------------------------------------------------------

CREATE TABLE {redshift_schema_edl}.academic_standing
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR NOT NULL,
    acad_standing_action VARCHAR,
    acad_standing_status VARCHAR,
    action_date VARCHAR
)
DISTKEY (sid)
SORTKEY (sid);

CREATE TABLE {redshift_schema_edl}.courses
SORTKEY (section_id)
AS (
  WITH edl_classes AS (
    SELECT class_number, semester_year_term_cd, instructional_format_nm, class_section_cd,
      course_nm, course_title_long_nm, graded_section_flg, instruction_mode_cd,
      -- TODO: We find the max units actually enrolled as a placeholder for SIS max allowable units (probably not entirely conformant)
      MAX(units) AS max_units,
      COUNT(*) AS enrollment_count
    FROM {redshift_schema_edl_external}.student_enrollment_data
    GROUP BY class_number, semester_year_term_cd, instructional_format_nm, class_section_cd,
      course_nm, course_title_long_nm, graded_section_flg, instruction_mode_cd
  )
  SELECT
    edl_classes.class_number::int AS section_id,
    edl_classes.semester_year_term_cd AS term_id,
    edl_classes.instructional_format_nm AS instruction_format,
    edl_classes.class_section_cd AS section_num,
    edl_classes.course_nm AS course_display_name,
    edl_classes.course_title_long_nm AS course_title,
    DECODE(edl_classes.graded_section_flg,
      'N', '0',
      'Y', '1'
    )::integer::boolean AS is_primary,
    edl_classes.enrollment_count AS enrollment_count,
    edl_classes.max_units AS allowed_units,
    edl_classes.instruction_mode_cd AS instruction_mode,
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
    meet.meeting_start_time AS meeting_start_time,
    meet.meeting_end_time AS meeting_end_time,
    meet.meeting_start_date AS meeting_start_date,
    meet.meeting_end_date AS meeting_end_date
  FROM edl_classes
  LEFT JOIN {redshift_schema_edl_external}.student_class_instructor_data instr
    ON edl_classes.class_number = instr.class_number
    AND edl_classes.semester_year_term_cd = instr.semester_year_term_cd
  LEFT JOIN {redshift_schema_edl_external}.student_class_meeting_pattern_data meet
    ON edl_classes.class_number = meet.class_number
    AND edl_classes.semester_year_term_cd = meet.semester_year_term_cd
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
      intended_academic_plan_cd_1 AS plan_code
      FROM {redshift_schema_edl_external}.student_academic_plan_data
      WHERE intended_academic_plan_cd_1 IS NOT NULL
      GROUP BY student_id, intended_academic_plan_cd_1
    UNION
    SELECT
      student_id AS sid,
      intended_academic_plan_cd_2 AS plan_code
      FROM {redshift_schema_edl_external}.student_academic_plan_data
      WHERE intended_academic_plan_cd_2 IS NOT NULL
      GROUP BY student_id, intended_academic_plan_cd_2
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
