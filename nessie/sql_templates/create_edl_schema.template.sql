/**
 * Copyright ©2022. The Regents of the University of California (Regents). All Rights Reserved.
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

DROP SCHEMA IF EXISTS {redshift_schema_edl_external_edw};
CREATE EXTERNAL SCHEMA {redshift_schema_edl_external_edw}
FROM data catalog
DATABASE 'edw_analytics'
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

CREATE TABLE {redshift_schema_edl}.academic_plan_hierarchy
SORTKEY (plan_code)
AS (
    SELECT
      academic_plan_cd AS plan_code,
      academic_plan_effdt AS effective_date,
      academic_plan_status AS plan_status,
      academic_plan_short_nm AS plan_short_name,
      academic_plan_nm AS plan_name,
      major_cd AS major_code,
      major_nm AS major_name,
      academic_plan_type_cd AS plan_type_code,
      academic_plan_type_shrt_nm AS plan_type_short_name,
      academic_plan_type_nm AS plan_type_name,
      academic_department_cd AS department_code,
      academic_department_short_nm AS department_short_name,
      academic_department_nm AS department_name,
      academic_division_cd AS division_code,
      academic_division_shrt_nm AS division_short_name,
      academic_division_nm AS division_name,
      reporting_college_school_letter_cd AS college_code,
      reporting_clg_school_short_nm AS college_short_name,
      reporting_college_school_nm AS college_name,
      academic_career_cd AS career_code,
      academic_career_short_nm AS career_short_name,
      academic_career_nm AS career_name,
      academic_program_cd AS program_code,
      academic_program_shrt_nm AS program_short_name,
      academic_program_nm AS program_name,
      degree_offered_cd AS degree_code,
      degree_offered_nm AS degree_name
    FROM {redshift_schema_edl_external}.student_academic_plan_hierarchy_data
);

CREATE TABLE {redshift_schema_edl}.academic_standing
SORTKEY (sid)
AS (
    SELECT
        s.student_id AS sid,
        s.semester_year_term_cd AS term_id,
        s.academic_standing_cd AS acad_standing_action,
        s.academic_standing_category_cd AS acad_standing_status,
        s.action_dt AS action_date
    FROM {redshift_schema_edl_external}.student_academic_standing_data s
    JOIN (
      SELECT student_id, semester_year_term_cd, 
      MAX(academic_standing_effective_dt || academic_standing_effective_dt_seq) AS eff_dt_seq
      FROM {redshift_schema_edl_external}.student_academic_standing_data
      GROUP BY student_id, semester_year_term_cd
    ) latest_actions
        ON s.student_id = latest_actions.student_id
        AND s.semester_year_term_cd = latest_actions.semester_year_term_cd
        AND s.academic_standing_effective_dt || s.academic_standing_effective_dt_seq = latest_actions.eff_dt_seq
    WHERE
        s.academic_standing_category_cd IS NOT NULL
        AND s.academic_standing_category_cd != ''
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
      A.load_dt AS edl_load_date
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

CREATE TABLE {redshift_schema_edl}.basic_attributes
SORTKEY (ldap_uid)
AS (
  SELECT
    calnet_uid::varchar AS ldap_uid,
    givenname AS first_name,
    sn AS last_name,
    officialemail AS email_address,
    stuid::varchar AS sid,
    affiliations,
    CASE ou
      WHEN 'people' THEN 'S'
      WHEN 'advcon people' THEN 'A'
      WHEN 'guests' THEN 'G'
      ELSE NULL END AS person_type
  FROM {redshift_schema_edl_external_edw}.edw_caldap_person
);

CREATE TABLE {redshift_schema_edl}.courses
SORTKEY (section_id)
AS (
  SELECT
    class.class_number::int AS section_id,
    class.semester_year_term_cd AS term_id,
    class.session_code AS session_code,
    class.instructional_format_nm AS instruction_format,
    class.class_section_cd AS section_num,
    class.course_id_display_desc AS course_display_name,
    class.course_title_long AS course_title,
    class.course_id AS cs_course_id,
    DECODE(class.graded_section_flg,
      'N', '0',
      'Y', '1'
    )::integer::boolean AS is_primary,
    class.enrollment_total_nbr AS enrollment_count,
    class.units_maximum_nbr AS allowed_units,
    class.instruction_mode_cd AS instruction_mode,
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
    AND (meet.class_meeting_number IS NOT NULL OR instr.class_meeting_number IS NULL)
);

CREATE TABLE {redshift_schema_edl}.enrollments
SORTKEY (sis_id)
AS (
  SELECT
    sed.class_number::int AS section_id,
    sed.semester_year_term_cd AS term_id,
    spd.calnet_uid AS ldap_uid,
    sed.student_id AS sis_id,
    sed.enrollment_action_cd AS enrollment_status,
    sed.enrollment_status_reason_cd as enrollment_status_reason,
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
  WHERE sed.enrollment_status_reason_cd != 'CANC' OR sed.enrollment_action_cd = 'D'
);

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
    'eform-' || ROW_NUMBER() OVER (ORDER BY g3form_origination_dt) AS id,
    academic_career_cd AS career_code,
    class_number::int AS section_id,
    class_section_cd AS section_num,
    course_id_display_desc AS course_display_name,
    course_title_nm AS course_title,
    g3form_id::int AS eform_id,
    DATE_TRUNC('second', g3form_last_update_tmsp AT TIME ZONE 'America/Los_Angeles') AS updated_at,
    DATE_TRUNC('second', g3form_origination_dt AT TIME ZONE 'America/Los_Angeles') AS created_at,
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
    feed VARCHAR(max) NOT NULL
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
      COALESCE(NULLIF(p.gender_identity_cd, ''), p.gender_cd) AS gender,
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

CREATE TABLE {redshift_schema_edl}.term_gpa
SORTKEY(sid)
AS (
    SELECT
      reg.student_id AS sid,
      reg.semester_year_term_cd AS term_id,
      reg.academic_career_cd AS career,
      reg.term_berkeley_completed_total_units AS units_total,
      reg.term_berkeley_completed_gpa_units AS units_taken_for_gpa,
      reg.current_term_gpa_nbr AS gpa
    FROM {redshift_schema_edl_external}.student_registration_term_data reg
);

-- Follow-up correction for a small number of past courses that are marked as non-primary (i.e. non-graded sections) in EDL
-- although enrollments did receive grades.

UPDATE {redshift_schema_edl}.courses
SET is_primary = TRUE
WHERE term_id || '-' || section_id IN
(
  SELECT c.term_id || '-' || c.section_id
  FROM edl_sis_data.courses c
  JOIN edl_sis_data.enrollments e
  ON c.term_id = e.term_id AND c.section_id = e.section_id
  AND c.is_primary IS FALSE
  AND e.grade != '' and e.grade != 'W'
);
