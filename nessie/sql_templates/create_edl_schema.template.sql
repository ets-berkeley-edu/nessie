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

CREATE TABLE {redshift_schema_edl}.student_ethnicities
DISTKEY (sid)
SORTKEY (sid, ethnicity)
AS (
  SELECT
    student_id AS sid,
    ethnic_desc AS ethnicity,
    ethnic_rollup_desc AS ethnicity_group,
    ethnic_hispanic_latino_flg AS hispanic_latino,
    load_dt AS edl_load_date
  FROM {redshift_schema_edl_external}.student_ethnicity_data
);

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

CREATE TABLE {redshift_schema_edl}.student_profile_index
DISTKEY (units)
INTERLEAVED SORTKEY (sid, last_name, level, gpa, units, uid, first_name)
AS (
    SELECT
      reg.student_id AS sid,
      NULL AS uid,
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
);
