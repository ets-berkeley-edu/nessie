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
-- CREATE EXTERNAL SCHEMA
--------------------------------------------------------------------

CREATE EXTERNAL SCHEMA {redshift_schema_sis}
FROM data catalog
DATABASE '{redshift_schema_sis}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

GRANT USAGE ON SCHEMA {redshift_schema_sis} TO GROUP {redshift_dblink_group};
ALTER DEFAULT PRIVILEGES IN SCHEMA {redshift_schema_sis} GRANT SELECT ON TABLES TO GROUP {redshift_dblink_group};
GRANT USAGE ON SCHEMA {redshift_schema_sis} TO GROUP {redshift_dblink_group_diablo};
ALTER DEFAULT PRIVILEGES IN SCHEMA {redshift_schema_sis} GRANT SELECT ON TABLES TO GROUP {redshift_dblink_group_diablo};

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

-- academic status
CREATE EXTERNAL TABLE {redshift_schema_sis}.academic_standing
(
    sid VARCHAR,
    term_id VARCHAR,
    acad_standing_action VARCHAR,
    acad_standing_action_descr VARCHAR,
    acad_standing_status VARCHAR,
    acad_standing_status_descr VARCHAR,
    action_date VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_data_path}/advisees/academic_standing';

-- basic attributes
CREATE EXTERNAL TABLE {redshift_schema_sis}.basic_attributes
(
    ldap_uid VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email_address VARCHAR,
    sid VARCHAR,
    affiliations VARCHAR,
    person_type VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_data_path}/advisees/basic_student_attributes';

-- courses
CREATE EXTERNAL TABLE {redshift_schema_sis}.courses(
    section_id INT,
    term_id INT,
    print_in_schedule_of_classes VARCHAR,
    is_primary BOOLEAN,
    instruction_format VARCHAR,
    section_num VARCHAR,
    course_display_name VARCHAR,
    enrollment_count INT,
    instructor_uid VARCHAR,
    instructor_name VARCHAR,
    instructor_role_code VARCHAR,
    meeting_location VARCHAR,
    meeting_days VARCHAR,
    meeting_start_time VARCHAR,
    meeting_end_time VARCHAR,
    meeting_start_date VARCHAR,
    meeting_end_date VARCHAR,
    course_title VARCHAR,
    allowed_units DOUBLE PRECISION,
    instruction_mode VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_data_path}/manifests/courses.json';

-- enrollments
CREATE EXTERNAL TABLE {redshift_schema_sis}.enrollments(
    section_id INT,
    term_id INT,
    ldap_uid VARCHAR,
    sis_id VARCHAR,
    enrollment_status VARCHAR,
    waitlist_position INT,
    units DOUBLE PRECISION,
    grade VARCHAR,
    grade_points DOUBLE PRECISION,
    grading_basis VARCHAR,
    grade_midterm VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_data_path}/manifests/enrollments.json';

-- intended majors
CREATE EXTERNAL TABLE {redshift_schema_sis}.intended_majors
(
    sid VARCHAR,
    plan_code VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_data_path}/advisees/intended_majors';

-- term gpa
CREATE EXTERNAL TABLE {redshift_schema_sis}.term_gpa
(
    sid VARCHAR,
    term_id INT,
    units_total DOUBLE PRECISION,
    units_taken_for_gpa DOUBLE PRECISION,
    gpa DOUBLE PRECISION
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_data_path}/historical/gpa';


--------------------------------------------------------------------
-- Internal schema
--------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS {redshift_schema_sis_internal};
GRANT USAGE ON SCHEMA {redshift_schema_sis_internal} TO GROUP {redshift_app_boa_user}_group;
ALTER default PRIVILEGES IN SCHEMA {redshift_schema_sis_internal} GRANT SELECT ON TABLES TO GROUP {redshift_app_boa_user}_group;
GRANT USAGE ON SCHEMA {redshift_schema_sis_internal} TO GROUP {redshift_dblink_group};
ALTER DEFAULT PRIVILEGES IN SCHEMA {redshift_schema_sis_internal} GRANT SELECT ON TABLES TO GROUP {redshift_dblink_group};

--------------------------------------------------------------------
-- Internal tables
--------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS {redshift_schema_sis_internal}.add_dates (
  sis_term_id INT,
  sis_section_id INT,
  ldap_uid VARCHAR,
  date DATE
)
DISTKEY (sis_term_id)
SORTKEY (sis_term_id, sis_section_id, ldap_uid);

CREATE TABLE IF NOT EXISTS {redshift_schema_sis_internal}.drop_dates (
  sis_term_id INT,
  sis_section_id INT,
  ldap_uid VARCHAR,
  date DATE
)
DISTKEY (sis_term_id)
SORTKEY (sis_term_id, sis_section_id, ldap_uid);

DROP TABLE IF EXISTS {redshift_schema_sis_internal}.academic_plan_hierarchy;

CREATE TABLE {redshift_schema_sis_internal}.academic_plan_hierarchy
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
