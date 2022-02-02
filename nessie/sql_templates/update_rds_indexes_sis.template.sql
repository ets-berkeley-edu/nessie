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

DROP TABLE IF EXISTS {rds_schema_sis_internal}.academic_plan_hierarchy CASCADE;

CREATE TABLE IF NOT EXISTS {rds_schema_sis_internal}.academic_plan_hierarchy
(
    plan_code VARCHAR,
    plan_status VARCHAR,
    plan_name VARCHAR,
    major_code VARCHAR,
    major_name VARCHAR,
    plan_type_code VARCHAR,
    department_code VARCHAR,
    department_name VARCHAR,
    division_code VARCHAR,
    division_name VARCHAR,
    college_code VARCHAR,
    college_name VARCHAR,
    career_code VARCHAR,
    career_name VARCHAR,
    program_code VARCHAR,
    program_name VARCHAR,
    degree_code VARCHAR,
    degree_name VARCHAR
);

INSERT INTO {rds_schema_sis_internal}.academic_plan_hierarchy (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT plan_code, plan_status, plan_name, major_code, major_name, plan_type_code,
      department_code, department_name, division_code, division_name,
      college_code, college_name, career_code, career_name,
      program_code, program_name, degree_code, degree_name
    FROM {redshift_schema_edl}.academic_plan_hierarchy
  $REDSHIFT$)
  AS redshift_academic_plan_hierarchy (
    plan_code VARCHAR,
    plan_status VARCHAR,
    plan_name VARCHAR,
    major_code VARCHAR,
    major_name VARCHAR,
    plan_type_code VARCHAR,
    department_code VARCHAR,
    department_name VARCHAR,
    division_code VARCHAR,
    division_name VARCHAR,
    college_code VARCHAR,
    college_name VARCHAR,
    career_code VARCHAR,
    career_name VARCHAR,
    program_code VARCHAR,
    program_name VARCHAR,
    degree_code VARCHAR,
    degree_name VARCHAR
  )
);

DROP TABLE IF EXISTS {rds_schema_sis_internal}.basic_attributes CASCADE;

CREATE TABLE IF NOT EXISTS {rds_schema_sis_internal}.basic_attributes (
    ldap_uid VARCHAR,
    sid VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email_address VARCHAR,
    affiliations VARCHAR,
    person_type VARCHAR
);

INSERT INTO {rds_schema_sis_internal}.basic_attributes (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT ldap_uid, sid, first_name, last_name, email_address, affiliations, person_type
    FROM {redshift_schema_edl}.basic_attributes
  $REDSHIFT$)
  AS redshift_basic_attributes (
    ldap_uid VARCHAR,
    sid VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email_address VARCHAR,
    affiliations VARCHAR,
    person_type VARCHAR
  )
);

CREATE INDEX idx_basic_attributes_ldap_uid ON {rds_schema_sis_internal}.basic_attributes(ldap_uid);
CREATE INDEX idx_basic_attributes_sid ON {rds_schema_sis_internal}.basic_attributes(sid);

GRANT SELECT ON TABLE {rds_schema_sis_internal}.basic_attributes to {rds_dblink_role_damien};

DROP TABLE IF EXISTS {rds_schema_sis_internal}.sis_enrollments CASCADE;

CREATE TABLE IF NOT EXISTS {rds_schema_sis_internal}.sis_enrollments
(
    sis_term_id VARCHAR,
    sis_section_id VARCHAR,
    ldap_uid VARCHAR,
    sis_enrollment_status VARCHAR,
    units DOUBLE PRECISION,
    grading_basis VARCHAR,
    grade VARCHAR,
    grade_midterm VARCHAR,
    sis_course_title VARCHAR,
    sis_course_name VARCHAR,
    sis_primary BOOLEAN,
    sis_instruction_format VARCHAR,
    sis_section_num VARCHAR
);

INSERT INTO {rds_schema_sis_internal}.sis_enrollments (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT sis_term_id, sis_section_id, ldap_uid, sis_enrollment_status, units, grading_basis,
      grade, grade_midterm, sis_course_title, sis_course_name, sis_primary, sis_instruction_format, sis_section_num
    FROM {redshift_schema_intermediate}.sis_enrollments
  $REDSHIFT$)
  AS redshift_sis_enrollments (
    sis_term_id VARCHAR,
    sis_section_id VARCHAR,
    ldap_uid VARCHAR,
    sis_enrollment_status VARCHAR,
    units DOUBLE PRECISION,
    grading_basis VARCHAR,
    grade VARCHAR,
    grade_midterm VARCHAR,
    sis_course_title VARCHAR,
    sis_course_name VARCHAR,
    sis_primary BOOLEAN,
    sis_instruction_format VARCHAR,
    sis_section_num VARCHAR
  )
);

CREATE INDEX idx_sis_enrollments_term_id_section_id ON {rds_schema_sis_internal}.sis_enrollments(sis_term_id, sis_section_id);
CREATE INDEX idx_sis_enrollments_ldap_uid ON {rds_schema_sis_internal}.sis_enrollments(ldap_uid);

GRANT SELECT ON TABLE {rds_schema_sis_internal}.sis_enrollments to {rds_dblink_role_damien};

DROP TABLE IF EXISTS {rds_schema_sis_internal}.sis_sections CASCADE;

CREATE TABLE IF NOT EXISTS {rds_schema_sis_internal}.sis_sections
(
    sis_term_id VARCHAR,
    sis_section_id VARCHAR,
    is_primary BOOLEAN,
    sis_course_name VARCHAR,
    sis_course_title VARCHAR,
    sis_instruction_format VARCHAR,
    sis_section_num VARCHAR,
    allowed_units DOUBLE PRECISION,
    instruction_mode VARCHAR,
    instructor_uid VARCHAR,
    instructor_name VARCHAR,
    instructor_role_code VARCHAR,
    meeting_location VARCHAR,
    meeting_days VARCHAR,
    meeting_start_time VARCHAR,
    meeting_end_time VARCHAR,
    meeting_start_date VARCHAR,
    meeting_end_date VARCHAR
);

INSERT INTO {rds_schema_sis_internal}.sis_sections (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT sis_term_id, sis_section_id, is_primary, sis_course_name, sis_course_title, sis_instruction_format,
      sis_section_num, allowed_units, instruction_mode,
      instructor_uid, instructor_name, instructor_role_code, meeting_location,
      meeting_days, meeting_start_time, meeting_end_time, meeting_start_date, meeting_end_date
    FROM {redshift_schema_intermediate}.sis_sections
  $REDSHIFT$)
  AS redshift_sis_sections (
    sis_term_id VARCHAR,
    sis_section_id VARCHAR,
    is_primary BOOLEAN,
    sis_course_name VARCHAR,
    sis_course_title VARCHAR,
    sis_instruction_format VARCHAR,
    sis_section_num VARCHAR,
    allowed_units DOUBLE PRECISION,
    instruction_mode VARCHAR,
    instructor_uid VARCHAR,
    instructor_name VARCHAR,
    instructor_role_code VARCHAR,
    meeting_location VARCHAR,
    meeting_days VARCHAR,
    meeting_start_time VARCHAR,
    meeting_end_time VARCHAR,
    meeting_start_date VARCHAR,
    meeting_end_date VARCHAR
  )
);

CREATE INDEX idx_sis_sections_term_id_section_id ON {rds_schema_sis_internal}.sis_sections(sis_term_id, sis_section_id);

GRANT SELECT ON TABLE {rds_schema_sis_internal}.sis_enrollments to {rds_dblink_role_damien};
GRANT SELECT ON TABLE {rds_schema_sis_internal}.sis_sections to {rds_dblink_role_diablo};
