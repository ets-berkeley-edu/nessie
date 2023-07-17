/**
 * Copyright ©2023. The Regents of the University of California (Regents). All Rights Reserved.
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

CREATE EXTERNAL SCHEMA {redshift_schema_sisedo}
FROM data catalog
DATABASE '{redshift_schema_sisedo}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

CREATE EXTERNAL TABLE {redshift_schema_sisedo}.basic_attributes
(
   ldap_uid VARCHAR,
   sid VARCHAR,
   first_name VARCHAR,
   last_name VARCHAR,
   email_address VARCHAR,
   affiliations VARCHAR,
   person_type VARCHAR,
   alternateid VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{sisedo_data_path}/basic-attributes';

CREATE EXTERNAL TABLE {redshift_schema_sisedo}.courses
(
   section_id VARCHAR,
   term_id VARCHAR,
   session_id VARCHAR,
   dept_name VARCHAR,
   dept_code VARCHAR,
   course_career_code VARCHAR,
   print_in_schedule_of_classes VARCHAR,
   is_primary VARCHAR,
   instruction_format VARCHAR,
   primary_associated_section_id VARCHAR,
   section_display_name VARCHAR,
   section_num VARCHAR,
   course_display_name VARCHAR,
   catalog_id VARCHAR,
   catalog_root VARCHAR,
   catalog_prefix VARCHAR,
   catalog_suffix VARCHAR,
   course_updated_date VARCHAR,
   course_version_independent_id VARCHAR,
   enrollment_count INTEGER,
   enroll_limit INTEGER,
   waitlist_limit INTEGER,
   start_date VARCHAR,
   end_date VARCHAR,
   instructor_uid VARCHAR,
   instructor_name VARCHAR,
   instructor_role_code VARCHAR,
   location VARCHAR,
   meeting_days VARCHAR,
   meeting_start_time VARCHAR,
   meeting_end_time VARCHAR,
   meeting_start_date VARCHAR,
   meeting_end_date VARCHAR,
   course_title VARCHAR,
   course_title_short VARCHAR,
   instruction_mode VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{sisedo_data_path}/courses';

CREATE EXTERNAL TABLE {redshift_schema_sisedo}.enrollments
(
   section_id VARCHAR,
   term_id VARCHAR,
   session_id VARCHAR,
   ldap_uid VARCHAR,
   sis_id VARCHAR,
   enrollment_status VARCHAR,
   waitlist_position VARCHAR,
   units VARCHAR,
   grade VARCHAR,
   grade_points VARCHAR,
   grading_basis VARCHAR,
   grade_midterm VARCHAR,
   institution VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{sisedo_data_path}/enrollments';

CREATE EXTERNAL TABLE {redshift_schema_sisedo}.enrollment_updates
(
   class_section_id VARCHAR,
   term_id VARCHAR,
   ldap_uid VARCHAR,
   sis_id VARCHAR,
   enroll_status VARCHAR,
   course_career VARCHAR,
   last_updated VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{sisedo_data_path}/enrollment_updates';

CREATE EXTERNAL TABLE {redshift_schema_sisedo}.instructor_updates
(
   sis_id VARCHAR,
   term_id VARCHAR,
   section_id VARCHAR,
   course_id VARCHAR,
   ldap_uid VARCHAR,
   role_code VARCHAR,
   is_primary VARCHAR,
   last_updated VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{sisedo_data_path}/instructor_updates';

--------------------------------------------------------------------
-- Internal Schema
--------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS {redshift_schema_sisedo_internal};
GRANT USAGE ON SCHEMA {redshift_schema_sisedo_internal} TO GROUP {redshift_dblink_group};
ALTER DEFAULT PRIVILEGES IN SCHEMA {redshift_schema_sisedo_internal} GRANT SELECT ON TABLES TO GROUP {redshift_dblink_group};

--------------------------------------------------------------------
-- Internal Tables
--------------------------------------------------------------------

DROP TABLE IF EXISTS {redshift_schema_sisedo_internal}.basic_attributes;

CREATE TABLE {redshift_schema_sisedo_internal}.basic_attributes
SORTKEY (ldap_uid)
AS (
  SELECT DISTINCT
   ldap_uid,
   sid,
   -- First names occasionally come in with quote literals that require unescaping.
   replace(first_name, '""', '"') AS first_name,
   last_name,
   email_address,
   affiliations,
   person_type,
   alternateid
  FROM {redshift_schema_sisedo}.basic_attributes
);

DROP TABLE IF EXISTS {redshift_schema_sisedo_internal}.courses;

CREATE TABLE {redshift_schema_sisedo_internal}.courses
SORTKEY (sis_term_id, sis_section_id)
AS (
  SELECT DISTINCT
    term_id AS sis_term_id,
    section_id AS sis_section_id,
    is_primary AS is_primary,
    section_display_name AS sis_course_name,
    course_title AS sis_course_title,
    instruction_format AS sis_instruction_format,
    section_num AS sis_section_num,
    course_version_independent_id AS cs_course_id,
    session_id AS session_code,
    primary_associated_section_id,
    instruction_mode,
    instructor_uid,
    instructor_name,
    instructor_role_code,
    location AS meeting_location,
    meeting_days,
    meeting_start_time,
    meeting_end_time,
    meeting_start_date,
    meeting_end_date,
    enrollment_count,
    enroll_limit,
    waitlist_limit
  FROM {redshift_schema_sisedo}.courses
);

DROP TABLE IF EXISTS {redshift_schema_sisedo_internal}.enrollments;

CREATE TABLE {redshift_schema_sisedo_internal}.enrollments
SORTKEY (sis_term_id, sis_section_id)
AS (
  SELECT DISTINCT
    term_id AS sis_term_id,
    section_id AS sis_section_id,
    ldap_uid AS ldap_uid,
    enrollment_status AS sis_enrollment_status,
    units,
    grading_basis,
    grade,
    grade_midterm
  FROM {redshift_schema_sisedo}.enrollments
);
