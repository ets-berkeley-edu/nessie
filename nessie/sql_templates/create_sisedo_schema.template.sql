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
   primary VARCHAR,
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
