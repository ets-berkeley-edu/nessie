/**
 * Copyright ©2019. The Regents of the University of California (Regents). All Rights Reserved.
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

CREATE EXTERNAL SCHEMA {redshift_schema_advisor}
FROM data catalog
DATABASE '{redshift_schema_advisor}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

-- Instructor-Advisor Mapping (BOA_INSTRUCTOR_ADVISOR_VW)
CREATE EXTERNAL TABLE {redshift_schema_advisor}.instructor_advisor
(
  ADVISOR_ID VARCHAR,
  CAMPUS_ID VARCHAR,
  INSTRUCTOR_ADVISOR_NBR INT,
  ADVISOR_TYPE VARCHAR,
  ADVISOR_TYPE_DESCR VARCHAR,
  INSTRUCTOR_TYPE VARCHAR,
  INSTRUCTOR_TYPE_DESCR VARCHAR,
  ACADEMIC_PROGRAM VARCHAR,
  ACADEMIC_PROGRAM_DESCR VARCHAR,
  ACADEMIC_PLAN VARCHAR,
  ACADEMIC_PLAN_DESCR VARCHAR,
  ACADEMIC_SUB_PLAN VARCHAR,
  ACADEMIC_SUB_PLAN_DESCR VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{advisor_data_path}/instructor-advisor-map';

-- Student-Advisor Mapping (BOA_STUDENT_ADVISOR_VW)
CREATE EXTERNAL TABLE {redshift_schema_advisor}.student_advisor
(
  STUDENT_ID VARCHAR,
  CAMPUS_ID VARCHAR,
  ADVISOR_ID VARCHAR,
  ADVISOR_ROLE VARCHAR,
  ADVISOR_ROLE_DESCR VARCHAR,
  ACADEMIC_PROGRAM VARCHAR,
  ACADEMIC_PROGRAM_DESCR VARCHAR,
  ACADEMIC_PLAN VARCHAR,
  ACADEMIC_PLAN_DESCR VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{advisor_data_path}/student-advisor-map';

-- Advisor note permissions (BOA_ADV_NOTES_ACCESS_VW)
CREATE EXTERNAL TABLE {redshift_schema_advisor}.advisor_note_permissions
(
  USER_ID VARCHAR,
  CS_ID VARCHAR,
  PERMISSION_LIST VARCHAR,
  DISPLAY_ONLY INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{advisor_data_path}/advisor-note-permissions';
