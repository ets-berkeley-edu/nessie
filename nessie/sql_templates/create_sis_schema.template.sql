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

CREATE EXTERNAL SCHEMA {redshift_schema_sis}
FROM data catalog
DATABASE '{redshift_schema_sis}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

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
    allowed_units DOUBLE PRECISION
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

-- Advisee demographics
CREATE EXTERNAL TABLE {redshift_schema_sis}.demographics
(
    sid VARCHAR,
    gender_of_record VARCHAR,
    gender_identity VARCHAR,
    usa_visa_type_code VARCHAR,
    ethnicity_group_descr VARCHAR,
    ethnicity_detail_descr VARCHAR,
    foreigncountry_descr VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_data_path}/advisees/demographics/';

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

--------------------------------------------------------------------
-- Internal tables
--------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS {redshift_schema_sis_internal}.sis_terms
(
    term_id VARCHAR(4) NOT NULL,
    term_name VARCHAR NOT NULL,
    academic_career VARCHAR NOT NULL,
    term_begins DATE NOT NULL,
    term_ends DATE NOT NULL,
    session_id VARCHAR NOT NULL,
    session_name VARCHAR NOT NULL,
    session_begins DATE NOT NULL,
    session_ends DATE NOT NULL
)
SORTKEY(term_id, academic_career);

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
