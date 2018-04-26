/**
 * Copyright ©2018. The Regents of the University of California (Regents). All Rights Reserved.
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
-- DROP and CREATE EXTERNAL SCHEMA
--------------------------------------------------------------------

-- Contrary to the documentation, this statement does not actually drop external database tables.
-- When the external schema is re-created, the table definitions will return as they were.
DROP SCHEMA IF EXISTS {redshift_schema_sis} CASCADE;
CREATE EXTERNAL SCHEMA {redshift_schema_sis}
FROM data catalog
DATABASE '{redshift_schema_sis}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

-- courses
DROP TABLE IF EXISTS {redshift_schema_sis}.courses CASCADE;
CREATE EXTERNAL TABLE {redshift_schema_sis}.courses(
    section_id INT,
    term_id INT,
    print_in_schedule_of_classes VARCHAR,
    is_primary VARCHAR,
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
    meeting_end_date VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_data_path}/manifests/courses.json';

-- enrollments
DROP TABLE IF EXISTS {redshift_schema_sis}.enrollments CASCADE;
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
    grading_basis VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_data_path}/manifests/enrollments.json';
