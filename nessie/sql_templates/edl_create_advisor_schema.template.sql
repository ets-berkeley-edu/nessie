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

CREATE EXTERNAL SCHEMA {redshift_schema_advisor}
FROM data catalog
DATABASE '{redshift_schema_advisor}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

-- Advisor note permissions (BOA_ADV_NOTES_ACCESS_VW)
CREATE EXTERNAL TABLE {redshift_schema_advisor}.advisor_note_permissions
(
  USER_ID VARCHAR,
  CS_ID VARCHAR,
  PERMISSION_LIST VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{advisor_data_path}/advisor-note-permissions';

--------------------------------------------------------------------
-- Internal Schema
--------------------------------------------------------------------

DROP SCHEMA IF EXISTS {redshift_schema_advisor_internal} CASCADE;
CREATE SCHEMA {redshift_schema_advisor_internal};
GRANT USAGE ON SCHEMA {redshift_schema_advisor_internal} TO GROUP {redshift_app_boa_user}_group;
ALTER default PRIVILEGES IN SCHEMA {redshift_schema_advisor_internal} GRANT SELECT ON TABLES TO GROUP {redshift_app_boa_user}_group;
GRANT USAGE ON SCHEMA {redshift_schema_advisor_internal} TO GROUP {redshift_dblink_group};
ALTER DEFAULT PRIVILEGES IN SCHEMA {redshift_schema_advisor_internal} GRANT SELECT ON TABLES TO GROUP {redshift_dblink_group};

--------------------------------------------------------------------
-- Internal Tables
--------------------------------------------------------------------

CREATE TABLE {redshift_schema_advisor_internal}.advisor_departments
SORTKEY (sid)
AS (
    SELECT
        advisor_id AS sid,
        advisor_role AS advisor_type_code,
        advisor_role_desc AS advisor_type,
        academic_plan_cd AS plan_code,
        academic_plan_nm AS plan,
        academic_department_cd AS department_code,
        academic_department_short_nm AS department
    FROM {redshift_schema_edl_external}.student_advisor_data
    WHERE
        academic_career_cd='UGRD'
        AND advisor_id ~ '[0-9]+'
        AND academic_department_cd IS NOT NULL
        AND academic_plan_cd IS NOT NULL
    ORDER BY advisor_id, academic_plan_cd
);

CREATE TABLE {redshift_schema_advisor_internal}.advisor_roles
SORTKEY (sid)
AS (
    SELECT DISTINCT
        p.CS_ID AS sid,
        p.USER_ID AS uid,
        a.advisor_role AS advisor_type_code,
        a.advisor_role_desc AS advisor_type,
        NULL AS instructor_type_code,
        NULL AS instructor_type,
        a.academic_program_cd AS academic_program_code,
        a.academic_program_nm AS academic_program,
        p.PERMISSION_LIST AS cs_permissions
    FROM {redshift_schema_advisor}.advisor_note_permissions p
    LEFT JOIN {redshift_schema_edl_external}.student_advisor_data a
    ON p.CS_ID = a.advisor_id
);

CREATE TABLE {redshift_schema_advisor_internal}.advisor_students
SORTKEY (advisor_sid, student_sid, student_uid)
AS (
    SELECT
        a.advisor_id AS advisor_sid,
        a.student_id AS student_sid,
        NULL AS student_uid,
        a.advisor_role AS advisor_type_code,
        a.advisor_role_desc AS advisor_type,
        a.academic_program_cd AS academic_program_code,
        a.academic_program_nm AS academic_program,
        a.academic_plan_cd AS academic_plan_code,
        a.academic_plan_nm AS academic_plan
    FROM {redshift_schema_edl_external}.student_advisor_data a
);

CREATE TABLE {redshift_schema_advisor_internal}.advisor_attributes(
    ldap_uid VARCHAR,
    csid VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    title VARCHAR,
    dept_code VARCHAR,
    email VARCHAR,
    campus_email VARCHAR
)
SORTKEY (ldap_uid);