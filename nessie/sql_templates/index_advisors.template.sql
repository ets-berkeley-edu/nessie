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

CREATE SCHEMA IF NOT EXISTS {rds_schema_advisor};
GRANT USAGE ON SCHEMA {rds_schema_advisor} TO {rds_app_boa_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_advisor} GRANT SELECT ON TABLES TO {rds_app_boa_user};

BEGIN TRANSACTION;

DROP TABLE IF EXISTS {rds_schema_advisor}.advisor_attributes CASCADE;

CREATE TABLE {rds_schema_advisor}.advisor_attributes (
    sid VARCHAR,
    uid VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    title VARCHAR,
    dept_code VARCHAR,
    email VARCHAR,
    campus_email VARCHAR
);

INSERT INTO {rds_schema_advisor}.advisor_attributes (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT csid, ldap_uid, first_name, last_name, NULLIF(title, ''), NULLIF(dept_code, ''), NULLIF(email, ''), campus_email
    FROM {redshift_schema_advisor_internal}.advisor_attributes
    ORDER BY ldap_uid
  $REDSHIFT$)
  AS redshift_advisor_attributes (
    sid VARCHAR,
    uid VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    title VARCHAR,
    dept_code VARCHAR,
    email VARCHAR,
    campus_email VARCHAR
  )
);

CREATE INDEX idx_advisor_attributes_uid ON {rds_schema_advisor}.advisor_attributes(uid);

DROP TABLE IF EXISTS {rds_schema_advisor}.advisor_departments CASCADE;

CREATE TABLE {rds_schema_advisor}.advisor_departments (
   sid VARCHAR,
   advisor_type_code VARCHAR,
   advisor_type VARCHAR,
   plan_code VARCHAR,
   plan VARCHAR,
   department_code VARCHAR,
   department VARCHAR
);

INSERT INTO {rds_schema_advisor}.advisor_departments (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT sid, advisor_type_code, advisor_type, plan_code, plan,
           department_code, department
    FROM {redshift_schema_advisor_internal}.advisor_departments
    ORDER BY sid, plan_code
  $REDSHIFT$)
  AS redshift_advisor_departments (
    sid VARCHAR,
    advisor_type_code VARCHAR,
    advisor_type VARCHAR,
    plan_code VARCHAR,
    plan VARCHAR,
    department_code VARCHAR,
    department VARCHAR
  )
);

CREATE INDEX idx_advisor_departments_sid ON {rds_schema_advisor}.advisor_departments(sid);
CREATE INDEX idx_advisor_departments_plan_code ON {rds_schema_advisor}.advisor_departments(plan_code);
CREATE INDEX idx_advisor_departments_department_code ON {rds_schema_advisor}.advisor_departments(department_code);

DROP TABLE IF EXISTS {rds_schema_advisor}.advisor_roles CASCADE;

CREATE TABLE {rds_schema_advisor}.advisor_roles (
   sid VARCHAR,
   uid VARCHAR,
   advisor_type_code VARCHAR,
   advisor_type VARCHAR,
   instructor_type_code VARCHAR,
   instructor_type VARCHAR,
   academic_program_code VARCHAR,
   academic_program VARCHAR,
   cs_permissions VARCHAR
);

INSERT INTO {rds_schema_advisor}.advisor_roles (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT sid, uid, advisor_type_code, advisor_type, instructor_type_code, instructor_type,
           academic_program_code, academic_program, cs_permissions
    FROM {redshift_schema_advisor_internal}.advisor_roles
    ORDER BY uid, academic_program_code, advisor_type_code
  $REDSHIFT$)
  AS redshift_advisor_roles (
    sid VARCHAR,
    uid VARCHAR,
    advisor_type_code VARCHAR,
    advisor_type VARCHAR,
    instructor_type_code VARCHAR,
    instructor_type VARCHAR,
    academic_program_code VARCHAR,
    academic_program VARCHAR,
    cs_permissions VARCHAR
  )
);

CREATE INDEX idx_advisor_roles_uid ON {rds_schema_advisor}.advisor_roles(uid);
CREATE INDEX idx_advisor_roles_advisor_type_code ON {rds_schema_advisor}.advisor_roles(advisor_type_code);
CREATE INDEX idx_advisor_roles_academic_program_code ON {rds_schema_advisor}.advisor_roles(academic_program_code);

DROP TABLE IF EXISTS {rds_schema_advisor}.advisor_students CASCADE;

CREATE TABLE {rds_schema_advisor}.advisor_students (
   advisor_sid VARCHAR,
   student_sid VARCHAR,
   student_uid VARCHAR,
   advisor_type_code VARCHAR,
   advisor_type VARCHAR,
   academic_program_code VARCHAR,
   academic_program VARCHAR,
   academic_plan_code VARCHAR,
   academic_plan VARCHAR
);

INSERT INTO {rds_schema_advisor}.advisor_students (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT advisor_sid, student_sid, student_uid, advisor_type_code, advisor_type, academic_program_code,
           academic_program, academic_plan_code, academic_plan
    FROM {redshift_schema_advisor_internal}.advisor_students
    ORDER BY advisor_sid, student_sid DESC
  $REDSHIFT$)
  AS redshift_advisor_students (
   advisor_sid VARCHAR,
   student_sid VARCHAR,
   student_uid VARCHAR,
   advisor_type_code VARCHAR,
   advisor_type VARCHAR,
   academic_program_code VARCHAR,
   academic_program VARCHAR,
   academic_plan_code VARCHAR,
   academic_plan VARCHAR
  )
);

CREATE INDEX idx_advisor_students_advisor_sid ON {rds_schema_advisor}.advisor_students(advisor_sid);
CREATE INDEX idx_advisor_students_student_sid ON {rds_schema_advisor}.advisor_students(student_sid);
CREATE INDEX idx_advisor_students_advisor_type_code ON {rds_schema_advisor}.advisor_students(advisor_type_code);
CREATE INDEX idx_advisor_students_academic_program_code ON {rds_schema_advisor}.advisor_students(academic_program_code);
CREATE INDEX idx_advisor_students_academic_plan_code ON {rds_schema_advisor}.advisor_students(academic_plan_code);

COMMIT TRANSACTION;
