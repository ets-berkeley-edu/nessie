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

CREATE SCHEMA IF NOT EXISTS {rds_schema_advisor};
GRANT USAGE ON SCHEMA {rds_schema_advisor} TO {rds_app_boa_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_advisor} GRANT SELECT ON TABLES TO {rds_app_boa_user};

BEGIN TRANSACTION;

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
