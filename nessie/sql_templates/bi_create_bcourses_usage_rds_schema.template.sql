/**
 * Copyright ©2025. The Regents of the University of California (Regents). All Rights Reserved.
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


----------------------------------------------------------------------------------------------------
-- BEGIN script for creating and populating RDS schema/tables for bCourses Usage Dashboard
----------------------------------------------------------------------------------------------------


----------------------------------------------------------------------------------------------------
-- CREATE SCHEMA: "{bi_rds_schema_bcourses_usage}"
----------------------------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS {bi_rds_schema_bcourses_usage};
GRANT USAGE ON SCHEMA {bi_rds_schema_bcourses_usage} TO {bi_rds_tableau_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {bi_rds_schema_bcourses_usage}
  GRANT SELECT ON TABLES TO {bi_rds_tableau_user};

----------------------------------------------------------------------------------------------------
-- CREATE TABLE: course_activity
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_bcourses_usage}.course_activity CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_bcourses_usage}.course_activity (
    id	numeric,
    sis_source_id	varchar,
    term_id	varchar,
    subject_cd	varchar,
    college_school_nm	varchar,
    anchor_date	date,
    total_unique_students	numeric,
    last_active_30_days_after_course_end	numeric,
    last_active_60_days_after_course_end	numeric,
    last_active_90_days_after_course_end	numeric,
    PRIMARY KEY (id)
);

INSERT INTO {bi_rds_schema_bcourses_usage}.course_activity (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
        id,
        sis_source_id,
        term_id,
        subject_cd,
        college_school_nm,
        anchor_date,
        total_unique_students,
        last_active_30_days_after_course_end,
        last_active_60_days_after_course_end,
        last_active_90_days_after_course_end
    FROM {bi_redshift_schema_bcourses_usage}.course_activity
  $REDSHIFT$)
  AS enrollments (
    id	numeric,
    sis_source_id	varchar,
    term_id	varchar,
    subject_cd	varchar,
    college_school_nm	varchar,
    anchor_date	date,
    total_unique_students	numeric,
    last_active_30_days_after_course_end	numeric,
    last_active_60_days_after_course_end	numeric,
    last_active_90_days_after_course_end	numeric
  )
);

CREATE INDEX idx_term_id ON {bi_rds_schema_bcourses_usage}.course_activity (id);

----------------------------------------------------------------------------------------------------
-- END script for creating and populating RDS schema/tables for bCourses Usage Data Dashboard
----------------------------------------------------------------------------------------------------
