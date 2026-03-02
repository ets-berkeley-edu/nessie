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
-- CREATE TABLE: student_course_activity_post_completion
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_bcourses_service_cd2}.student_course_activity_post_completion CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_bcourses_service_cd2}.student_course_activity_post_completion (
    id	numeric,
    account_id	numeric,
    enrollment_term_id	numeric,
    anchor_date	date,
    total_unique_students	numeric,
    last_active_30_days_after_course_end	numeric,
    last_active_60_days_after_course_end	numeric,
    last_active_90_days_after_course_end	numeric,
    PRIMARY KEY (id)
);

INSERT INTO {bi_rds_schema_bcourses_service_cd2}.student_course_activity_post_completion (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
        id,
        account_id,
        enrollment_term_id,
        anchor_date,
        total_unique_students,
        last_active_30_days_after_course_end,
        last_active_60_days_after_course_end,
        last_active_90_days_after_course_end
    FROM {bi_redshift_schema_bcourses_service_cd2}.student_course_activity_post_completion
  $REDSHIFT$)
  AS enrollments (
    id	numeric,
    account_id	numeric,
    enrollment_term_id	numeric,
    anchor_date	date,
    total_unique_students	numeric,
    last_active_30_days_after_course_end	numeric,
    last_active_60_days_after_course_end	numeric,
    last_active_90_days_after_course_end	numeric
  )
);

CREATE INDEX idx_term_id ON {bi_rds_schema_bcourses_service_cd2}.student_course_activity_post_completion (id);

----------------------------------------------------------------------------------------------------
-- END script for creating and populating RDS schema/tables for bCourses Usage Data Dashboard
----------------------------------------------------------------------------------------------------
