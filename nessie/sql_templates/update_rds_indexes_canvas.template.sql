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

CREATE SCHEMA IF NOT EXISTS {rds_schema_canvas};
GRANT USAGE ON SCHEMA {rds_schema_canvas} TO {rds_app_ripley_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_canvas} GRANT SELECT ON TABLES TO {rds_app_ripley_user};

DROP TABLE IF EXISTS {rds_schema_canvas}.course_sites CASCADE;

CREATE TABLE {rds_schema_canvas}.course_sites
(
    id VARCHAR NOT NULL PRIMARY KEY,
    account_id VARCHAR,
    sis_term_id VARCHAR,
    sis_course_id VARCHAR,
    course_code VARCHAR,
    name VARCHAR,
    workflow_state VARCHAR,
    last_activity TIMESTAMP,
    created_at TIMESTAMP
);

INSERT INTO {rds_schema_canvas}.course_sites
(id, account_id, sis_term_id, sis_course_id, course_code, name, workflow_state, last_activity, created_at) 
(SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT DISTINCT
      canvas_courses.course_id,
      canvas_courses.account_id,
      bcourses_enrollment_terms.sis_source_id,
      canvas_courses.sis_source_id,
      canvas_courses.code,
      canvas_courses.name,
      canvas_courses.workflow_state,
      canvas_courses.max_last_activity_at,
      canvas_courses.created_at
    FROM {bi_redshift_schema_bcourses_service_cd2}.canvas_courses
    LEFT JOIN {bi_redshift_schema_bcourses_service_cd2}.bcourses_enrollment_terms
      ON canvas_courses.enrollment_term_id = bcourses_enrollment_terms.enrollment_term_id
  $REDSHIFT$)
  AS redshift_course_sites (
    id VARCHAR,
    account_id VARCHAR,
    sis_term_id VARCHAR,
    sis_course_id VARCHAR,
    course_code VARCHAR,
    name VARCHAR,
    workflow_state VARCHAR,
    max_last_activity_at TIMESTAMP,
    created_at TIMESTAMP
  )
);
