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
-- BEGIN script for creating and populating RDS schema/tables for bCourses Service (CD2) Dashboard
----------------------------------------------------------------------------------------------------


----------------------------------------------------------------------------------------------------
-- CREATE SCHEMA: "{bi_rds_schema_bcourses_service_cd2}"
----------------------------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS {bi_rds_schema_bcourses_service_cd2};
GRANT USAGE ON SCHEMA {bi_rds_schema_bcourses_service_cd2} TO {bi_rds_tableau_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {bi_rds_schema_bcourses_service_cd2}
  GRANT SELECT ON TABLES TO {bi_rds_tableau_user};


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: bcourses_accounts
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_bcourses_service_cd2}.bcourses_accounts CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_bcourses_service_cd2}.bcourses_accounts (
  account_id BIGINT PRIMARY KEY,
  name VARCHAR(255),
  sis_source_id VARCHAR(255),
  subject_cd VARCHAR(20),
  subject_nm VARCHAR(255),
  dept_cd VARCHAR(20),
  dept_nm VARCHAR(255),
  division_cd VARCHAR(20),
  division_nm VARCHAR(255),
  college_school_cd VARCHAR(20),
  college_school_nm VARCHAR(255),
  workflow_state VARCHAR(255)
);

INSERT INTO {bi_rds_schema_bcourses_service_cd2}.bcourses_accounts (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      account_id,
      name,
      sis_source_id,
      subject_cd,
      subject_nm,
      dept_cd,
      dept_nm,
      division_cd,
      division_nm,
      college_school_cd,
      college_school_nm,
      workflow_state
    FROM {bi_redshift_schema_bcourses_service_cd2}.bcourses_accounts
  $REDSHIFT$)
  AS bcourses_accounts (
    account_id BIGINT,
    name VARCHAR(255),
    sis_source_id VARCHAR(255),
    subject_cd VARCHAR(20),
    subject_nm VARCHAR(255),
    dept_cd VARCHAR(20),
    dept_nm VARCHAR(255),
    division_cd VARCHAR(20),
    division_nm VARCHAR(255),
    college_school_cd VARCHAR(20),
    college_school_nm VARCHAR(255),
    workflow_state VARCHAR(255)
  )
);

CREATE INDEX idx_bi_bcs_accounts_name ON {bi_rds_schema_bcourses_service_cd2}.bcourses_accounts (name);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: canvas_courses
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_bcourses_service_cd2}.canvas_courses CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_bcourses_service_cd2}.canvas_courses (
  course_id BIGINT PRIMARY KEY,
  account_id BIGINT,
  enrollment_term_id BIGINT,
  name VARCHAR(255),
  code VARCHAR(255),
  created_at DATE,
  start_date DATE,
  end_date DATE,
  sis_source_id VARCHAR(255),
  publicly_visible BOOLEAN,
  workflow_state VARCHAR(255),
  wiki_id BIGINT,
  syllabus VARCHAR(3),
  quizzes VARCHAR(3),
  announcements VARCHAR(3),
  discussion_topics VARCHAR(3),
  files VARCHAR(3),
  assignments VARCHAR(3),
  modules VARCHAR(3),
  pages VARCHAR(3),
  gradebook VARCHAR(3)
);

INSERT INTO {bi_rds_schema_bcourses_service_cd2}.canvas_courses (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      course_id,
      account_id,
      enrollment_term_id,
      name,
      code,
      created_at,
      start_date,
      end_date,
      sis_source_id,
      publicly_visible,
      workflow_state,
      wiki_id,
      syllabus,
      quizzes,
      announcements,
      discussion_topics,
      files,
      assignments,
      modules,
      pages,
      gradebook
    FROM {bi_redshift_schema_bcourses_service_cd2}.canvas_courses
  $REDSHIFT$)
  AS canvas_courses (
    course_id BIGINT,
    account_id BIGINT,
    enrollment_term_id BIGINT,
    name VARCHAR(255),
    code VARCHAR(255),
    created_at DATE,
    start_date DATE,
    end_date DATE,
    sis_source_id VARCHAR(255),
    publicly_visible BOOLEAN,
    workflow_state VARCHAR(255),
    wiki_id BIGINT,
    syllabus VARCHAR(3),
    quizzes VARCHAR(3),
    announcements VARCHAR(3),
    discussion_topics VARCHAR(3),
    files VARCHAR(3),
    assignments VARCHAR(3),
    modules VARCHAR(3),
    pages VARCHAR(3),
    gradebook VARCHAR(3)
  )
);

CREATE INDEX idx_bi_bcs_courses_name ON {bi_rds_schema_bcourses_service_cd2}.canvas_courses (name);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: bcourses_assignments
-- removed PRIMARY KEY on assignment_id
-- duplicate key (assignment_id)=(4355026) possibly due to multiple workflow states?
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_bcourses_service_cd2}.bcourses_assignments CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_bcourses_service_cd2}.bcourses_assignments
(
  assignment_id BIGINT,
  course_id BIGINT,
  enrollment_term_id BIGINT,
  title VARCHAR(255),
  created_at TIMESTAMP WITHOUT TIME ZONE,
  corrected_created_at TIMESTAMP WITHOUT TIME ZONE,
  updated_at TIMESTAMP WITHOUT TIME ZONE,
  due_at TIMESTAMP WITHOUT TIME ZONE,
  points_possible DOUBLE PRECISION,
  grading_type VARCHAR(255),
  submission_types VARCHAR(256),
  workflow_state VARCHAR(255),
  visibility VARCHAR(25),
  external_tool_id BIGINT,
  quizzes VARCHAR(4),
  discussion_topics VARCHAR(17),
  non_quiz_assignments VARCHAR(8)
);

INSERT INTO {bi_rds_schema_bcourses_service_cd2}.bcourses_assignments (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      assignment_id,
      course_id,
      enrollment_term_id,
      title,
      created_at,
      corrected_created_at,
      updated_at,
      due_at,
      points_possible,
      grading_type,
      submission_types,
      workflow_state,
      visibility,
      external_tool_id,
      quizzes,
      discussion_topics,
      non_quiz_assignments
    FROM {bi_redshift_schema_bcourses_service_cd2}.bcourses_assignments
  $REDSHIFT$)
  AS bcourses_assignments (
    assignment_id BIGINT,
    course_id BIGINT,
    enrollment_term_id BIGINT,
    title VARCHAR(255),
    created_at TIMESTAMP WITHOUT TIME ZONE,
    corrected_created_at TIMESTAMP WITHOUT TIME ZONE,
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    due_at TIMESTAMP WITHOUT TIME ZONE,
    points_possible DOUBLE PRECISION,
    grading_type VARCHAR(255),
    submission_types VARCHAR(256),
    workflow_state VARCHAR(255),
    visibility VARCHAR(25),
    external_tool_id BIGINT,
    quizzes VARCHAR(4),
    discussion_topics VARCHAR(17),
    non_quiz_assignments VARCHAR(8)
  )
);

CREATE INDEX idx_bi_bcs_assignments_course_id ON {bi_rds_schema_bcourses_service_cd2}.bcourses_assignments (course_id);
CREATE INDEX idx_bi_bcs_assignments_enrollment_term_id ON {bi_rds_schema_bcourses_service_cd2}.bcourses_assignments (enrollment_term_id);
CREATE INDEX idx_bi_bcs_assignments_title ON {bi_rds_schema_bcourses_service_cd2}.bcourses_assignments (title);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: bcourses_enrollment_terms
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_bcourses_service_cd2}.bcourses_enrollment_terms CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_bcourses_service_cd2}.bcourses_enrollment_terms (
  enrollment_term_id BIGINT PRIMARY KEY,
  term_name VARCHAR(255),
  year VARCHAR(255),
  term VARCHAR(255),
  academic_year VARCHAR(264),
  sis_source_id VARCHAR(255),
  date_start TIMESTAMP WITHOUT TIME ZONE,
  date_start_range_padded DATE,
  date_end_range_padded DATE
);

INSERT INTO {bi_rds_schema_bcourses_service_cd2}.bcourses_enrollment_terms (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      enrollment_term_id,
      term_name,
      year,
      term,
      academic_year,
      sis_source_id,
      date_start,
      date_start_range_padded,
      date_end_range_padded
    FROM {bi_redshift_schema_bcourses_service_cd2}.bcourses_enrollment_terms
  $REDSHIFT$)
  AS bcourses_enrollment_terms (
   enrollment_term_id BIGINT,
    term_name VARCHAR(255),
    year VARCHAR(255),
    term VARCHAR(255),
    academic_year VARCHAR(264),
    sis_source_id VARCHAR(255),
    date_start TIMESTAMP WITHOUT TIME ZONE,
    date_start_range_padded DATE,
    date_end_range_padded DATE
  )
);

CREATE INDEX idx_bi_bcs_enrollment_terms_term_name ON {bi_rds_schema_bcourses_service_cd2}.bcourses_enrollment_terms (term_name);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: discussion_posts_daily_agg
-- REMOVED PRIMARY KEY (course_id, enrollment_term_id, corrected_created_at)
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_bcourses_service_cd2}.discussion_posts_daily_agg CASCADE;
 
CREATE TABLE IF NOT EXISTS {bi_rds_schema_bcourses_service_cd2}.discussion_posts_daily_agg (
  term_year VARCHAR(255),
  term VARCHAR(255),
  term_name VARCHAR(511),
  enrollment_term_id BIGINT,
  department VARCHAR(255),
  course_id BIGINT,
  corrected_created_at DATE,
  discussion_entry_count BIGINT
); 
 
INSERT INTO {bi_rds_schema_bcourses_service_cd2}.discussion_posts_daily_agg (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT 
      term_year,
      term,
      term_name,
      enrollment_term_id,
      department,
      course_id,
      corrected_created_at,
      discussion_entry_count
    FROM {bi_redshift_schema_bcourses_service_cd2}.discussion_posts_daily_agg
  $REDSHIFT$)
  AS discussion_posts_daily_agg (
    term_year VARCHAR(255),
    term VARCHAR(255),
    term_name VARCHAR(511),
    enrollment_term_id BIGINT,
    department VARCHAR(255),
    course_id BIGINT,
    corrected_created_at DATE,
    discussion_entry_count BIGINT
  )
); 
 

----------------------------------------------------------------------------------------------------
-- CREATE TABLE: discussion_topics_daily_agg
-- REMOVED PRIMARY KEY (course_id, enrollment_term_id, announcements, discussion_topic, corrected_created_at)
-- Announcements has null values so PK is invalid. Add index?
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_bcourses_service_cd2}.discussion_topics_daily_agg CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_bcourses_service_cd2}.discussion_topics_daily_agg (
  term_year VARCHAR(255),
  term VARCHAR(255),
  term_name VARCHAR(511),
  enrollment_term_id BIGINT,
  department VARCHAR(255),
  course_id BIGINT,
  announcements VARCHAR(255),
  discussion_topic VARCHAR(16),
  corrected_created_at DATE,
  discussion_topic_count BIGINT
);

INSERT INTO {bi_rds_schema_bcourses_service_cd2}.discussion_topics_daily_agg (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      term_year,
      term,
      term_name,
      enrollment_term_id,
      department,
      course_id,
      announcements,
      discussion_topic,
      corrected_created_at,
      discussion_topic_count
    FROM {bi_redshift_schema_bcourses_service_cd2}.discussion_topics_daily_agg
  $REDSHIFT$)
  AS discussion_topics_daily_agg (
    term_year VARCHAR(255),
    term VARCHAR(255),
    term_name VARCHAR(511),
    enrollment_term_id BIGINT,
    department VARCHAR(255),
    course_id BIGINT,
    announcements VARCHAR(255),
    discussion_topic VARCHAR(16),
    corrected_created_at DATE,
    discussion_topic_count BIGINT
  )
); 


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: enrollment_agg_by_role
-- REMOVED PRIMARY KEY (course_id, enrollment_term_id, most_privileged_role)
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_bcourses_service_cd2}.enrollment_agg_by_role CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_bcourses_service_cd2}.enrollment_agg_by_role (
  term_year VARCHAR(255), 
  term VARCHAR(255),
  term_name VARCHAR(511),
  enrollment_term_id BIGINT,
  course_id BIGINT,
  most_privileged_role VARCHAR(255),
  enrollment_count BIGINT
);

INSERT INTO {bi_rds_schema_bcourses_service_cd2}.enrollment_agg_by_role (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      term_year,
      term,
      term_name,
      enrollment_term_id,
      course_id,
      most_privileged_role,
      enrollment_count
    FROM {bi_redshift_schema_bcourses_service_cd2}.enrollment_agg_by_role
  $REDSHIFT$)
  AS enrollment_agg_by_role (
    term_year VARCHAR(255),
    term VARCHAR(255),
    term_name VARCHAR(511),
    enrollment_term_id BIGINT,
    course_id BIGINT,
    most_privileged_role VARCHAR(255),
    enrollment_count BIGINT
  )
); 


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: file_upload_daily_agg
-- REMOVED PRIMARY KEY (course_id, enrollment_term_id, owner_entity_type, corrected_created_at)
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_bcourses_service_cd2}.file_upload_daily_agg CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_bcourses_service_cd2}.file_upload_daily_agg (
  term_year VARCHAR(255),
  term VARCHAR(255),
  term_name VARCHAR(511),
  enrollment_term_id BIGINT,
  department VARCHAR(255),
  owner_entity_type VARCHAR(255),
  course_id BIGINT,
  corrected_created_at DATE,
  file_upload_count BIGINT
);

INSERT INTO {bi_rds_schema_bcourses_service_cd2}.file_upload_daily_agg (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      term_year,
      term,
      term_name,
      enrollment_term_id,
      department,
      owner_entity_type,
      course_id,
      corrected_created_at,
      file_upload_count
    FROM {bi_redshift_schema_bcourses_service_cd2}.file_upload_daily_agg
  $REDSHIFT$)
  AS file_upload_daily_agg (
    term_year VARCHAR(255),
    term VARCHAR(255),
    term_name VARCHAR(511),
    enrollment_term_id BIGINT,
    department VARCHAR(255),
    owner_entity_type VARCHAR(255),
    course_id BIGINT,
    corrected_created_at DATE,
    file_upload_count BIGINT
  )
); 


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: submission_daily_agg
-- REMOVED PRIMARY KEY (course_id, enrollment_term_id, submission_type, workflow_state, corrected_created_at)
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_bcourses_service_cd2}.submission_daily_agg CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_bcourses_service_cd2}.submission_daily_agg (
  term_year VARCHAR(255),
  term VARCHAR(255),
  term_name VARCHAR(511),
  enrollment_term_id BIGINT,
  department VARCHAR(255),
  submission_type VARCHAR(256),
  workflow_state VARCHAR(255),
  course_id BIGINT,
  corrected_created_at DATE,
  submission_count BIGINT
);

INSERT INTO {bi_rds_schema_bcourses_service_cd2}.submission_daily_agg (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      term_year,
      term,
      term_name,
      enrollment_term_id,
      department,
      submission_type,
      workflow_state,
      course_id,
      corrected_created_at,
      submission_count
    FROM {bi_redshift_schema_bcourses_service_cd2}.submission_daily_agg
  $REDSHIFT$)
  AS submission_daily_agg (
    term_year VARCHAR(255),
    term VARCHAR(255),
    term_name VARCHAR(511),
    enrollment_term_id BIGINT,
    department VARCHAR(255),
    submission_type VARCHAR(256),
    workflow_state VARCHAR(255),
    course_id BIGINT,
    corrected_created_at DATE,
    submission_count BIGINT
  )
); 


----------------------------------------------------------------------------------------------------
-- END script for creating and populating RDS schema/tables for bCourses Service (CD2) Dashboard
----------------------------------------------------------------------------------------------------
