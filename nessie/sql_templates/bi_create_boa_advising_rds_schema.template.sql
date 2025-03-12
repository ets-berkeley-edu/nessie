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
-- BEGIN script for creating and populating RDS schema/tables for Advising Notes Dashboard
----------------------------------------------------------------------------------------------------


----------------------------------------------------------------------------------------------------
-- CREATE SCHEMA: "{bi_rds_schema_boa_advising}"
----------------------------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS {bi_rds_schema_boa_advising};
GRANT USAGE ON SCHEMA {bi_rds_schema_boa_advising} TO {bi_rds_tableau_user}, {bi_rds_boa_advising_role};
ALTER DEFAULT PRIVILEGES IN SCHEMA {bi_rds_schema_boa_advising}
  GRANT SELECT ON TABLES TO {bi_rds_tableau_user}, {bi_rds_boa_advising_role};


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: authors
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_boa_advising}.authors CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_boa_advising}.authors (
  author_uid VARCHAR(255) PRIMARY KEY,
  author_name_sort VARCHAR(255),
  author_name VARCHAR(255),
  last_name VARCHAR(255),
  first_name VARCHAR(255),
  author_aliases VARCHAR(65535)
);

INSERT INTO {bi_rds_schema_boa_advising}.authors (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      author_uid,
      last_name || COALESCE(', ' || first_name, ''),
      author_name,
      last_name,
      first_name,
      author_aliases
    FROM {bi_redshift_schema_boa_advising}.authors
  $REDSHIFT$)
  AS authors (
    author_uid VARCHAR(255),
    author_name_sort VARCHAR(255),
    author_name VARCHAR(255),
    last_name VARCHAR(255),
    first_name VARCHAR(255),
    author_aliases VARCHAR(65535)
  )
);

CREATE INDEX idx_bi_authors_author_name_sort ON {bi_rds_schema_boa_advising}.authors (author_name_sort);
CREATE INDEX idx_bi_authors_author_name ON {bi_rds_schema_boa_advising}.authors (author_name);
CREATE INDEX idx_bi_authors_author_aliases ON {bi_rds_schema_boa_advising}.authors (author_aliases);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: departments
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_boa_advising}.departments CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_boa_advising}.departments (
  dept_code VARCHAR(255) PRIMARY KEY,
  dept_name VARCHAR(255)
);

INSERT INTO {bi_rds_schema_boa_advising}.departments (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      dept_code,
      dept_name
    FROM {bi_redshift_schema_boa_advising}.departments
  $REDSHIFT$)
  AS departments (
    dept_code VARCHAR(255),
    dept_name VARCHAR(255)
  )
);

CREATE INDEX idx_bi_departments_dept_name ON {bi_rds_schema_boa_advising}.departments (dept_name);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: students
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_boa_advising}.students CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_boa_advising}.students
(
  sid VARCHAR(80) PRIMARY KEY,
  student_name_sort VARCHAR(513),
  student_name VARCHAR(513),
  last_name VARCHAR(255),
  first_name VARCHAR(255),
  cohort_list VARCHAR(65535),
  group_list VARCHAR(65535),
  is_manually_added BOOLEAN
);

INSERT INTO {bi_rds_schema_boa_advising}.students (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      sid,
      last_name || COALESCE(', ' || first_name, ''),
      student_name,
      last_name,
      first_name,
      cohort_list,
      group_list,
      is_manually_added
    FROM {bi_redshift_schema_boa_advising}.students
    WHERE sid IS NOT NULL
  $REDSHIFT$)
  AS students (
    sid VARCHAR(80),
    student_name_sort VARCHAR(513),
    student_name VARCHAR(513),
    last_name VARCHAR(255),
    first_name VARCHAR(255),
    cohort_list VARCHAR(65535),
    group_list VARCHAR(65535),
    is_manually_added BOOLEAN
  )
);

CREATE INDEX idx_bi_students_student_name_sort ON {bi_rds_schema_boa_advising}.students (student_name_sort);
CREATE INDEX idx_bi_students_student_name ON {bi_rds_schema_boa_advising}.students (student_name);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: notes
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_boa_advising}.notes CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_boa_advising}.notes (
  note_id INTEGER PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE,
  created_at_date_pst DATE,
  created_at_time_pst VARCHAR(12),
  set_date DATE,
  author_uid VARCHAR(255) REFERENCES {bi_rds_schema_boa_advising}.authors (author_uid),
  note_author_name VARCHAR(255),
  author_dept_code VARCHAR(255) REFERENCES {bi_rds_schema_boa_advising}.departments (dept_code),
  author_role VARCHAR(255),
  contact_type VARCHAR(40),
  is_private BOOLEAN,
  sid VARCHAR(80) REFERENCES {bi_rds_schema_boa_advising}.students (sid),
  subject VARCHAR(255)
);

INSERT INTO {bi_rds_schema_boa_advising}.notes (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      note_id,
      created_at,
      created_at_date_pst,
      created_at_time_pst,
      set_date,
      author_uid,
      note_author_name,
      author_dept_code,
      author_role,
      contact_type,
      is_private,
      sid,
      subject
    FROM {bi_redshift_schema_boa_advising}.notes
  $REDSHIFT$)
  AS notes (
    note_id INTEGER,
    created_at TIMESTAMP WITH TIME ZONE,
    created_at_date_pst DATE,
    created_at_time_pst VARCHAR(12),
    set_date DATE,
    author_uid VARCHAR(255),
    note_author_name VARCHAR(255),
    author_dept_code VARCHAR(255),
    author_role VARCHAR(255),
    contact_type VARCHAR(40),
    is_private BOOLEAN,
    sid VARCHAR(80),
    subject VARCHAR(255)
  )
);

CREATE INDEX idx_bi_notes_created_at ON {bi_rds_schema_boa_advising}.notes (created_at);
CREATE INDEX idx_bi_notes_set_date ON {bi_rds_schema_boa_advising}.notes (set_date);
CREATE INDEX idx_bi_notes_contact_type ON {bi_rds_schema_boa_advising}.notes (contact_type);
CREATE INDEX idx_bi_notes_is_private ON {bi_rds_schema_boa_advising}.notes (is_private);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: note_topics
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_boa_advising}.note_topics CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_boa_advising}.note_topics (
  note_id INTEGER,
  topic VARCHAR(50),
  PRIMARY KEY (note_id, topic)
);

INSERT INTO {bi_rds_schema_boa_advising}.note_topics (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT DISTINCT
      note_id,
      topic
    FROM {bi_redshift_schema_boa_advising}.note_topics
  $REDSHIFT$)
  AS note_topics (
    note_id INTEGER,
    topic VARCHAR(50)
  )
);

CREATE INDEX idx_bi_note_topics_note_id ON {bi_rds_schema_boa_advising}.note_topics (note_id);
CREATE INDEX idx_bi_note_topics_topic ON {bi_rds_schema_boa_advising}.note_topics (topic);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: student_groups
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_boa_advising}.student_groups CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_boa_advising}.student_groups (
  student_group_id INTEGER,
  student_group_name VARCHAR(255),
  sid VARCHAR(80),
  PRIMARY KEY (student_group_id, sid)
);

INSERT INTO {bi_rds_schema_boa_advising}.student_groups (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      student_group_id,
      student_group_name,
      sid
    FROM {bi_redshift_schema_boa_advising}.student_groups
  $REDSHIFT$)
  AS student_groups (
    student_group_id INTEGER,
    student_group_name VARCHAR(255),
    sid VARCHAR(80)
  )
);

CREATE INDEX idx_bi_student_groups_student_group_name ON {bi_rds_schema_boa_advising}.student_groups (student_group_name);
CREATE INDEX idx_bi_student_groups_sid ON {bi_rds_schema_boa_advising}.student_groups (sid);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: student_cohorts
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_boa_advising}.student_cohorts CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_boa_advising}.student_cohorts (
  cohort_id INTEGER,
  cohort_name VARCHAR(255),
  sid VARCHAR(255),
  PRIMARY KEY (cohort_id, sid)
);

INSERT INTO {bi_rds_schema_boa_advising}.student_cohorts (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      cohort_id,
      cohort_name,
      sid
    FROM {bi_redshift_schema_boa_advising}.student_cohorts
  $REDSHIFT$)
  AS student_cohorts (
    cohort_id INTEGER,
    cohort_name VARCHAR(255),
    sid VARCHAR(255)
  )
);

CREATE INDEX idx_bi_student_cohorts_cohort_name ON {bi_rds_schema_boa_advising}.student_cohorts (cohort_name);
CREATE INDEX idx_bi_student_cohorts_sid ON {bi_rds_schema_boa_advising}.student_cohorts (sid);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: bi_reports_boa_advising.student_degrees
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_boa_advising}.student_degrees CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_boa_advising}.student_degrees (
  sid VARCHAR(256),
  degree_date VARCHAR(65535),
  degree_awarded VARCHAR(65535),
  plan_type VARCHAR(65535),
  plan_group VARCHAR(65535),
  PRIMARY KEY (sid, degree_date, degree_awarded)
);

INSERT INTO {bi_rds_schema_boa_advising}.student_degrees (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT DISTINCT
      sid,
      degree_date,
      degree_awarded,
      plan_type,
      plan_group 
    FROM {bi_redshift_schema_boa_advising}.student_degrees
    WHERE sid IS NOT NULL
  $REDSHIFT$)
  AS student_degrees (
    sid VARCHAR(256),
    degree_date VARCHAR(65535),
    degree_awarded VARCHAR(65535),
    plan_type VARCHAR(65535),
    plan_group VARCHAR(65535)
  )   
);

CREATE INDEX idx_bi_student_degrees_sid ON {bi_rds_schema_boa_advising}.student_degrees (sid);
CREATE INDEX idx_bi_student_degrees_degree_date ON {bi_rds_schema_boa_advising}.student_degrees (degree_date);
CREATE INDEX idx_bi_student_degrees_degree_awarded ON {bi_rds_schema_boa_advising}.student_degrees (degree_awarded);


----------------------------------------------------------------------------------------------------
-- END script for creating and populating RDS schema/tables for Advising Notes Dashboard
----------------------------------------------------------------------------------------------------
