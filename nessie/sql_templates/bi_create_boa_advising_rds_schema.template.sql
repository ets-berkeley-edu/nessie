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
  author_uid INTEGER PRIMARY KEY,
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
    author_uid INTEGER,
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
  sid BIGINT PRIMARY KEY,
  student_name_sort VARCHAR(513),
  student_name VARCHAR(513),
  last_name VARCHAR(255),
  first_name VARCHAR(255),
  is_manually_added BOOLEAN,
  cohort_list VARCHAR(65535),
  group_list VARCHAR(65535),
  degree_list VARCHAR(65535)
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
      is_manually_added,
      cohort_list,
      group_list,
      degree_list
    FROM {bi_redshift_schema_boa_advising}.students
    WHERE sid IS NOT NULL
  $REDSHIFT$)
  AS students (
    sid BIGINT,
    student_name_sort VARCHAR(513),
    student_name VARCHAR(513),
    last_name VARCHAR(255),
    first_name VARCHAR(255),
    is_manually_added BOOLEAN,
    cohort_list VARCHAR(65535),
    group_list VARCHAR(65535),
    degree_list VARCHAR(65535)
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
  author_uid INTEGER REFERENCES {bi_rds_schema_boa_advising}.authors (author_uid),
  note_author_name VARCHAR(255),
  author_dept_code VARCHAR(255) REFERENCES {bi_rds_schema_boa_advising}.departments (dept_code),
  author_role VARCHAR(255),
  contact_type VARCHAR(40),
  is_private BOOLEAN,
  sid BIGINT REFERENCES {bi_rds_schema_boa_advising}.students (sid),
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
    author_uid INTEGER,
    note_author_name VARCHAR(255),
    author_dept_code VARCHAR(255),
    author_role VARCHAR(255),
    contact_type VARCHAR(40),
    is_private BOOLEAN,
    sid BIGINT,
    subject VARCHAR(255)
  )
);

CREATE INDEX idx_bi_notes_created_at ON {bi_rds_schema_boa_advising}.notes (created_at);
CREATE INDEX idx_bi_notes_set_date ON {bi_rds_schema_boa_advising}.notes (set_date);
CREATE INDEX idx_bi_notes_contact_type ON {bi_rds_schema_boa_advising}.notes (contact_type);
CREATE INDEX idx_bi_notes_is_private ON {bi_rds_schema_boa_advising}.notes (is_private);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: note_topics
-- Exlcude deleted topics.
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_boa_advising}.note_topics CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_boa_advising}.note_topics (
  note_topic_id INTEGER,
  note_id INTEGER REFERENCES {bi_rds_schema_boa_advising}.notes (note_id),
  topic VARCHAR(50),
  author_uid INTEGER REFERENCES {bi_rds_schema_boa_advising}.authors (author_uid),
  deleted_at TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY (note_topic_id, topic)
);

INSERT INTO {bi_rds_schema_boa_advising}.note_topics (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      note_topic_id,
      note_id,
      topic,
      author_uid,
      deleted_at
    FROM {bi_redshift_schema_boa_advising}.note_topics
    WHERE deleted_at IS NULL
  $REDSHIFT$)
  AS note_topics (
    note_topic_id INTEGER,
    note_id INTEGER,
    topic VARCHAR(50),
    author_uid INTEGER,
    deleted_at TIMESTAMP WITH TIME ZONE
  )
);

CREATE INDEX idx_bi_note_topics_note_id ON {bi_rds_schema_boa_advising}.note_topics (note_id);
CREATE INDEX idx_bi_note_topics_topic ON {bi_rds_schema_boa_advising}.note_topics (topic);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: topics
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_boa_advising}.topics CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_boa_advising}.topics (
  topic_id INTEGER PRIMARY KEY,
  topic VARCHAR(50),
  created_at TIMESTAMP WITH TIME ZONE,
  deleted_at TIMESTAMP WITH TIME ZONE
);

INSERT INTO {bi_rds_schema_boa_advising}.topics (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      topic_id,
      topic,
      created_at,
      deleted_at
    FROM {bi_redshift_schema_boa_advising}.topics
  $REDSHIFT$)
  AS topics (
    topic_id INTEGER,
    topic VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE,
    deleted_at TIMESTAMP WITH TIME ZONE
  )
);

CREATE INDEX idx_bi_topics_topic ON {bi_rds_schema_boa_advising}.topics (topic);
  

----------------------------------------------------------------------------------------------------
-- CREATE TABLE: student_groups
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_boa_advising}.student_groups CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_boa_advising}.student_groups (
  student_group_id INTEGER,
  student_group_name VARCHAR(255),
  sid BIGINT,
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
    sid BIGINT
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
  sid BIGINT,
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
    sid BIGINT
  )
);

CREATE INDEX idx_bi_student_cohorts_cohort_name ON {bi_rds_schema_boa_advising}.student_cohorts (cohort_name);
CREATE INDEX idx_bi_student_cohorts_sid ON {bi_rds_schema_boa_advising}.student_cohorts (sid);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: bi_reports_boa_advising.student_degrees
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_boa_advising}.student_degrees CASCADE;

CREATE TABLE IF NOT EXISTS {bi_rds_schema_boa_advising}.student_degrees (
  sid BIGINT,
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
    sid BIGINT,
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
-- Create materialized views for BOA-CE3 Advising Dashboard
-- Include author_dept_code = 'ZCEEE' as well as author_uid in list of additional users.
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- CREATE MATERIALIZED VIEW: ce3_notes_mv
----------------------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW {bi_rds_schema_boa_advising}.ce3_notes_mv AS
  WITH note_topics AS (
    SELECT
      note_id,
      STRING_AGG(DISTINCT topic, ', ' ORDER BY topic) AS topics
    FROM {bi_rds_schema_boa_advising}.note_topics
    GROUP BY note_id
  )
  
  SELECT
    notes.note_id,
    notes.author_uid,
    notes.sid,
    notes.contact_type,
    notes.created_at_date_pst AS create_date,
    notes.created_at_time_pst AS create_time, 
    notes.set_date,
    CASE WHEN notes.is_private THEN 'CE3 Only' ELSE 'All Advisors' END AS private,
    notes.subject,
    note_topics.topics
  FROM {bi_rds_schema_boa_advising}.notes notes
  LEFT JOIN note_topics
    ON notes.note_id = note_topics.note_id
  WHERE (notes.author_dept_code = 'ZCEEE' OR notes.author_uid in ({bi_rds_ce3_add_users}))
  AND notes.sid IS NOT NULL;

CREATE INDEX idx_bi_ce3_notes_note_id ON {bi_rds_schema_boa_advising}.ce3_notes_mv (note_id);
CREATE INDEX idx_bi_ce3_notes_author_uid ON {bi_rds_schema_boa_advising}.ce3_notes_mv (author_uid);
CREATE INDEX idx_bi_ce3_notes_sid ON {bi_rds_schema_boa_advising}.ce3_notes_mv (sid);
CREATE INDEX idx_bi_ce3_notes_contact_type ON {bi_rds_schema_boa_advising}.ce3_notes_mv (contact_type);
CREATE INDEX idx_bi_ce3_notes_create_date ON {bi_rds_schema_boa_advising}.ce3_notes_mv (create_date);
CREATE INDEX idx_bi_ce3_notes_set_date ON {bi_rds_schema_boa_advising}.ce3_notes_mv (set_date);
CREATE INDEX idx_bi_ce3_notes_private ON {bi_rds_schema_boa_advising}.ce3_notes_mv (private);
CREATE INDEX idx_bi_ce3_notes_subject ON {bi_rds_schema_boa_advising}.ce3_notes_mv (subject);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: ce3_author_contacts
-- counts by author_id of 1) distinct student count, 2) note count (total), 3) contact type count
----------------------------------------------------------------------------------------------------
DO $$
  DECLARE 
    trow RECORD;
    sqlstr TEXT;

  BEGIN
    DROP TABLE IF EXISTS {bi_rds_schema_boa_advising}.ce3_author_contacts CASCADE;

    CREATE TABLE IF NOT EXISTS {bi_rds_schema_boa_advising}.ce3_author_contacts AS
      SELECT
        author_uid,
        COUNT(DISTINCT sid) AS student_count,
        COUNT(*) AS note_count
      FROM {bi_rds_schema_boa_advising}.ce3_notes_mv
      GROUP BY author_uid;

    CREATE TEMP TABLE types_temp AS
      SELECT
        author_uid, 
        coalesce(contact_type, 'Unknown') AS contact_type,
        COUNT(contact_type) AS contact_count
      FROM {bi_rds_schema_boa_advising}.ce3_notes_mv
      GROUP BY 1, 2;
  
    FOR trow IN SELECT DISTINCT COALESCE(contact_type, 'Unknown') AS contact_type
                FROM {bi_rds_schema_boa_advising}.ce3_notes_mv
      LOOP
        sqlstr := 'ALTER TABLE {bi_rds_schema_boa_advising}.ce3_author_contacts ';
        sqlstr := sqlstr || 'ADD COLUMN ' || quote_ident(trow.contact_type) || ' INTEGER';
  
        EXECUTE sqlstr;
      
        sqlstr := 'UPDATE {bi_rds_schema_boa_advising}.ce3_author_contacts ac ';
        sqlstr := sqlstr || 'SET ' || quote_ident(trow.contact_type) || ' = t.contact_count ';
        sqlstr := sqlstr || 'FROM types_temp t ';
        sqlstr := sqlstr || 'WHERE ac.author_uid = t.author_uid ';
        sqlstr := sqlstr || 'AND COALESCE(t.contact_type, ''Unknown'') = ' || quote_literal(trow.contact_type);

        EXECUTE sqlstr;

      END LOOP;

    CREATE INDEX idx_bi_ce3_author_contacts_author_uid ON {bi_rds_schema_boa_advising}.ce3_author_contacts (author_uid);
  END
$$;


----------------------------------------------------------------------------------------------------
-- CREATE MATERIALIZED VIEW: ce3_authors_mv
----------------------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW {bi_rds_schema_boa_advising}.ce3_authors_mv AS
  WITH ce3_authors AS (
    SELECT DISTINCT author_uid
    FROM {bi_rds_schema_boa_advising}.notes
    WHERE (author_dept_code = 'ZCEEE' OR author_uid in ({bi_rds_ce3_add_users}))
  )

  SELECT
    authors.author_uid,
    authors.author_name_sort AS sort_name,
    authors.author_name AS full_name
  FROM ce3_authors
  JOIN {bi_rds_schema_boa_advising}.authors authors
    ON ce3_authors.author_uid = authors.author_uid;

CREATE INDEX idx_bi_ce3_authors_author_uid ON {bi_rds_schema_boa_advising}.ce3_authors_mv (author_uid);
CREATE INDEX idx_bi_ce3_authors_sort_name ON {bi_rds_schema_boa_advising}.ce3_authors_mv (sort_name);
CREATE INDEX idx_bi_ce3_authors_full_name ON {bi_rds_schema_boa_advising}.ce3_authors_mv (full_name);


----------------------------------------------------------------------------------------------------
-- CREATE MATERIALIZED VIEW: ce3_topics_mv
-- exclude duplicate and deleted topics
-- includes orphaned topics (in note_topics but not in topics)
----------------------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW {bi_rds_schema_boa_advising}.ce3_topics_mv AS
  SELECT DISTINCT
    topics.note_id,
    topics.topic
  FROM {bi_rds_schema_boa_advising}.note_topics topics
  JOIN {bi_rds_schema_boa_advising}.notes notes
    ON topics.note_id = notes.note_id
  WHERE (notes.author_dept_code = 'ZCEEE' OR notes.author_uid in ({bi_rds_ce3_add_users}))
  AND topics.deleted_at IS NULL;

CREATE INDEX idx_bi_ce3_topics_note_id ON {bi_rds_schema_boa_advising}.ce3_topics_mv (note_id);
CREATE INDEX idx_bi_ce3_topics_topic ON {bi_rds_schema_boa_advising}.ce3_topics_mv (topic);


----------------------------------------------------------------------------------------------------
-- CREATE MATERIALIZED VIEW: ce3_students_mv
----------------------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW {bi_rds_schema_boa_advising}.ce3_students_mv AS
  WITH ce3_students AS (
    SELECT DISTINCT sid
    FROM {bi_rds_schema_boa_advising}.notes
    WHERE (author_dept_code = 'ZCEEE' OR author_uid in ({bi_rds_ce3_add_users}))
  )
  
  SELECT
    students.sid,
    students.student_name_sort AS sort_name,
    students.student_name AS full_name,
    regexp_replace(students.cohort_list, '\|', CHR(10), 'g') as cohorts,
    regexp_replace(students.group_list, '\|', CHR(10), 'g') as groups,
    regexp_replace(students.degree_list, '\|', CHR(10), 'g') as degrees
  FROM ce3_students
  JOIN {bi_rds_schema_boa_advising}.students students
    ON ce3_students.sid = students.sid;

CREATE INDEX idx_bi_ce3_students_sid ON {bi_rds_schema_boa_advising}.ce3_students_mv (sid);
CREATE INDEX idx_bi_ce3_students_sort_name ON {bi_rds_schema_boa_advising}.ce3_students_mv (sort_name);


----------------------------------------------------------------------------------------------------
-- CREATE MATERIALIZED VIEW: ce3_student_cohorts_mv
----------------------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW {bi_rds_schema_boa_advising}.ce3_student_cohorts_mv AS
  WITH ce3_students AS (
    SELECT DISTINCT sid
    FROM {bi_rds_schema_boa_advising}.notes
    WHERE (author_dept_code = 'ZCEEE' OR author_uid in ({bi_rds_ce3_add_users}))
  )

  SELECT DISTINCT
     cohorts.sid,
     cohorts.cohort_name AS cohort_name
  FROM ce3_students
  JOIN {bi_rds_schema_boa_advising}.student_cohorts cohorts
    ON ce3_students.sid = cohorts.sid;

CREATE INDEX idx_bi_ce3_student_cohorts_sid ON {bi_rds_schema_boa_advising}.ce3_student_cohorts_mv (sid);
CREATE INDEX idx_bi_ce3_student_cohorts_cohort_name ON {bi_rds_schema_boa_advising}.ce3_student_cohorts_mv (cohort_name);


----------------------------------------------------------------------------------------------------
-- CREATE MATERIALIZED VIEW: ce3_student_groups_mv
----------------------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW {bi_rds_schema_boa_advising}.ce3_student_groups_mv AS
  WITH ce3_students AS (
    SELECT DISTINCT sid
    FROM {bi_rds_schema_boa_advising}.notes
    WHERE (author_dept_code = 'ZCEEE' OR author_uid in ({bi_rds_ce3_add_users}))
  )

  SELECT DISTINCT
    groups.sid,
    groups.student_group_name AS group_name
  FROM ce3_students ce3_students
  JOIN {bi_rds_schema_boa_advising}.student_groups groups
    ON ce3_students.sid = groups.sid;

CREATE INDEX idx_bi_ce3_student_groups_sid ON {bi_rds_schema_boa_advising}.ce3_student_groups_mv (sid);
CREATE INDEX idx_bi_ce3_student_groups_group_name ON {bi_rds_schema_boa_advising}.ce3_student_groups_mv (group_name);


----------------------------------------------------------------------------------------------------
-- CREATE MATERIALIZED VIEW: ce3_student_degrees_mv
----------------------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW {bi_rds_schema_boa_advising}.ce3_student_degrees_mv AS
  WITH ce3_students AS (
    SELECT DISTINCT sid
    FROM {bi_rds_schema_boa_advising}.notes
    WHERE (author_dept_code = 'ZCEEE' OR author_uid in ({bi_rds_ce3_add_users}))
  )

  SELECT DISTINCT
    degrees.sid,
    degrees.degree_awarded,
    degrees.degree_date
  FROM ce3_students
  JOIN {bi_rds_schema_boa_advising}.student_degrees degrees
    ON ce3_students.sid = degrees.sid;

CREATE INDEX idx_bi_ce3_student_degrees_sid ON {bi_rds_schema_boa_advising}.ce3_student_degrees_mv (sid);
CREATE INDEX idx_bi_ce3_student_degrees_degree ON {bi_rds_schema_boa_advising}.ce3_student_degrees_mv (degree_awarded);
CREATE INDEX idx_bi_ce3_student_degrees_date ON {bi_rds_schema_boa_advising}.ce3_student_degrees_mv (degree_date);


----------------------------------------------------------------------------------------------------
-- END script for creating and populating RDS schema/tables for Advising Notes Dashboard
----------------------------------------------------------------------------------------------------
