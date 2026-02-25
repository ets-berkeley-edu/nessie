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

CREATE INDEX idx_authors_author_name_sort ON {bi_rds_schema_boa_advising}.authors (author_name_sort);
CREATE INDEX idx_authors_author_name ON {bi_rds_schema_boa_advising}.authors (author_name);
CREATE INDEX idx_authors_author_aliases ON {bi_rds_schema_boa_advising}.authors (author_aliases);


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

CREATE INDEX idx_departments_dept_name ON {bi_rds_schema_boa_advising}.departments (dept_name);


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
    cohort_list VARCHAR(65535),
    group_list VARCHAR(65535),
    degree_list VARCHAR(65535)
  )
);

CREATE INDEX idx_students_student_name_sort ON {bi_rds_schema_boa_advising}.students (student_name_sort);
CREATE INDEX idx_students_student_name ON {bi_rds_schema_boa_advising}.students (student_name);


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

CREATE INDEX idx_notes_created_at ON {bi_rds_schema_boa_advising}.notes (created_at);
CREATE INDEX idx_notes_set_date ON {bi_rds_schema_boa_advising}.notes (set_date);
CREATE INDEX idx_notes_contact_type ON {bi_rds_schema_boa_advising}.notes (contact_type);
CREATE INDEX idx_notes_is_private ON {bi_rds_schema_boa_advising}.notes (is_private);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: note_topics
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

CREATE INDEX idx_note_topics_note_id ON {bi_rds_schema_boa_advising}.note_topics (note_id);
CREATE INDEX idx_note_topics_topic ON {bi_rds_schema_boa_advising}.note_topics (topic);


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

CREATE INDEX idx_topics_topic ON {bi_rds_schema_boa_advising}.topics (topic);
  

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

CREATE INDEX idx_student_groups_student_group_name ON {bi_rds_schema_boa_advising}.student_groups (student_group_name);
CREATE INDEX idx_student_groups_sid ON {bi_rds_schema_boa_advising}.student_groups (sid);


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

CREATE INDEX idx_student_cohorts_cohort_name ON {bi_rds_schema_boa_advising}.student_cohorts (cohort_name);
CREATE INDEX idx_student_cohorts_sid ON {bi_rds_schema_boa_advising}.student_cohorts (sid);


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

CREATE INDEX idx_student_degrees_sid ON {bi_rds_schema_boa_advising}.student_degrees (sid);
CREATE INDEX idx_student_degrees_degree_date ON {bi_rds_schema_boa_advising}.student_degrees (degree_date);
CREATE INDEX idx_student_degrees_degree_awarded ON {bi_rds_schema_boa_advising}.student_degrees (degree_awarded);


----------------------------------------------------------------------------------------------------
-- Create materialized views for BOA-CE3 Advising Dashboard
-- Include author_dept_code = 'ZCEEE' as well as author_uid in list of additional users.
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- CREATE MATERIALIZED VIEW: ce3_notes_mv
----------------------------------------------------------------------------------------------------

DROP MATERIALIZED VIEW IF EXISTS {bi_rds_schema_boa_advising}.ce3_notes_mv CASCADE;

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

CREATE INDEX idx_ce3_notes_mv_note_id ON {bi_rds_schema_boa_advising}.ce3_notes_mv (note_id);
CREATE INDEX idx_ce3_notes_mv_author_uid ON {bi_rds_schema_boa_advising}.ce3_notes_mv (author_uid);
CREATE INDEX idx_ce3_notes_mv_sid ON {bi_rds_schema_boa_advising}.ce3_notes_mv (sid);
CREATE INDEX idx_ce3_notes_mv_contact_type ON {bi_rds_schema_boa_advising}.ce3_notes_mv (contact_type);
CREATE INDEX idx_ce3_notes_mv_create_date ON {bi_rds_schema_boa_advising}.ce3_notes_mv (create_date);
CREATE INDEX idx_ce3_notes_mv_set_date ON {bi_rds_schema_boa_advising}.ce3_notes_mv (set_date);
CREATE INDEX idx_ce3_notes_mv_private ON {bi_rds_schema_boa_advising}.ce3_notes_mv (private);
CREATE INDEX idx_ce3_notes_mv_subject ON {bi_rds_schema_boa_advising}.ce3_notes_mv (subject);


----------------------------------------------------------------------------------------------------
-- CREATE MATERIALIZED VIEW: ce3_authors_mv
----------------------------------------------------------------------------------------------------

DROP MATERIALIZED VIEW IF EXISTS {bi_rds_schema_boa_advising}.ce3_authors_mv CASCADE;

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

CREATE INDEX idx_ce3_authors_mv_author_uid ON {bi_rds_schema_boa_advising}.ce3_authors_mv (author_uid);
CREATE INDEX idx_ce3_authors_mv_sort_name ON {bi_rds_schema_boa_advising}.ce3_authors_mv (sort_name);
CREATE INDEX idx_ce3_authors_mv_full_name ON {bi_rds_schema_boa_advising}.ce3_authors_mv (full_name);


----------------------------------------------------------------------------------------------------
-- CREATE TEMP TABLES to build TABLE ce3_author_contacts_counts
----------------------------------------------------------------------------------------------------

DO $$
  DECLARE
    trow RECORD;
    sqlstr TEXT;

  BEGIN

    -- get daily note count and student count for each note author
    CREATE TEMP TABLE tt_total_counts AS
      SELECT
        n.author_uid,
        a.full_name as author_name,
        n.create_date,
        COUNT(n.note_id) AS note_count,
        COUNT(DISTINCT(n.sid)) AS sid_count
      FROM {bi_rds_schema_boa_advising}.ce3_notes_mv n
      LEFT OUTER JOIN {bi_rds_schema_boa_advising}.ce3_authors_mv a ON (n.author_uid = a.author_uid)
      GROUP BY 1, 2, 3;

    -- get daily contact type counts for each note author and contact_type
    CREATE TEMP TABLE tt_author_date_contact_counts AS
      SELECT
        author_uid,
        create_date,
        COALESCE(contact_type, 'Unknown') AS contact_type,
        COUNT(note_id) AS ct_count
      FROM {bi_rds_schema_boa_advising}.ce3_notes_mv
      GROUP BY 1, 2, 3;

    -- loop through contact_types to get daily note count for each note author by contact_type
    FOR trow IN
      SELECT DISTINCT
        COALESCE(contact_type, 'Unknown') AS contact_type,
        COALESCE(REGEXP_REPLACE(LOWER(contact_type), '[^A-Za-z]', '', 'g'), 'unknown') as ct_name
      FROM tt_author_date_contact_counts
    LOOP
        sqlstr := 'CREATE TEMP TABLE tt_' || trow.ct_name || ' AS ';
        sqlstr := sqlstr || 'SELECT author_uid, create_date, ct_count FROM tt_author_date_contact_counts ';
        sqlstr := sqlstr || 'WHERE contact_type = ''' || trow.contact_type || '''';

        -- RAISE NOTICE 'sqlstr: %', sqlstr;
        EXECUTE sqlstr;

    END LOOP;

  END
$$;


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: ce3_author_contact_counts
-- Current contact_type values from note_contact_type_enum 
--   'Email', 'Phone', 'Online same day', 'Online scheduled',
--   'In-person same day', 'In-person scheduled', 'Group event', 'Admin',
--   NULL is converted to 'Unknown'
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_boa_advising}.ce3_author_contact_counts CASCADE;

CREATE TABLE {bi_rds_schema_boa_advising}.ce3_author_contact_counts AS
  SELECT
    tc.author_uid, tc.author_name, tc.create_date,
    COALESCE(a.ct_count, 0) AS admin,
    COALESCE(e.ct_count, 0) AS email,
    COALESCE(g.ct_count, 0) AS group,
    COALESCE(ipsd.ct_count, 0) AS inperson_sd,
    COALESCE(ips.ct_count, 0) AS inperson_sch,
    COALESCE(osd.ct_count, 0) AS online_sd,
    COALESCE(os.ct_count, 0) AS online_sch,
    COALESCE(p.ct_count, 0) AS phone,
    COALESCE(u.ct_count, 0) AS unknown,
    COALESCE(tc.note_count, 0) AS note_count,
    COALESCE(tc.sid_count, 0) AS sid_count
  FROM tt_total_counts tc
  LEFT OUTER JOIN tt_admin a ON (tc.author_uid = a.author_uid AND tc.create_date = a.create_date)
  LEFT OUTER JOIN tt_email e ON (tc.author_uid = e.author_uid AND tc.create_date = e.create_date)
  LEFT OUTER JOIN tt_groupevent g ON (tc.author_uid = g.author_uid AND tc.create_date = g.create_date)
  LEFT OUTER JOIN tt_inpersonscheduled ips ON (tc.author_uid = ips.author_uid AND tc.create_date = ips.create_date)
  LEFT OUTER JOIN tt_inpersonsameday ipsd ON (tc.author_uid = ipsd.author_uid AND tc.create_date = ipsd.create_date)
  LEFT OUTER JOIN tt_onlinesameday osd ON (tc.author_uid = osd.author_uid AND tc.create_date = osd.create_date)
  LEFT OUTER JOIN tt_onlinescheduled os ON (tc.author_uid = os.author_uid AND tc.create_date = os.create_date)
  LEFT OUTER JOIN tt_phone p ON (tc.author_uid = p.author_uid AND tc.create_date = p.create_date)
  LEFT OUTER JOIN tt_unknown u ON (tc.author_uid = u.author_uid AND tc.create_date = u.create_date);

CREATE UNIQUE INDEX idx_ce3_author_contact_counts_auid_crdate ON {bi_rds_schema_boa_advising}.ce3_author_contact_counts (author_uid, create_date);
CREATE INDEX idx_ce3_author_contact_counts_author_name ON {bi_rds_schema_boa_advising}.ce3_author_contact_counts (author_name);
CREATE INDEX idx_ce3_author_contact_counts_create_date ON {bi_rds_schema_boa_advising}.ce3_author_contact_counts (create_date);


----------------------------------------------------------------------------------------------------
-- CREATE MATERIALIZED VIEW: ce3_topics_mv
-- Exclude duplicate and deleted topics.
-- Include orphaned topics (in note_topics but not in topics).
----------------------------------------------------------------------------------------------------

DROP MATERIALIZED VIEW IF EXISTS {bi_rds_schema_boa_advising}.ce3_topics_mv CASCADE;

CREATE MATERIALIZED VIEW {bi_rds_schema_boa_advising}.ce3_topics_mv AS
  SELECT DISTINCT
    topics.note_id,
    topics.topic
  FROM {bi_rds_schema_boa_advising}.note_topics topics
  JOIN {bi_rds_schema_boa_advising}.notes notes
    ON topics.note_id = notes.note_id
  WHERE (notes.author_dept_code = 'ZCEEE' OR notes.author_uid in ({bi_rds_ce3_add_users}))
  AND topics.deleted_at IS NULL;

CREATE INDEX idx_ce3_topics_mv_note_id ON {bi_rds_schema_boa_advising}.ce3_topics_mv (note_id);
CREATE INDEX idx_ce3_topics_mv_topic ON {bi_rds_schema_boa_advising}.ce3_topics_mv (topic);


----------------------------------------------------------------------------------------------------
-- CREATE MATERIALIZED VIEW: ce3_students_mv
----------------------------------------------------------------------------------------------------

DROP MATERIALIZED VIEW IF EXISTS {bi_rds_schema_boa_advising}.ce3_students_mv CASCADE;

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
    REGEXP_REPLACE(students.cohort_list, '\|', CHR(10), 'g') as cohorts,
    REGEXP_REPLACE(students.group_list, '\|', CHR(10), 'g') as groups,
    REGEXP_REPLACE(students.degree_list, '\|', CHR(10), 'g') as degrees
  FROM ce3_students
  JOIN {bi_rds_schema_boa_advising}.students students
    ON ce3_students.sid = students.sid;

CREATE INDEX idx_ce3_students_mv_sid ON {bi_rds_schema_boa_advising}.ce3_students_mv (sid);
CREATE INDEX idx_ce3_students_mv_sort_name ON {bi_rds_schema_boa_advising}.ce3_students_mv (sort_name);


----------------------------------------------------------------------------------------------------
-- CREATE MATERIALIZED VIEW: ce3_student_cohorts_mv
----------------------------------------------------------------------------------------------------

DROP MATERIALIZED VIEW IF EXISTS {bi_rds_schema_boa_advising}.ce3_student_cohorts_mv CASCADE;

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

CREATE INDEX idx_ce3_student_cohorts_mv_sid ON {bi_rds_schema_boa_advising}.ce3_student_cohorts_mv (sid);
CREATE INDEX idx_ce3_student_cohorts_mv_cohort_name ON {bi_rds_schema_boa_advising}.ce3_student_cohorts_mv (cohort_name);


----------------------------------------------------------------------------------------------------
-- CREATE MATERIALIZED VIEW: ce3_student_groups_mv
----------------------------------------------------------------------------------------------------

DROP MATERIALIZED VIEW IF EXISTS {bi_rds_schema_boa_advising}.ce3_student_groups_mv CASCADE;

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

CREATE INDEX idx_ce3_student_groups_mv_sid ON {bi_rds_schema_boa_advising}.ce3_student_groups_mv (sid);
CREATE INDEX idx_ce3_student_groups_mv_group_name ON {bi_rds_schema_boa_advising}.ce3_student_groups_mv (group_name);


----------------------------------------------------------------------------------------------------
-- CREATE MATERIALIZED VIEW: ce3_student_degrees_mv
----------------------------------------------------------------------------------------------------

DROP MATERIALIZED VIEW IF EXISTS {bi_rds_schema_boa_advising}.ce3_student_degrees_mv CASCADE;

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

CREATE INDEX idx_ce3_student_degrees_mv_sid ON {bi_rds_schema_boa_advising}.ce3_student_degrees_mv (sid);
CREATE INDEX idx_ce3_student_degrees_mv_degree ON {bi_rds_schema_boa_advising}.ce3_student_degrees_mv (degree_awarded);
CREATE INDEX idx_ce3_student_degrees_mv_date ON {bi_rds_schema_boa_advising}.ce3_student_degrees_mv (degree_date);


----------------------------------------------------------------------------------------------------
-- END script for creating and populating RDS schema/tables for Advising Notes Dashboard
----------------------------------------------------------------------------------------------------
