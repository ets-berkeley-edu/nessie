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

-----------------------------------------------------------------------------------------------------
-- BEGIN: Create Nessie RDS schema for BOA App RDS Data Advising Notes Fulltext Search/Index
-----------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------
-- Create schema, grant usage, and alter default privileges
-----------------------------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS {rds_schema_boa_app_rds_data};

GRANT USAGE
  ON SCHEMA {rds_schema_boa_app_rds_data}
  TO {rds_app_boa_user};

ALTER DEFAULT PRIVILEGES
  IN SCHEMA {rds_schema_boa_app_rds_data}
  GRANT SELECT ON TABLES TO {rds_app_boa_user};


-----------------------------------------------------------------------------------------------------
-- Create tables from BOA App RDS Data Redshift external schema via dblink
-----------------------------------------------------------------------------------------------------

BEGIN TRANSACTION;

-----------------------------------------------------------------------------------------------------
-- Create table advising_notes
--   generate new id as (sid || '-' || id)
--   author name changes over time as it changes in CalNet
--   first_name and last_name are parsed from notes.author_name
--     remove everything following comma, if exists
--     last_name is everything that follows last space in author_name
--     first_name is everything that precedes last space in author_name
--   strip seconds from created_at and updated_at timestamps and convert to timestamptz
--   exclude drafts, deleted records, and records not associated with a student
-----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {rds_schema_boa_app_rds_data}.advising_notes CASCADE;

CREATE TABLE {rds_schema_boa_app_rds_data}.advising_notes (
  id VARCHAR PRIMARY KEY,
  sid VARCHAR NOT NULL,
  boa_id VARCHAR NOT NULL,
  advisor_uid VARCHAR,
  author_name VARCHAR,
  advisor_first_name VARCHAR,
  advisor_last_name VARCHAR,
  subject VARCHAR,
  note_body TEXT,
  is_private BOOLEAN,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL,
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

INSERT INTO {rds_schema_boa_app_rds_data}.advising_notes (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',
    $REDSHIFT$
      SELECT
        sid || '-' || id AS id,
        sid,
        id::VARCHAR AS boa_id,
        author_uid AS advisor_uid,
        TRIM(author_name) AS author_name,
        REGEXP_REPLACE(REGEXP_REPLACE(TRIM(author_name), ',.*$', ''), '^(.+) ([^ ]+)$', '$1') AS advisor_first_name,
        REGEXP_REPLACE(REGEXP_REPLACE(TRIM(author_name), ',.*$', ''), '^(.+) ([^ ]+)$', '$2') AS advisor_last_name,
        subject,
        body AS note_body,
        is_private,
        TO_TIMESTAMP(DATE_TRUNC('minute', created_at), 'YYYY-MM-DD"T"HH.MI.SS%z') AS created_at,
        TO_TIMESTAMP(DATE_TRUNC('minute', updated_at), 'YYYY-MM-DD"T"HH.MI.SS%z') AS updated_at
      FROM {redshift_schema_boa_app_rds_data}.notes 
      WHERE is_draft IS FALSE
      AND deleted_at IS NULL
      AND sid IS NOT NULL
    $REDSHIFT$)
  AS rs_notes (
    id VARCHAR,
    sid VARCHAR,
    boa_id VARCHAR,
    advisor_uid VARCHAR,
    author_name VARCHAR,
    advisor_first_name VARCHAR,
    advisor_last_name VARCHAR,
    subject VARCHAR,
    note_body TEXT,
    is_private BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
  )
);


-----------------------------------------------------------------------------------------------------
-- Create indexes on table advising_notes
-----------------------------------------------------------------------------------------------------

CREATE INDEX idx_advising_notes_sid
  ON {rds_schema_boa_app_rds_data}.advising_notes (sid);

CREATE INDEX idx_advising_notes_boa_id
  ON {rds_schema_boa_app_rds_data}.advising_notes (boa_id);

CREATE INDEX idx_advising_notes_advisor_uid
  ON {rds_schema_boa_app_rds_data}.advising_notes (advisor_uid);

CREATE INDEX idx_advising_notes_updated_at
  ON {rds_schema_boa_app_rds_data}.advising_notes (updated_at);


-----------------------------------------------------------------------------------------------------
-- Create table advising_note_topics
--   use generated id (sid || '-' || id) from advising_notes as identifier
--   remove duplicate topics for each note
--   exclude deleted note_topic records
-----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {rds_schema_boa_app_rds_data}.advising_note_topics CASCADE;

CREATE TABLE {rds_schema_boa_app_rds_data}.advising_note_topics (
  id VARCHAR NOT NULL,
  sid VARCHAR NOT NULL,
  boa_id VARCHAR NOT NULL,
  topic VARCHAR NOT NULL,
  PRIMARY KEY (id, topic)
);

INSERT INTO {rds_schema_boa_app_rds_data}.advising_note_topics (
  SELECT DISTINCT
    n.id,
    n.sid,
    n.boa_id,
    rs_nt.topic
  FROM {rds_schema_boa_app_rds_data}.advising_notes n
  JOIN dblink('{rds_dblink_to_redshift}',
    $REDSHIFT$
      SELECT DISTINCT note_id, topic
      FROM {redshift_schema_boa_app_rds_data}.note_topics
      WHERE deleted_at IS NULL
    $REDSHIFT$)
  AS rs_nt (note_id VARCHAR, topic VARCHAR)
    ON n.boa_id = rs_nt.note_id
);

CREATE INDEX idx_advising_notes_topics_sid
  ON {rds_schema_boa_app_rds_data}.advising_note_topics (sid);

CREATE INDEX idx_advising_notes_topics_boa_id
  ON {rds_schema_boa_app_rds_data}.advising_note_topics (boa_id);

CREATE INDEX idx_advising_notes_topics_topic
  ON {rds_schema_boa_app_rds_data}.advising_note_topics (topic);


-----------------------------------------------------------------------------------------------------
-- Create table author_depts
-- one:many uid:dept_code and retain deleted records to show prior relationships
-----------------------------------------------------------------------------------------------------

CREATE TABLE {rds_schema_boa_app_rds_data}.author_depts (
  uid INTEGER,
  dept_code VARCHAR(80)
);


INSERT INTO {rds_schema_boa_app_rds_data}.author_depts (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      au.uid,
      ud.dept_code
    FROM {redshift_schema_boa_app_rds_data}.authorized_users au
    JOIN {redshift_schema_boa_app_rds_data}.university_dept_members udm ON au.id = udm.authorized_user_id
    JOIN {redshift_schema_boa_app_rds_data}.university_depts ud ON udm.university_dept_id = ud.id
    UNION
    SELECT
      au.uid,
      ud.dept_code
    FROM {redshift_schema_boa_app_rds_data}.authorized_users au
    JOIN {redshift_schema_boa_app_rds_data}.peer_advising_department_members pdm ON au.id = pdm.authorized_user_id
    JOIN {redshift_schema_boa_app_rds_data}.peer_advising_departments pd ON pdm.peer_advising_department_id = pd.id
    JOIN {redshift_schema_boa_app_rds_data}.university_depts ud ON pd.university_dept_id = ud.id
  $REDSHIFT$)
  AS rs_ad (
    uid INTEGER,
    dept_code VARCHAR(80)
  )
);

CREATE INDEX idx_author_depts_uid
  ON {rds_schema_boa_app_rds_data}.author_depts (uid);

CREATE INDEX idx_author_depts_dept_code
  ON {rds_schema_boa_app_rds_data}.author_depts (dept_code);


-----------------------------------------------------------------------------------------------------
-- Create materialized view advising_notes_search_index and GIN index
-----------------------------------------------------------------------------------------------------

DROP MATERIALIZED VIEW IF EXISTS {rds_schema_boa_app_rds_data}.advising_notes_search_index CASCADE;

CREATE MATERIALIZED VIEW {rds_schema_boa_app_rds_data}.advising_notes_search_index AS (
  SELECT
    n.id,
    TO_TSVECTOR(
      'english',
      CASE
        WHEN n.note_body IS NOT NULL THEN COALESCE(n.subject || ' ', '') || n.note_body
        ELSE COALESCE(t.topic || ' ', '') || n.advisor_first_name || ' ' || n.advisor_last_name
      END
    ) AS fts_index
  FROM {rds_schema_boa_app_rds_data}.advising_notes n
  LEFT OUTER JOIN {rds_schema_boa_app_rds_data}.advising_note_topics t ON n.id = t.id
);

CREATE INDEX idx_advising_notes_ft_search
  ON {rds_schema_boa_app_rds_data}.advising_notes_search_index
  USING gin (fts_index);

COMMIT TRANSACTION;

-----------------------------------------------------------------------------------------------------
-- End: Create Nessie RDS schema for BOA App RDS Data Advising Notes Fulltext Search/Index
-----------------------------------------------------------------------------------------------------
