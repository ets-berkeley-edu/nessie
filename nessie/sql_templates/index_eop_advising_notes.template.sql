/**
 * Copyright ©2022. The Regents of the University of California (Regents). All Rights Reserved.
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

CREATE SCHEMA IF NOT EXISTS {rds_schema_eop};
GRANT USAGE ON SCHEMA {rds_schema_eop} TO {rds_app_boa_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_eop} GRANT SELECT ON TABLES TO {rds_app_boa_user};

BEGIN TRANSACTION;

DROP TABLE IF EXISTS {rds_schema_eop}.advising_notes CASCADE;

CREATE TABLE {rds_schema_eop}.advising_notes (
  id VARCHAR NOT NULL,
  sid VARCHAR NOT NULL,
  student_first_name VARCHAR,
  student_last_name VARCHAR,
  meeting_date VARCHAR,
  advisor_uid VARCHAR,
  advisor_first_name VARCHAR,
  advisor_last_name VARCHAR,
  overview VARCHAR,
  note TEXT,
  contact_method VARCHAR,
  attachment_url VARCHAR,
  privacy_permissions VARCHAR,
  searchable_topics VARCHAR,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL,
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
  PRIMARY KEY (id)
);

INSERT INTO {rds_schema_eop}.advising_notes (
  SELECT id, sid, student_first_name, student_last_name, meeting_date, advisor_uid, advisor_first_name,
    advisor_last_name, overview, note, contact_method, attachment_url, privacy_permissions,
    regexp_replace(topics, '(,?null)+', '') AS searchable_topics,
    CAST (created_at AS TIMESTAMP WITH TIME ZONE), CAST (updated_at AS TIMESTAMP WITH TIME ZONE)
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT DISTINCT id, sid, student_first_name, student_last_name, meeting_date, advisor_uid, advisor_first_name,
      advisor_last_name, overview, note, contact_method, attachment_url, privacy_permissions, topics,
      to_timestamp(COALESCE(meeting_date, '3/1/2023'), 'MM/DD/YYYY') AT TIME ZONE 'America/Los_Angeles' AT TIME ZONE 'UTC' AS created_at,
      to_timestamp(COALESCE(meeting_date, '3/1/2023'), 'MM/DD/YYYY') AT TIME ZONE 'America/Los_Angeles' AT TIME ZONE 'UTC' AS updated_at
    FROM {redshift_schema_eop_advising_notes_internal}.advising_notes
  $REDSHIFT$)
  AS redshift_notes (
    id VARCHAR,
    sid VARCHAR,
    student_first_name VARCHAR,
    student_last_name VARCHAR,
    meeting_date VARCHAR,
    advisor_uid VARCHAR,
    advisor_first_name VARCHAR,
    advisor_last_name VARCHAR,
    overview VARCHAR,
    note TEXT,
    contact_method VARCHAR,
    attachment_url VARCHAR,
    privacy_permissions VARCHAR,
    topics VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
  )
);

CREATE INDEX idx_eop_advising_notes_sid ON {rds_schema_eop}.advising_notes(sid);
CREATE INDEX idx_eop_advising_notes_advisor_uid ON {rds_schema_eop}.advising_notes(advisor_uid);
CREATE INDEX idx_eop_advising_notes_updated_at ON {rds_schema_eop}.advising_notes(updated_at);

DROP TABLE IF EXISTS {rds_schema_eop}.advising_note_topics CASCADE;

CREATE TABLE {rds_schema_eop}.advising_note_topics (
  id VARCHAR NOT NULL,
  sid VARCHAR NOT NULL,
  topic VARCHAR NOT NULL,
  PRIMARY KEY (id, topic)
);

INSERT INTO {rds_schema_eop}.advising_note_topics (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT DISTINCT id, sid, topic
    FROM {redshift_schema_eop_advising_notes_internal}.advising_note_topics
  $REDSHIFT$)
  AS redshift_note_topics (
    id VARCHAR,
    sid VARCHAR,
    topic VARCHAR
  )
);

DROP MATERIALIZED VIEW IF EXISTS {rds_schema_eop}.advising_notes_search_index CASCADE;

CREATE MATERIALIZED VIEW {rds_schema_eop}.advising_notes_search_index AS (
  SELECT n.id, to_tsvector('english', COALESCE(n.searchable_topics || ' ', '') || n.advisor_first_name || ' ' || n.advisor_last_name || ' ' || n.overview || ' ' || n.note) AS fts_index
  FROM {rds_schema_eop}.advising_notes n
);

CREATE INDEX idx_advising_notes_ft_search
ON {rds_schema_eop}.advising_notes_search_index
USING gin(fts_index);

COMMIT TRANSACTION;
