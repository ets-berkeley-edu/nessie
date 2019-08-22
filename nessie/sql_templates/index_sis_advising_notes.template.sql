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

CREATE SCHEMA IF NOT EXISTS {rds_schema_sis_advising_notes};
GRANT USAGE ON SCHEMA {rds_schema_sis_advising_notes} TO {rds_app_boa_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_sis_advising_notes} GRANT SELECT ON TABLES TO {rds_app_boa_user};

BEGIN TRANSACTION;

DROP TABLE IF EXISTS {rds_schema_sis_advising_notes}.advising_notes CASCADE;

CREATE TABLE {rds_schema_sis_advising_notes}.advising_notes (
  id VARCHAR NOT NULL,
  sid VARCHAR NOT NULL,
  student_note_nr INTEGER NOT NULL,
  advisor_sid VARCHAR NOT NULL,
  note_category VARCHAR,
  note_subcategory VARCHAR,
  note_body TEXT,
  created_by VARCHAR,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL,
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
  PRIMARY KEY (id)
);

INSERT INTO {rds_schema_sis_advising_notes}.advising_notes (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT id, sid, student_note_nr, advisor_sid, note_category, note_subcategory, note_body,
           created_by, created_at, updated_at
    FROM {redshift_schema_sis_advising_notes_internal}.advising_notes
    ORDER BY updated_at DESC
  $REDSHIFT$)
  AS redshift_notes (
    id VARCHAR,
    sid VARCHAR,
    student_note_nr INTEGER,
    advisor_sid VARCHAR,
    note_category VARCHAR,
    note_subcategory VARCHAR,
    note_body TEXT,
    created_by VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
  )
);

CREATE INDEX idx_sis_advising_notes_advisor_sid ON {rds_schema_sis_advising_notes}.advising_notes(advisor_sid);
CREATE INDEX idx_sis_advising_notes_created_at ON {rds_schema_sis_advising_notes}.advising_notes(created_at);
CREATE INDEX idx_sis_advising_notes_created_by ON {rds_schema_sis_advising_notes}.advising_notes(created_by);
CREATE INDEX idx_sis_advising_notes_sid ON {rds_schema_sis_advising_notes}.advising_notes(sid);
CREATE INDEX idx_sis_advising_notes_updated_at ON {rds_schema_sis_advising_notes}.advising_notes(updated_at);

DROP TABLE IF EXISTS {rds_schema_sis_advising_notes}.advising_note_attachments CASCADE;

CREATE TABLE {rds_schema_sis_advising_notes}.advising_note_attachments (
    advising_note_id VARCHAR,
    sid VARCHAR,
    student_note_nr VARCHAR,
    created_by VARCHAR,
    user_file_name VARCHAR,
    sis_file_name VARCHAR,
    is_historical BOOLEAN
);

INSERT INTO {rds_schema_sis_advising_notes}.advising_note_attachments (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT advising_note_id, sid, student_note_nr, created_by, user_file_name,
      sis_file_name, is_historical
    FROM {redshift_schema_sis_advising_notes_internal}.advising_note_attachments
  $REDSHIFT$)
  AS redshift_notes (
    advising_note_id VARCHAR,
    sid VARCHAR,
    student_note_nr VARCHAR,
    created_by VARCHAR,
    user_file_name VARCHAR,
    sis_file_name VARCHAR,
    is_historical BOOLEAN
  )
);

CREATE INDEX idx_sis_advising_note_attachments_advising_note_id
ON {rds_schema_sis_advising_notes}.advising_note_attachments(advising_note_id);

DROP TABLE IF EXISTS {rds_schema_sis_advising_notes}.advising_note_topics CASCADE;

CREATE TABLE {rds_schema_sis_advising_notes}.advising_note_topics (
  advising_note_id VARCHAR NOT NULL,
  note_topic VARCHAR NOT NULL,
  PRIMARY KEY (advising_note_id, note_topic)
);

INSERT INTO {rds_schema_sis_advising_notes}.advising_note_topics (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT DISTINCT advising_note_id, note_topic
    FROM {redshift_schema_sis_advising_notes_internal}.advising_note_topics
    WHERE note_topic IS NOT NULL
  $REDSHIFT$)
  AS redshift_notes (
    advising_note_id VARCHAR,
    note_topic VARCHAR
  )
);

CREATE INDEX idx_sis_advising_note_topics_topic ON {rds_schema_sis_advising_notes}.advising_note_topics(note_topic);

DROP MATERIALIZED VIEW IF EXISTS {rds_schema_sis_advising_notes}.advising_notes_search_index CASCADE;

CREATE MATERIALIZED VIEW {rds_schema_sis_advising_notes}.advising_notes_search_index AS (
  SELECT id, to_tsvector(
    'english',
    CASE
      WHEN note_body IS NOT NULL and TRIM(note_body) != '' THEN note_body
      WHEN note_subcategory IS NOT NULL THEN note_category || ' ' || note_subcategory
      ELSE note_category
    END
  ) AS fts_index
  FROM {rds_schema_sis_advising_notes}.advising_notes
);

CREATE INDEX idx_advising_notes_ft_search
ON {rds_schema_sis_advising_notes}.advising_notes_search_index
USING gin(fts_index);

COMMIT TRANSACTION;
