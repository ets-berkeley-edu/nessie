/**
 * Copyright ©2021. The Regents of the University of California (Regents). All Rights Reserved.
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

CREATE SCHEMA IF NOT EXISTS {rds_schema_history_dept};
GRANT USAGE ON SCHEMA {rds_schema_history_dept} TO {rds_app_boa_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_history_dept} GRANT SELECT ON TABLES TO {rds_app_boa_user};

BEGIN TRANSACTION;

DROP TABLE IF EXISTS {rds_schema_history_dept}.advising_notes CASCADE;

CREATE TABLE {rds_schema_history_dept}.advising_notes (
  id VARCHAR NOT NULL,
  sid VARCHAR NOT NULL,
  student_first_name VARCHAR,
  student_last_name VARCHAR,
  note TEXT,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL,
  PRIMARY KEY (id)
);

INSERT INTO {rds_schema_history_dept}.advising_notes (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT DISTINCT id, sid, student_first_name, student_last_name, note
    FROM {redshift_schema_history_dept_advising_internal}.advising_notes
    WHERE sid <> ''
  $REDSHIFT$)
  AS redshift_notes (
    id VARCHAR,
    sid VARCHAR,
    student_first_name VARCHAR,
    student_last_name VARCHAR,
    note TEXT
  )
);

CREATE INDEX idx_history_dept_advising_notes_sid ON {rds_schema_history_dept}.advising_notes(sid);

DROP MATERIALIZED VIEW IF EXISTS {rds_schema_history_dept}.advising_notes_search_index CASCADE;

CREATE MATERIALIZED VIEW {rds_schema_history_dept}.advising_notes_search_index AS (
  SELECT n.id, to_tsvector('english', COALESCE(n.note || ' ', '')) AS fts_index
  FROM {rds_schema_history_dept}.advising_notes n
);

CREATE INDEX idx_advising_notes_ft_search
ON {rds_schema_history_dept}.advising_notes_search_index
USING gin(fts_index);

COMMIT TRANSACTION;
