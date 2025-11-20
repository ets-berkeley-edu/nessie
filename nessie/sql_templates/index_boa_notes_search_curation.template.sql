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
-- Create curated tables and indexes for Fulltext Search for Advising Notes.
--   Union BOA records to existing ASC, Data Science, E & I, EOP, History, and SIS records.
----------------------------------------------------------------------------------------------------

BEGIN TRANSACTION;

----------------------------------------------------------------------------------------------------
-- Add BOA App RDS Data author names to boac_advising_notes.advising_note_author_names.
----------------------------------------------------------------------------------------------------

INSERT INTO {rds_schema_advising_notes}.advising_note_author_names (uid, name) (
  SELECT DISTINCT uid, unnest(string_to_array(regexp_replace(upper(first_name), '[^\w ]', '', 'g'), ' ')) AS name
  FROM {rds_schema_advising_notes}.advising_note_authors
  UNION
  SELECT DISTINCT uid, unnest(string_to_array(regexp_replace(upper(last_name), '[^\w ]', '', 'g'), ' ')) AS name
  FROM {rds_schema_advising_notes}.advising_note_authors
)
ON CONFLICT DO NOTHING;

----------------------------------------------------------------------------------------------------
-- Create curated advising notes tables and indexes
-- Union {rds_schema_advising_notes} and {rds_schema_bard}
--   as separate tables for adding incremental data.
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- Create table advising_notes_curated and indexes.
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {rds_schema_advising_notes}.advising_notes_curated CASCADE;

CREATE TABLE {rds_schema_advising_notes}.advising_notes_curated AS (
SELECT
  sid,
  id,
  note_body,
  advisor_sid,
  advisor_uid,
  advisor_first_name,
  advisor_last_name,
  note_category,
  note_subcategory,
  is_private,
  created_by,
  created_at,
  updated_at
FROM {rds_schema_advising_notes}.advising_notes
UNION
SELECT
  sid,
  id,
  COALESCE(subject || ' ', '') || COALESCE(note_body, '') AS note_body,
  NULL AS advisor_sid,
  advisor_uid,
  advisor_first_name,
  advisor_last_name,
  NULL AS note_category,
  NULL AS note_subcategory,
  is_private,
  advisor_uid AS created_by,
  created_at,
  updated_at
FROM {rds_schema_bard}.advising_notes
);

CREATE INDEX idx_advising_notes_curated_id ON {rds_schema_advising_notes}.advising_notes_curated (id);
CREATE INDEX idx_advising_notes_curated_sid ON {rds_schema_advising_notes}.advising_notes_curated (sid);
CREATE INDEX idx_advising_notes_curated_advisor_sid ON {rds_schema_advising_notes}.advising_notes_curated (advisor_sid);
CREATE INDEX idx_advising_notes_curated_advisor_uid ON {rds_schema_advising_notes}.advising_notes_curated (advisor_uid);
CREATE INDEX idx_advising_notes_curated_created_at ON {rds_schema_advising_notes}.advising_notes_curated (created_at);
CREATE INDEX idx_advising_notes_curated_created_by ON {rds_schema_advising_notes}.advising_notes_curated (created_by);
CREATE INDEX idx_advising_notes_curated_updated_at ON {rds_schema_advising_notes}.advising_notes_curated (updated_at);

----------------------------------------------------------------------------------------------------
-- Create table advising_notes_search_index_curated and GIN index.
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {rds_schema_advising_notes}.advising_notes_search_index_curated CASCADE;

CREATE TABLE {rds_schema_advising_notes}.advising_notes_search_index_curated AS (
  SELECT id, fts_index
  FROM {rds_schema_advising_notes}.advising_notes_search_index
  UNION
  SELECT id, fts_index
  FROM {rds_schema_bard}.advising_notes_search_index
);

CREATE INDEX idx_advising_notes_ft_search_curated
  ON {rds_schema_advising_notes}.advising_notes_search_index_curated
  USING GIN (fts_index);

COMMIT TRANSACTION;
