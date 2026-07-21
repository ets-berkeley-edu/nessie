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
-- BEGIN TRANSACTION: Create curated tables and indexes for Fulltext Search for Advising Notes.
--   Union BOA records to existing ASC, Data Science, E & I, EOP, History, and SIS records.
----------------------------------------------------------------------------------------------------

BEGIN TRANSACTION;

----------------------------------------------------------------------------------------------------
-- Add BOA App RDS Data authors added to boac_advising_notes.advising_note_authors. These advisors
--   have likely already been added by IndexAdvisingNotes, but this will capture any who may have
--   created a BOA note in the past but no longer have advisor permissions in SIS.
----------------------------------------------------------------------------------------------------

INSERT INTO {rds_schema_advising_notes}.advising_note_authors (uid, sid, first_name, last_name, campus_email)
  SELECT DISTINCT ba.ldap_uid AS uid, ba.sid, ba.first_name, ba.last_name, ba.email_address AS campus_email
  FROM {rds_schema_boa_app_rds_data}.advising_notes ann
  JOIN {rds_schema_sis_internal}.basic_attributes ba ON ann.advisor_uid = ba.ldap_uid
ON CONFLICT DO NOTHING;

----------------------------------------------------------------------------------------------------
-- Add BOA App RDS Data author names added to boac_advising_notes.advising_note_authors
--   to boac_advising_notes.advising_note_author_names.
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
-- Union {rds_schema_advising_notes} and {rds_schema_boa_app_rds_data}
--   as separate tables for adding incremental data.
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- Drop, create, and index table advising_notes_curated
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
  COALESCE(subject, '') || ' ' || COALESCE(note_body, '') AS note_body,
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
FROM {rds_schema_boa_app_rds_data}.advising_notes
);

CREATE INDEX advising_notes_curated_id_idx ON {rds_schema_advising_notes}.advising_notes_curated (id);
CREATE INDEX advising_notes_curated_sid_idx ON {rds_schema_advising_notes}.advising_notes_curated (sid);
CREATE INDEX advising_notes_curated_advisor_sid_idx ON {rds_schema_advising_notes}.advising_notes_curated (advisor_sid);
CREATE INDEX advising_notes_curated_advisor_uid_idx ON {rds_schema_advising_notes}.advising_notes_curated (advisor_uid);
CREATE INDEX advising_notes_curated_created_at_idx ON {rds_schema_advising_notes}.advising_notes_curated (created_at);
CREATE INDEX advising_notes_curated_created_by_idx ON {rds_schema_advising_notes}.advising_notes_curated (created_by);
CREATE INDEX advising_notes_curated_updated_at_idx ON {rds_schema_advising_notes}.advising_notes_curated (updated_at);

----------------------------------------------------------------------------------------------------
-- Create and index table advising_notes_search_index_curated
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {rds_schema_advising_notes}.advising_notes_search_index_curated CASCADE;

CREATE TABLE IF NOT EXISTS {rds_schema_advising_notes}.advising_notes_search_index_curated AS (
  SELECT id, fts_index
  FROM {rds_schema_advising_notes}.advising_notes_search_index
  UNION
  SELECT id, fts_index
  FROM {rds_schema_boa_app_rds_data}.advising_notes_search_index
);

CREATE INDEX advising_notes_search_curated_fts_index_idx
  ON {rds_schema_advising_notes}.advising_notes_search_index_curated
  USING GIN (fts_index);


----------------------------------------------------------------------------------------------------
-- COMMIT TRANSACTION
----------------------------------------------------------------------------------------------------

COMMIT TRANSACTION;

