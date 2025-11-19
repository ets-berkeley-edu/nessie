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
-- Create Nessie Redshift Internal Schema for BOA App RDS Advising Notes
-----------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------
-- Drop and re-create Redshift internal schema, grant usage, and alter default privileges
-----------------------------------------------------------------------------------------------------

DROP SCHEMA IF EXISTS {redshift_schema_boa_app_rds_data_advising_notes_internal} CASCADE;

CREATE SCHEMA {redshift_schema_boa_app_rds_data_advising_notes_internal};

GRANT USAGE ON SCHEMA {redshift_schema_boa_app_rds_data_advising_notes_internal}
  TO GROUP {redshift_app_boa_user}_group,
     GROUP {redshift_dblink_group};

ALTER DEFAULT PRIVILEGES IN SCHEMA {redshift_schema_boa_app_rds_data_advising_notes_internal}
  GRANT SELECT ON TABLES
    TO GROUP {redshift_app_boa_user}_group,
       GROUP {redshift_dblink_group};


-----------------------------------------------------------------------------------------------------
-- Create internal schema tables
-----------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------
-- Create table advising_notes
-- generate new id as (sid || '-' || id)
-- author name changes over time as it changes in CalNet
-- first_name and last_name are parsed from author_name
--   remove everything following comma, if exists
--   last_name is everything that follows last space in author_name
--   first_name is everything that precedes last space in author_name
-- strip seconds from created_at and updated_at timestamps and convert to timestamptz
-- exclude drafts, deleted records, and records not associated with a student
-----------------------------------------------------------------------------------------------------

CREATE TABLE {redshift_schema_boa_app_rds_data_advising_notes_internal}.advising_notes
SORTKEY (id)
AS (
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
);


-----------------------------------------------------------------------------------------------------
-- Create table advising_note_topics
-- use generated id (sid || '-' || id) from advising_notes as identifier
-- remove duplicate topics for each note
-- exclude deleted note_topic records
-----------------------------------------------------------------------------------------------------

CREATE TABLE {redshift_schema_boa_app_rds_data_advising_notes_internal}.advising_note_topics
SORTKEY (id)
AS (
  SELECT DISTINCT
    n.id,
    n.sid,
    n.boa_id,
    nt.topic::VARCHAR
  FROM {redshift_schema_boa_app_rds_data_advising_notes_internal}.advising_notes n
  JOIN {redshift_schema_boa_app_rds_data}.note_topics nt ON n.boa_id = nt.note_id
  WHERE nt.deleted_at IS NULL
);


-----------------------------------------------------------------------------------------------------
-- END of BOA App Advising Notes SQL Template
-----------------------------------------------------------------------------------------------------
