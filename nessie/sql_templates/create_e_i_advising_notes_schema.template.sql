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

--------------------------------------------------------------------
-- CREATE EXTERNAL SCHEMA
--------------------------------------------------------------------

CREATE EXTERNAL SCHEMA {redshift_schema_e_i_advising_notes}
FROM data catalog
DATABASE '{redshift_schema_e_i_advising_notes}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

CREATE EXTERNAL TABLE {redshift_schema_e_i_advising_notes}.advising_notes
(
  id VARCHAR,
  studentSid VARCHAR,
  studentFirstName VARCHAR,
  studentLastName VARCHAR,
  meetingDate VARCHAR,
  advisorUid VARCHAR,
  advisorFirstName VARCHAR,
  advisorLastName VARCHAR,
  overview VARCHAR,
  topics ARRAY<VARCHAR>,
  note VARCHAR(MAX),
  createdDate VARCHAR,
  lastModifiedDate VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '{e_i_advising_notes_path}';

--------------------------------------------------------------------
-- Internal schema
--------------------------------------------------------------------

DROP SCHEMA IF EXISTS {redshift_schema_e_i_advising_notes_internal} CASCADE;
CREATE SCHEMA {redshift_schema_e_i_advising_notes_internal};
GRANT USAGE ON SCHEMA {redshift_schema_e_i_advising_notes_internal} TO GROUP {redshift_app_boa_user}_group;
ALTER default PRIVILEGES IN SCHEMA {redshift_schema_e_i_advising_notes_internal} GRANT SELECT ON TABLES TO GROUP {redshift_app_boa_user}_group;
GRANT USAGE ON SCHEMA {redshift_schema_e_i_advising_notes_internal} TO GROUP {redshift_dblink_group};
ALTER DEFAULT PRIVILEGES IN SCHEMA {redshift_schema_e_i_advising_notes_internal} GRANT SELECT ON TABLES TO GROUP {redshift_dblink_group};

--------------------------------------------------------------------
-- Internal tables
--------------------------------------------------------------------

CREATE OR REPLACE FUNCTION {redshift_schema_e_i_advising_notes_internal}.to_utc_iso_string(date_string VARCHAR)
RETURNS VARCHAR
STABLE
AS $$
  from datetime import datetime
  import pytz

  d = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S')
  d = pytz.timezone('America/Los_Angeles').localize(d)
  return d.astimezone(pytz.utc).isoformat()
$$ language plpythonu;

GRANT EXECUTE
ON function {redshift_schema_e_i_advising_notes_internal}.to_utc_iso_string(VARCHAR)
TO GROUP {redshift_app_boa_user}_group;

CREATE TABLE {redshift_schema_e_i_advising_notes_internal}.advising_notes
SORTKEY (id)
AS (
    SELECT
      studentSid || '-' || id AS id,
      id AS e_i_id,
      studentSid AS sid,
      studentFirstName AS student_first_name,
      studentLastName AS student_last_name,
      meetingDate AS meeting_date,
      advisorUid AS advisor_uid,
      advisorFirstName AS advisor_first_name,
      advisorLastName AS advisor_last_name,
      overview AS overview,
      note AS note,
      TO_TIMESTAMP({redshift_schema_e_i_advising_notes_internal}.to_utc_iso_string(createdDate), 'YYYY-MM-DD"T"HH.MI.SS%z') AS created_at,
      TO_TIMESTAMP({redshift_schema_e_i_advising_notes_internal}.to_utc_iso_string(lastModifiedDate), 'YYYY-MM-DD"T"HH.MI.SS%z') AS updated_at
    FROM {redshift_schema_e_i_advising_notes}.advising_notes
);

CREATE TABLE {redshift_schema_e_i_advising_notes_internal}.advising_note_topics
SORTKEY (id)
AS (
    SELECT DISTINCT
      studentSid || '-' || id AS id,
      id AS e_i_id,
      studentSid AS sid,
      topic
    FROM {redshift_schema_e_i_advising_notes}.advising_notes
);

DROP FUNCTION {redshift_schema_e_i_advising_notes_internal}.to_utc_iso_string(VARCHAR);
