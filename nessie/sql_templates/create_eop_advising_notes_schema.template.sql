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

--------------------------------------------------------------------
-- CREATE EXTERNAL SCHEMA
--------------------------------------------------------------------

CREATE EXTERNAL SCHEMA {redshift_schema_eop_advising_notes}
FROM data catalog
DATABASE '{redshift_schema_eop_advising_notes}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

CREATE EXTERNAL TABLE {redshift_schema_eop_advising_notes}.advising_notes
(
  advisor_name VARCHAR,
  advisor_uid VARCHAR,
  student_sid VARCHAR,
  student_name VARCHAR,
  overview VARCHAR,
  note VARCHAR(MAX),
  topic_1 VARCHAR,
  topic_2 VARCHAR,
  topic_3 VARCHAR,
  privacy_permissions VARCHAR,
  contact_method VARCHAR,
  meeting_date VARCHAR,
  attachment VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_eop_data_path}/historical/advising-notes'
TABLE PROPERTIES ('skip.header.line.count'='1', 'serialization.null.format'='');

--------------------------------------------------------------------
-- Internal schema
--------------------------------------------------------------------

DROP SCHEMA IF EXISTS {redshift_schema_eop_advising_notes_internal} CASCADE;
CREATE SCHEMA {redshift_schema_eop_advising_notes_internal};
GRANT USAGE ON SCHEMA {redshift_schema_eop_advising_notes_internal} TO GROUP {redshift_app_boa_user}_group;
ALTER default PRIVILEGES IN SCHEMA {redshift_schema_eop_advising_notes_internal} GRANT SELECT ON TABLES TO GROUP {redshift_app_boa_user}_group;
GRANT USAGE ON SCHEMA {redshift_schema_eop_advising_notes_internal} TO GROUP {redshift_dblink_group};
ALTER DEFAULT PRIVILEGES IN SCHEMA {redshift_schema_eop_advising_notes_internal} GRANT SELECT ON TABLES TO GROUP {redshift_dblink_group};

--------------------------------------------------------------------
-- Internal tables
--------------------------------------------------------------------

CREATE OR REPLACE FUNCTION {redshift_schema_eop_advising_notes_internal}.to_utc_iso_string(date_string VARCHAR)
RETURNS VARCHAR
STABLE
AS $$
  from datetime import datetime
  import pytz

  d = datetime.strptime(date_string, '%m/%d/%Y %H:%M:%S')
  d = pytz.timezone('America/Los_Angeles').localize(d)
  return d.astimezone(pytz.utc).isoformat()
$$ language plpythonu;

GRANT EXECUTE
ON function {redshift_schema_eop_advising_notes_internal}.to_utc_iso_string(VARCHAR)
TO GROUP {redshift_app_boa_user}_group;

CREATE TABLE {redshift_schema_eop_advising_notes_internal}.advising_notes
SORTKEY (sid)
AS (
    SELECT
      'eop_advising_note_' || (ROW_NUMBER() OVER (ORDER BY meeting_date)) AS id,
      student_sid AS sid,
      student_name,
      advisor_uid,
      split_part(advisor_name, ' ', 1) AS advisor_first_name,
      split_part(advisor_name, ' ', 2) AS advisor_last_name,
      privacy_permissions,
      contact_method,
      overview,
      note,
      meeting_date,
      attachment,
      ARRAY(topic_1, topic_2, topic_3) AS topics,
      TO_TIMESTAMP({redshift_schema_eop_advising_notes_internal}.to_utc_iso_string(meeting_date || ' 12:00:00'), 'YYYY-MM-DD"T"HH.MI.SS%z') AS created_at
    FROM {redshift_schema_eop_advising_notes}.advising_notes
);

CREATE TABLE {redshift_schema_eop_advising_notes_internal}.advising_note_topics
SORTKEY (sid)
AS (
    SELECT DISTINCT
      n.id,
      n.sid,
      t::VARCHAR AS topic
    FROM {redshift_schema_eop_advising_notes_internal}.advising_notes AS n
    JOIN n.topics AS t ON TRUE
    WHERE t IS NOT NULL
);

DROP FUNCTION {redshift_schema_eop_advising_notes_internal}.to_utc_iso_string(VARCHAR);
