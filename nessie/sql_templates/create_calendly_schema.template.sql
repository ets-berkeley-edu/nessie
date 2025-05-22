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

--------------------------------------------------------------------
-- CREATE EXTERNAL SCHEMA
--------------------------------------------------------------------

CREATE EXTERNAL SCHEMA {redshift_schema_calendly}
FROM data catalog
DATABASE '{redshift_schema_calendly}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

-- events
CREATE EXTERNAL TABLE {redshift_schema_calendly}.events(
    calendar_event STRUCT<id: VARCHAR, kind: VARCHAR>,
    start_time VARCHAR,
    end_time VARCHAR,
    meeting_notes_html VARCHAR,
    meeting_notes_plain VARCHAR,
    name VARCHAR,
    status VARCHAR,
    updated_at VARCHAR,
    uri VARCHAR,
    imported_at VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION '{loch_s3_calendly_data_path}/archive';

--------------------------------------------------------------------
-- Internal schema
--------------------------------------------------------------------

DROP SCHEMA IF EXISTS {redshift_schema_calendly_internal} CASCADE;
CREATE SCHEMA {redshift_schema_calendly_internal};
GRANT USAGE ON SCHEMA {redshift_schema_calendly_internal} TO GROUP {redshift_app_boa_user}_group;
ALTER default PRIVILEGES IN SCHEMA {redshift_schema_calendly_internal} GRANT SELECT ON TABLES TO GROUP {redshift_app_boa_user}_group;
GRANT USAGE ON SCHEMA {redshift_schema_calendly_internal} TO GROUP {redshift_dblink_group};
ALTER DEFAULT PRIVILEGES IN SCHEMA {redshift_schema_calendly_internal} GRANT SELECT ON TABLES TO GROUP {redshift_dblink_group};

--------------------------------------------------------------------
-- Internal tables
--------------------------------------------------------------------

CREATE OR REPLACE FUNCTION {redshift_schema_calendly_internal}.to_utc_iso_string(date_string VARCHAR)
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
ON function {redshift_schema_calendly_internal}.to_utc_iso_string(VARCHAR)
TO GROUP {redshift_app_boa_user}_group;

CREATE TABLE {redshift_schema_calendly_internal}.events
SORTKEY (id)
AS (
  SELECT
    e.id,
    NULL::VARCHAR(10) AS ldap_uid,
    e.name,
    TO_TIMESTAMP({redshift_schema_calendly_internal}.to_utc_iso_string(e.start_time), 'YYYY-MM-DD"T"HH.MI.SS%z') AS start_time,
    TO_TIMESTAMP({redshift_schema_calendly_internal}.to_utc_iso_string(e.end_time), 'YYYY-MM-DD"T"HH.MI.SS%z') AS end_time,
    e.meeting_notes_html,
    e.meeting_notes_plain,
    e.status,
    e.uri VARCHAR,
    MAX(e.imported_at) AS imported_at
  FROM {redshift_schema_calendly}.events e
  GROUP BY
    e.calendar_event.id, e.name, e.start_time, e.end_time, e.meeting_notes_html, e.meeting_notes_plain, e.status, e.uri
);

DROP FUNCTION {redshift_schema_calendly_internal}.to_utc_iso_string(VARCHAR);

UPDATE {redshift_schema_calendly_internal}.events
SET ldap_uid = ba.ldap_uid
FROM {redshift_schema_edl}.basic_attributes ba
  JOIN {redshift_schema_calendly_internal}.events b
  ON ba.sid = b.calendly_sid;

-- Second pass: try to fill in remaining UIDs from CalNet matches on email address.
UPDATE {redshift_schema_calendly_internal}.events
SET ldap_uid = ba.ldap_uid
FROM {redshift_schema_edl}.basic_attributes ba
  JOIN {redshift_schema_calendly_internal}.events b
  ON ba.email_address = b.calendly_student_email
  AND b.ldap_uid IS NULL;
