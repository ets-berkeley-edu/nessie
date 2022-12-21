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

CREATE EXTERNAL SCHEMA {redshift_schema_ycbm}
FROM data catalog
DATABASE '{redshift_schema_ycbm}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

-- bookings
CREATE EXTERNAL TABLE {redshift_schema_ycbm}.bookings(
      id VARCHAR,
      title VARCHAR,
      startsAt VARCHAR,
      endsAt VARCHAR,
      cancelled BOOLEAN,
      cancellationReason CHAR(max),
      teamMember STRUCT<id: VARCHAR, name: VARCHAR, email: VARCHAR>,
      answers STRUCT<sid: VARCHAR, email: VARCHAR, fname: VARCHAR, q5: CHAR(max), q6: CHAR(max)>,
      importedAt VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION '{loch_s3_ycbm_data_path}/archive';

--------------------------------------------------------------------
-- Internal schema
--------------------------------------------------------------------

DROP SCHEMA IF EXISTS {redshift_schema_ycbm_internal} CASCADE;
CREATE SCHEMA {redshift_schema_ycbm_internal};
GRANT USAGE ON SCHEMA {redshift_schema_ycbm_internal} TO GROUP {redshift_app_boa_user}_group;
ALTER default PRIVILEGES IN SCHEMA {redshift_schema_ycbm_internal} GRANT SELECT ON TABLES TO GROUP {redshift_app_boa_user}_group;
GRANT USAGE ON SCHEMA {redshift_schema_ycbm_internal} TO GROUP {redshift_dblink_group};
ALTER DEFAULT PRIVILEGES IN SCHEMA {redshift_schema_ycbm_internal} GRANT SELECT ON TABLES TO GROUP {redshift_dblink_group};

--------------------------------------------------------------------
-- Internal tables
--------------------------------------------------------------------

CREATE OR REPLACE FUNCTION {redshift_schema_ycbm_internal}.to_utc_iso_string(date_string VARCHAR)
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
ON function {redshift_schema_ycbm_internal}.to_utc_iso_string(VARCHAR)
TO GROUP {redshift_app_boa_user}_group;

CREATE TABLE {redshift_schema_ycbm_internal}.bookings
SORTKEY (id)
AS (
  SELECT
    t.id,
    NULL::VARCHAR(10) AS ldap_uid,
    t.answers.sid AS ycbm_sid, t.answers.email AS ycbm_student_email, t.answers.fname AS ycbm_student_name,
    t.title,
    TO_TIMESTAMP({redshift_schema_ycbm_internal}.to_utc_iso_string(t.startsat), 'YYYY-MM-DD"T"HH.MI.SS%z') AS starts_at,
    TO_TIMESTAMP({redshift_schema_ycbm_internal}.to_utc_iso_string(t.endsat), 'YYYY-MM-DD"T"HH.MI.SS%z') AS ends_at,
    t.cancelled, t.cancellationreason AS cancellation_reason,
    t.teammember.id AS advisor_id, t.teammember.name AS advisor_name, t.teammember.email AS advisor_email,
    t.answers.q5 AS q5, t.answers.q6 AS q6,
    MAX(t.importedat) AS imported_at
  FROM {redshift_schema_ycbm}.bookings t
  GROUP BY
    t.id, t.title, t.startsat, t.endsat, t.cancelled, t.cancellationreason,
    t.teammember.id, t.teammember.name, t.teammember.email,
    t.answers.sid, t.answers.email, t.answers.fname, t.answers.q5, t.answers.q6
);

DROP FUNCTION {redshift_schema_ycbm_internal}.to_utc_iso_string(VARCHAR);

-- First pass: fill in UIDs from CalNet matches on SID.
UPDATE {redshift_schema_ycbm_internal}.bookings
SET ldap_uid = ba.ldap_uid
FROM {redshift_schema_edl}.basic_attributes ba
  JOIN {redshift_schema_ycbm_internal}.bookings b
  ON ba.sid = b.ycbm_sid;

-- Second pass: try to fill in remaining UIDs from CalNet matches on email address.
UPDATE {redshift_schema_ycbm_internal}.bookings
SET ldap_uid = ba.ldap_uid
FROM {redshift_schema_edl}.basic_attributes ba
  JOIN {redshift_schema_ycbm_internal}.bookings b
  ON ba.email_address = b.ycbm_student_email
  AND b.ldap_uid IS NULL;
