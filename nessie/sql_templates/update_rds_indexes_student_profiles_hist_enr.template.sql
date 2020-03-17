/**
 * Copyright ©2020. The Regents of the University of California (Regents). All Rights Reserved.
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

DROP TABLE IF EXISTS {rds_schema_student}.student_profiles_hist_enr CASCADE;
CREATE TABLE IF NOT EXISTS {rds_schema_student}.student_profiles_hist_enr
(
    sid VARCHAR NOT NULL PRIMARY KEY,
    uid VARCHAR,
    profile TEXT NOT NULL
);

INSERT INTO {rds_schema_student}.student_profiles_hist_enr (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT *
    FROM {redshift_schema_student}.student_profiles_hist_enr
  $REDSHIFT$)
  AS redshift_profiles (
    sid VARCHAR,
    uid VARCHAR,
    profile TEXT
  )
);

DELETE FROM {rds_schema_student}.student_profile_index WHERE hist_enr IS TRUE;

INSERT INTO {rds_schema_student}.student_profile_index
  (sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term, terms_in_attendance,
   hist_enr)
SELECT
  sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term, terms_in_attendance,
  TRUE
FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
  SELECT *
  FROM {redshift_schema_student}.student_profile_index_hist_enr
$REDSHIFT$)
AS redshift_profile_index_hist_enr (
  sid VARCHAR,
  uid VARCHAR,
  first_name VARCHAR,
  last_name VARCHAR,
  level VARCHAR,
  gpa NUMERIC,
  units NUMERIC,
  transfer BOOLEAN,
  expected_grad_term VARCHAR,
  terms_in_attendance INT
)
ON CONFLICT (sid) DO NOTHING;

UPDATE {rds_schema_student}.student_profile_index spidx
  SET email_address = lower(p.profile::json->'sisProfile'->>'emailAddress')
  FROM {rds_schema_student}.student_profiles_hist_enr p
  WHERE spidx.sid = p.sid
  AND spidx.hist_enr IS TRUE;

UPDATE {rds_schema_student}.student_profile_index spidx
  SET entering_term =
  substr(split_part(p.profile::json->'sisProfile'->>'matriculation', ' ', 2), 1, 1)
  ||
  substr(split_part(p.profile::json->'sisProfile'->>'matriculation', ' ', 2), 3, 2)
  ||
  CASE split_part(p.profile::json->'sisProfile'->>'matriculation', ' ', 1)
  WHEN 'Winter' THEN 0 WHEN 'Spring' THEN 2 WHEN 'Summer' THEN 5 WHEN 'Fall' THEN 8 END
  FROM {rds_schema_student}.student_profiles_hist_enr p
  WHERE p.sid = spidx.sid
  AND p.profile::json->'sisProfile'->>'matriculation' IS NOT NULL
  AND spidx.hist_enr IS TRUE;

UPDATE {rds_schema_student}.student_profile_index spidx
  SET academic_career_status = lower(p.profile::json->'sisProfile'->>'academicCareerStatus')
  FROM {rds_schema_student}.student_profiles_hist_enr p
  WHERE spidx.sid = p.sid
  AND spidx.hist_enr IS TRUE;

DROP TABLE IF EXISTS {rds_schema_student}.student_enrollment_terms_hist_enr CASCADE;
CREATE TABLE IF NOT EXISTS {rds_schema_student}.student_enrollment_terms_hist_enr
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    enrollment_term TEXT NOT NULL,
    PRIMARY KEY (sid, term_id)
);

INSERT INTO {rds_schema_student}.student_enrollment_terms_hist_enr (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT *
    FROM {redshift_schema_student}.student_enrollment_terms_hist_enr
  $REDSHIFT$)
  AS redshift_profiles (
    sid VARCHAR,
    term_id VARCHAR,
    enrollment_term TEXT
  )
);

DROP TABLE IF EXISTS {rds_schema_student}.student_names_hist_enr CASCADE;
CREATE TABLE IF NOT EXISTS {rds_schema_student}.student_names_hist_enr
(
    sid VARCHAR NOT NULL PRIMARY KEY,
    uid VARCHAR,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL
);

INSERT INTO {rds_schema_student}.student_names_hist_enr (
  SELECT DISTINCT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT sid, uid, first_name, last_name
    FROM {redshift_schema_student}.student_names_hist_enr
  $REDSHIFT$)
  AS redshift_names (
    sid VARCHAR,
    uid VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR
  )
);

DROP TABLE IF EXISTS {rds_schema_student}.student_name_index_hist_enr CASCADE;
CREATE TABLE IF NOT EXISTS {rds_schema_student}.student_name_index_hist_enr
(
    sid VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    PRIMARY KEY (sid, name)
);

INSERT INTO {rds_schema_student}.student_name_index_hist_enr (
  SELECT sid, unnest(string_to_array(
      regexp_replace(upper(first_name), '[^\w ]', '', 'g'),
      ' '
  )) AS name FROM {rds_schema_student}.student_names_hist_enr
  UNION
  SELECT sid, unnest(string_to_array(
      regexp_replace(upper(last_name), '[^\w ]', '', 'g'),
      ' '
  )) AS name FROM {rds_schema_student}.student_names_hist_enr
);
