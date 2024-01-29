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


BEGIN TRANSACTION;

--
-- Student profile feed table
--

TRUNCATE {rds_schema_student}.student_profiles;

INSERT INTO {rds_schema_student}.student_profiles (
  SELECT *
      FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
          SELECT sid, profile, profile_summary
          FROM {redshift_schema_student}.student_profiles sp
    $REDSHIFT$)
  AS redshift_profiles (
      sid VARCHAR,
      profile TEXT,
      profile_summary TEXT
  )
);

--
-- Main student profile index table
--

TRUNCATE {rds_schema_student}.student_profile_index;

INSERT INTO {rds_schema_student}.student_profile_index
  (sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term, terms_in_attendance)
SELECT
  sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term, terms_in_attendance
FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
  SELECT *
  FROM {redshift_schema_student}.student_profile_index
$REDSHIFT$)
AS redshift_profile_index (
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
ON CONFLICT (sid) DO UPDATE SET
  sid=EXCLUDED.sid, uid=EXCLUDED.uid, first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name,
  level=EXCLUDED.level, gpa=EXCLUDED.gpa, units=EXCLUDED.units, transfer=EXCLUDED.transfer,
  expected_grad_term=EXCLUDED.expected_grad_term, terms_in_attendance=EXCLUDED.terms_in_attendance;

UPDATE {rds_schema_student}.student_profile_index spidx
  SET academic_career_status = lower(p.profile::json->'sisProfile'->>'academicCareerStatus')
  FROM {rds_schema_student}.student_profiles p
  WHERE spidx.sid = p.sid;

UPDATE {rds_schema_student}.student_profile_index spidx
  SET email_address = lower(p.profile::json->'sisProfile'->>'emailAddress')
  FROM {rds_schema_student}.student_profiles p
  WHERE spidx.sid = p.sid;

UPDATE {rds_schema_student}.student_profile_index spidx
  SET entering_term =
  substr(split_part(p.profile::json->'sisProfile'->>'matriculation', ' ', 2), 1, 1)
  ||
  substr(split_part(p.profile::json->'sisProfile'->>'matriculation', ' ', 2), 3, 2)
  ||
  CASE split_part(p.profile::json->'sisProfile'->>'matriculation', ' ', 1)
  WHEN 'Winter' THEN 0 WHEN 'Spring' THEN 2 WHEN 'Summer' THEN 5 WHEN 'Fall' THEN 8 END
  FROM {rds_schema_student}.student_profiles p
  WHERE p.sid = spidx.sid
  AND p.profile::json->'sisProfile'->>'matriculation' IS NOT NULL;


--
-- Main student profile index table
--


TRUNCATE {rds_schema_student}.academic_standing;

INSERT INTO {rds_schema_student}.academic_standing (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
      SELECT DISTINCT sid, term_id, acad_standing_action, acad_standing_status, LEFT(action_date, 10)
      FROM {redshift_schema_student}.academic_standing
    $REDSHIFT$)
  AS redshift_academic_standing (
      sid VARCHAR,
      term_id VARCHAR,
      acad_standing_action VARCHAR,
      acad_standing_status VARCHAR,
      action_date VARCHAR
  )
);

TRUNCATE {rds_schema_student}.demographics;

INSERT INTO {rds_schema_student}.demographics (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
      SELECT sid, gender, minority
      FROM {redshift_schema_student}.demographics
    $REDSHIFT$)
  AS redshift_demographics (
      sid VARCHAR,
      gender VARCHAR,
      minority BOOLEAN
  )
);

TRUNCATE {rds_schema_student}.ethnicities;

INSERT INTO {rds_schema_student}.ethnicities (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
      SELECT sid, ethnicity
      FROM {redshift_schema_student}.ethnicities
    $REDSHIFT$)
  AS redshift_ethnicities (
      sid VARCHAR,
      ethnicity VARCHAR
  )
);

TRUNCATE {rds_schema_student}.intended_majors;

INSERT INTO {rds_schema_student}.intended_majors (
  SELECT DISTINCT *
      FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
          SELECT sid, major
          FROM {redshift_schema_student}.intended_majors
    $REDSHIFT$)
  AS redshift_intended_majors (
      sid VARCHAR,
      major VARCHAR
  )
);

TRUNCATE {rds_schema_student}.minors;

INSERT INTO {rds_schema_student}.minors (
  SELECT DISTINCT *
      FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
          SELECT sid, minor
          FROM {redshift_schema_student}.minors
    $REDSHIFT$)
  AS redshift_minors (
      sid VARCHAR,
      minor VARCHAR
  )
);

TRUNCATE {rds_schema_student}.student_academic_programs;

INSERT INTO {rds_schema_student}.student_academic_programs (
  SELECT DISTINCT *
      FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
          SELECT sid, academic_career_code, academic_program_status_code, academic_program_status,
              academic_program_code, academic_program_name, effective_date
          FROM {redshift_schema_student}.student_academic_programs
      $REDSHIFT$)
  AS redshift_student_academic_programs (
      sid VARCHAR NOT NULL,
      academic_career_code VARCHAR,
      academic_program_status_code VARCHAR,
      academic_program_status VARCHAR,
      academic_program_code VARCHAR,
      academic_program_name VARCHAR,
      effective_date DATE
  )
);

TRUNCATE {rds_schema_student}.student_degrees;

INSERT INTO {rds_schema_student}.student_degrees
SELECT DISTINCT degrees.sid, plan, date_awarded,
  CASE substr(date_awarded, 6, 2)
    WHEN '03' THEN substr(date_awarded, 1, 1) || substr(date_awarded, 3, 2) || '0'
    WHEN '05' THEN substr(date_awarded, 1, 1) || substr(date_awarded, 3, 2) || '2'
    WHEN '06' THEN substr(date_awarded, 1, 1) || substr(date_awarded, 3, 2) || '2'
    WHEN '08' THEN substr(date_awarded, 1, 1) || substr(date_awarded, 3, 2) || '5'
    WHEN '12' THEN substr(date_awarded, 1, 1) || substr(date_awarded, 3, 2) || '8'
    END AS term_id
FROM (
  SELECT sid, "dateAwarded" AS date_awarded, plans.*
  FROM
  (
    SELECT sid, "dateAwarded", plans
    FROM {rds_schema_student}.student_profiles p,
    json_to_recordset(p.profile::json->'sisProfile'->'degrees') AS degrees("dateAwarded" varchar, "plans" varchar)
  ) p,
  json_to_recordset(plans::json) AS plans(plan varchar)
) degrees
LEFT JOIN student.student_profile_index spi on spi.sid = degrees.sid
ON CONFLICT (sid, plan, term_id) DO UPDATE SET
  sid=EXCLUDED.sid, plan=EXCLUDED.plan, date_awarded=EXCLUDED.date_awarded, term_id=EXCLUDED.term_id;

TRUNCATE {rds_schema_student}.student_holds;

INSERT INTO {rds_schema_student}.student_holds (
  SELECT *
      FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
          SELECT sid, feed
          FROM {redshift_schema_student}.student_holds
    $REDSHIFT$)
  AS redshift_holds (
      sid VARCHAR,
      feed TEXT
  )
);

TRUNCATE {rds_schema_student}.student_majors;

INSERT INTO {rds_schema_student}.student_majors (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
      SELECT DISTINCT sid, college, major, division
      FROM {redshift_schema_student}.student_majors
    $REDSHIFT$)
  AS redshift_majors (
      sid VARCHAR,
      college VARCHAR,
      major VARCHAR,
      division VARCHAR
  )
);

TRUNCATE {rds_schema_student}.student_names;

INSERT INTO {rds_schema_student}.student_names (
  SELECT DISTINCT sid, unnest(string_to_array(
      regexp_replace(upper(first_name), '[^\w ]', '', 'g'),
      ' '
  )) AS name FROM {rds_schema_student}.student_profile_index WHERE academic_career_status = 'active'
  UNION
  SELECT DISTINCT sid, unnest(string_to_array(
      regexp_replace(upper(last_name), '[^\w ]', '', 'g'),
      ' '
  )) AS name FROM {rds_schema_student}.student_profile_index WHERE academic_career_status = 'active'
);

COMMIT TRANSACTION;

TRUNCATE {rds_schema_student}.visas;

INSERT INTO {rds_schema_student}.visas (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
      SELECT sid, visa_status, visa_type
      FROM {redshift_schema_student}.visas
    $REDSHIFT$)
  AS redshift_visas (
      sid VARCHAR,
      visa_status VARCHAR,
      visa_type VARCHAR
  )
);
