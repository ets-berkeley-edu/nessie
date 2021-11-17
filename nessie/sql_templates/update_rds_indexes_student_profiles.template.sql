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

DELETE FROM {rds_schema_student}.student_profile_index WHERE hist_enr IS FALSE;

INSERT INTO {rds_schema_student}.student_profile_index
  (sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term, terms_in_attendance,
   hist_enr)
SELECT
  sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term, terms_in_attendance,
  FALSE
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
  expected_grad_term=EXCLUDED.expected_grad_term, terms_in_attendance=EXCLUDED.terms_in_attendance,
  hist_enr=FALSE;

UPDATE {rds_schema_student}.student_profile_index spidx
  SET email_address = lower(p.profile::json->'sisProfile'->>'emailAddress')
  FROM {rds_schema_student}.student_profiles p
  WHERE spidx.sid = p.sid
  AND spidx.hist_enr IS FALSE;

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
  AND p.profile::json->'sisProfile'->>'matriculation' IS NOT NULL
  AND spidx.hist_enr IS FALSE;

UPDATE {rds_schema_student}.student_profile_index spidx
  SET academic_career_status = lower(p.profile::json->'sisProfile'->>'academicCareerStatus')
  FROM {rds_schema_student}.student_profiles p
  WHERE spidx.sid = p.sid
  AND spidx.hist_enr IS FALSE;

DELETE FROM {rds_schema_student}.student_degrees WHERE hist_enr IS FALSE;

INSERT INTO {rds_schema_student}.student_degrees
SELECT sid, plan, date_awarded,
CASE substr(date_awarded, 6, 2)
  WHEN '03' THEN substr(date_awarded, 1, 1) || substr(date_awarded, 3, 2) || '0'
  WHEN '05' THEN substr(date_awarded, 1, 1) || substr(date_awarded, 3, 2) || '2'
  WHEN '06' THEN substr(date_awarded, 1, 1) || substr(date_awarded, 3, 2) || '2'
  WHEN '08' THEN substr(date_awarded, 1, 1) || substr(date_awarded, 3, 2) || '5'
  WHEN '12' THEN substr(date_awarded, 1, 1) || substr(date_awarded, 3, 2) || '8'
  END AS term_id,
  FALSE AS hist_enr
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
ON CONFLICT (sid, plan, term_id) DO UPDATE SET
  sid=EXCLUDED.sid, plan=EXCLUDED.plan, date_awarded=EXCLUDED.date_awarded, term_id=EXCLUDED.term_id,
  hist_enr=FALSE;
