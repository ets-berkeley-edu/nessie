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
-- BEGIN: Create and Populate Redshift Internal Schema for BI Reports BOA Advising
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- CREATE REDSHIFT INTERNAL SCHEMA: {bi_redshift_schema_boa_advising}
----------------------------------------------------------------------------------------------------

DROP SCHEMA IF EXISTS {bi_redshift_schema_boa_advising} CASCADE;

CREATE SCHEMA {bi_redshift_schema_boa_advising};

GRANT USAGE ON SCHEMA {bi_redshift_schema_boa_advising} TO GROUP {bi_redshift_la_reports_dblink_group};

ALTER DEFAULT PRIVILEGES IN SCHEMA {bi_redshift_schema_boa_advising}
  GRANT SELECT ON TABLES TO GROUP {bi_redshift_la_reports_dblink_group};


----------------------------------------------------------------------------------------------------
-- CREATE TABLES in INTERNAL Schema for BI Reports BOA Advising
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : notes
-- Exclude BOA notes.body as it contains sensitive information, and any unnecessary/unused data.
-- Unpack author_dept_code array. Data only contains single depts per note_id.
----------------------------------------------------------------------------------------------------

CREATE TABLE {bi_redshift_schema_boa_advising}.notes AS
SELECT
  notes.id AS note_id,
  notes.created_at,
  CONVERT_TIMEZONE('PST8PDT', notes.created_at)::DATE AS created_at_date_pst,
  TO_CHAR(CONVERT_TIMEZONE('PST8PDT', notes.created_at), 'HH12:MI AM') AS created_at_time_pst,
  notes.set_date,
  notes.author_uid::INTEGER,
  notes.author_name AS note_author_name,
  dept AS author_dept_code,
  notes.author_role,
  notes.contact_type,
  notes.is_private,
  notes.sid::BIGINT,
  notes.subject
FROM {redshift_schema_boa_app_rds_data}.notes notes, notes.author_dept_codes as dept;


----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : authors
-- Authors used in notes: author_uid, author_name, author_aliases.
-- 409 distinct author_uids in {redshift_schema_boa_app_rds_data}.notes, but only 276 in {redshift_schema_advisor_internal}.advisor_attributes.
-- If advisor's first or last name is null then get name from most recently updated note.
-- Include list of author aliases composed of variations used in BOA notes.author_name.
-- Include advisors.
-- DO NOT use semicolon as list separator. resolve_sql_template in util.py is not happy with it.
----------------------------------------------------------------------------------------------------

CREATE TABLE {bi_redshift_schema_boa_advising}.authors AS
WITH note_auths AS (
  SELECT DISTINCT
    author_uid::INTEGER AS author_uid,
    LISTAGG(DISTINCT author_name, ' | ') WITHIN GROUP (ORDER BY updated_at DESC) AS author_aliases
  FROM {redshift_schema_boa_app_rds_data}.notes
  GROUP BY author_uid
)

SELECT
  note_auths.author_uid,
  advisors.last_name,
  advisors.first_name,
  COALESCE(
    advisors.first_name || ' ' || advisors.last_name,
    REGEXP_REPLACE(note_auths.author_aliases, ' [|]+.*$', '')) AS author_name,
  note_auths.author_aliases
FROM note_auths
LEFT JOIN {redshift_schema_advisor_internal}.advisor_attributes advisors
  ON note_auths.author_uid = advisors.ldap_uid;

--
-- For author_uids not in {redshift_schema_advisor_internal}.advisor_attributes,
-- extract most recent first_name and last_name from BOA notes.author_name.
-- Does not work correctly for last names containing spaces, e.g. unhyphenated compound surnames.
--

UPDATE {bi_redshift_schema_boa_advising}.authors
SET
  first_name = REGEXP_REPLACE(REGEXP_REPLACE(TRIM(author_name), ',.*$', ''), '^(.+) ([^ ]+)$', '$1'),
  last_name = REGEXP_REPLACE(REGEXP_REPLACE(TRIM(author_name), ',.*$', ''), '^(.+) ([^ ]+)$', '$2')
WHERE last_name IS NULL;


----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : departments
-- Department codes used in notes.
-- {redshift_schema_boa_app_rds_data}.university_depts only has 12 rows vs 85 dist dept_codes in notes table.
----------------------------------------------------------------------------------------------------

CREATE TABLE {bi_redshift_schema_boa_advising}.departments AS
SELECT DISTINCT
  n.author_dept_code as dept_code,
  u.dept_name
FROM {bi_redshift_schema_boa_advising}.notes n
LEFT JOIN {redshift_schema_boa_app_rds_data}.university_depts u ON n.author_dept_code = u.dept_code
WHERE n.author_dept_code is not null;


----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : note_topics
-- Join table of note_id to topic. Exclude deleted topics and dups. Orphaned topics will have null topic_id.
----------------------------------------------------------------------------------------------------
 
CREATE TABLE {bi_redshift_schema_boa_advising}.note_topics AS
SELECT
  id AS note_topic_id,
  note_id,
  topic,
  author_uid::INTEGER,
  deleted_at
FROM {redshift_schema_boa_app_rds_data}.note_topics;


----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : topics
----------------------------------------------------------------------------------------------------
 
CREATE TABLE {bi_redshift_schema_boa_advising}.topics AS
SELECT
  id AS topic_id,
  topic,
  created_at,
  deleted_at
FROM {redshift_schema_boa_app_rds_data}.topics;


----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : student_groups
-- Join table of student_group_id, student_group_name to sid.
----------------------------------------------------------------------------------------------------

CREATE TABLE {bi_redshift_schema_boa_advising}.student_groups AS
SELECT
  sg.id as student_group_id,
  sg.name as student_group_name,
  sgm.sid::BIGINT
FROM {redshift_schema_boa_app_rds_data}.student_group_members sgm
LEFT JOIN {redshift_schema_boa_app_rds_data}.student_groups sg ON sgm.student_group_id = sg.id;


----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : student_cohorts
-- Join table of cohort_id, cohort_name to sid.
----------------------------------------------------------------------------------------------------

CREATE TABLE {bi_redshift_schema_boa_advising}.student_cohorts AS
SELECT
  cohorts.id AS cohort_id,
  cohorts.name AS cohort_name,
  sid::BIGINT
FROM {redshift_schema_boa_app_rds_data}.cohort_filters cohorts, cohorts.sids AS sid;


----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : student_degrees
-- Join table of sid to degree_date, degree_awarded, plan_type, plan_group.
-- Students can have multiple degrees (major/minor, double major, etc).
-- May need to add index for degrees and plans to properly associate award date to degree/plan.
-- May not need plan_type, plan_group.
----------------------------------------------------------------------------------------------------

-- Setting to get data for camel case attribute names, e.g. "dateAwarded", otherwise returns null.
SET enable_case_sensitive_identifier TO TRUE;

CREATE TABLE {bi_redshift_schema_boa_advising}.student_degrees AS
WITH
distinct_sids AS (
  SELECT DISTINCT sid
  FROM {bi_redshift_schema_boa_advising}.notes
),

degrees_super AS (
  SELECT
    sp.sid,  
    JSON_PARSE(NULLIF(JSON_EXTRACT_PATH_TEXT(sp.profile, 'sisProfile', 'degrees', TRUE), '')) AS degrees_superdata
  FROM distinct_sids s
  JOIN student.student_profiles sp ON sp.sid = s.sid
),

degrees_data AS (
  SELECT
    ds.sid,
    CAST(degrees_unnested."dateAwarded" AS VARCHAR) AS degree_date,
    degrees_unnested.plans AS plans
  FROM degrees_super ds, ds.degrees_superdata AS degrees_unnested
)

SELECT
  d.sid::BIGINT,
  d.degree_date,
  CAST(plan.plan AS VARCHAR) AS degree_awarded,
  CAST(plan.type AS VARCHAR) AS plan_type,
  CAST(plan.group AS VARCHAR) AS plan_group
FROM degrees_data d, d.plans AS plan;


----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : students
-- Students used in notes.
-- Includes list of cohorts by sid, list of student groups by sid.
-- DO NOT use semicolon as list separator. resolve_sql_template in util.py is not happy with it.
-- TO DO: will need to handle students that do not have a record in student_profile_index.
----------------------------------------------------------------------------------------------------

CREATE TABLE {bi_redshift_schema_boa_advising}.students AS
WITH
distinct_sids AS (
  SELECT DISTINCT sid
  FROM {bi_redshift_schema_boa_advising}.notes
),

cohorts AS (
  SELECT
    distinct_sids.sid,
    LISTAGG(DISTINCT c.cohort_name, '|')
      WITHIN GROUP (ORDER BY c.cohort_name) AS cohort_list
  FROM distinct_sids
  LEFT JOIN {bi_redshift_schema_boa_advising}.student_cohorts c ON distinct_sids.sid = c.sid
  GROUP BY distinct_sids.sid
),

groups AS (
  SELECT
    distinct_sids.sid,
    LISTAGG(DISTINCT g.student_group_name, '|')
      WITHIN GROUP (ORDER BY g.student_group_name, g.student_group_id) AS group_list
  FROM distinct_sids
  LEFT JOIN {bi_redshift_schema_boa_advising}.student_groups g ON distinct_sids.sid = g.sid
  GROUP BY distinct_sids.sid
),

degrees AS (
  SELECT
    d.sid,
    LISTAGG(DISTINCT d.degree_awarded || COALESCE(' (' || d.degree_date || ')', ''), '|')
      WITHIN GROUP (ORDER BY d.degree_date, d.plan_type) AS degree_list
  FROM {bi_redshift_schema_boa_advising}.student_degrees d
  GROUP BY d.sid
)

SELECT
  distinct_sids.sid,
  student_profile_index.last_name AS last_name,
  student_profile_index.first_name AS first_name,
  student_profile_index.first_name || ' ' || student_profile_index.last_name AS student_name,
  cohorts.cohort_list,
  groups.group_list,
  degrees.degree_list
FROM distinct_sids
LEFT JOIN student.student_profile_index student_profile_index ON distinct_sids.sid = student_profile_index.sid
LEFT JOIN cohorts ON distinct_sids.sid = cohorts.sid
LEFT JOIN groups ON distinct_sids.sid = groups.sid
LEFT JOIN degrees ON distinct_sids.sid = degrees.sid;


----------------------------------------------------------------------------------------------------
-- END: Create and Populate Redshift Internal Schema for BI Reports BOA Advising
----------------------------------------------------------------------------------------------------
