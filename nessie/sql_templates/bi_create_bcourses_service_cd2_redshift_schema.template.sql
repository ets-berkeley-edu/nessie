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
-- BEGIN script for creating/populating REDSHIFT schema/tables for bCourses Service (CD2) Dashboard
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- CREATE nessie INTERNAL REDSHIFT SCHEMA: "{bi_redshift_schema_bcourses_service_cd2}"
----------------------------------------------------------------------------------------------------

DROP SCHEMA IF EXISTS {bi_redshift_schema_bcourses_service_cd2} CASCADE;

CREATE SCHEMA {bi_redshift_schema_bcourses_service_cd2};

GRANT USAGE ON SCHEMA {bi_redshift_schema_bcourses_service_cd2} TO GROUP {bi_redshift_la_reports_dblink_group};

ALTER DEFAULT PRIVILEGES IN SCHEMA {bi_redshift_schema_bcourses_service_cd2}
  GRANT SELECT ON TABLES TO GROUP {bi_redshift_la_reports_dblink_group};


----------------------------------------------------------------------------------------------------
-- CREATE TABLES in INTERNAL Schema for BI Reports: bCourses Service Dashboard 
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : "bcourses_accounts"
----------------------------------------------------------------------------------------------------

CREATE TABLE {bi_redshift_schema_bcourses_service_cd2}.bcourses_accounts AS
  WITH p_account AS (
    SELECT id AS p_id, name AS p_name
    FROM {redshift_schema_canvas_data_2}.accounts
    WHERE name = 'Official Courses'
  )
  SELECT
    a.id AS account_id,
    a.name,
    p.p_id AS parent_account_id,
    p.p_name AS parent_account,
    a.sis_source_id,
    a.workflow_state
FROM {redshift_schema_canvas_data_2}.accounts a
JOIN p_account p ON a.parent_account_id = p.p_id
WHERE a.workflow_state <> 'deleted';


----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : "canvas_courses"
----------------------------------------------------------------------------------------------------

CREATE TABLE {bi_redshift_schema_bcourses_service_cd2}.canvas_courses AS
  WITH
  courses AS (
    SELECT
      cd2c.id AS course_id,
      cd2c.account_id,
      cd2c.enrollment_term_id,
      cd2c.name,
      cd2c.course_code AS code,
      cd2c.created_at::DATE AS created_at,
      cd2c.start_at::DATE AS start_date,
      cd2c.conclude_at::DATE AS end_date,
      cd2c.sis_source_id,
      cd2c.is_public AS publicly_visible,
      cd2c.workflow_state,
      cd2c.wiki_id,
      CASE WHEN cd2c.syllabus_body IS NULL THEN NULL ELSE 'yes' END AS syllabus
    FROM {redshift_schema_canvas_data_2}.courses cd2c
    JOIN {bi_redshift_schema_bcourses_service_cd2}.bcourses_accounts a ON (cd2c.account_id = a.account_id)
    WHERE cd2c.workflow_state <> 'deleted'
  ),
  quizzes AS (
    SELECT DISTINCT context_id AS course_id, 'yes' AS quizzes
    FROM {redshift_schema_canvas_data_2}.quizzes 
  ),
  announcements AS (
    SELECT DISTINCT context_id AS course_id, 'yes' AS announcements
    FROM {redshift_schema_canvas_data_2}.discussion_topics
    WHERE type = 'Announcement' 
  ),
  discussion_topics AS (
    SELECT DISTINCT context_id AS course_id, 'yes' AS discussion_topics
    FROM {redshift_schema_canvas_data_2}.discussion_topics
    WHERE type IS NULL
  ),
  files AS (
    SELECT DISTINCT context_id AS course_id, 'yes' AS files
    FROM {redshift_schema_canvas_data_2}.attachments
  ),
  assignments AS (
    SELECT DISTINCT context_id AS course_id, 'yes' AS assignments
    FROM {redshift_schema_canvas_data_2}.assignments
  ),
  modules AS (
    SELECT DISTINCT context_id AS course_id, 'yes' AS modules
    FROM {redshift_schema_canvas_data_2}.context_modules
  ),
  pages AS (
    SELECT DISTINCT context_id AS course_id, 'yes' AS pages
    FROM {redshift_schema_canvas_data_2}.wiki_pages
  ),
  gradebook AS (
    SELECT DISTINCT e.course_id, 'yes' AS gradebook
    FROM {redshift_schema_canvas_data_2}.scores s
    JOIN {redshift_schema_canvas_data_2}.enrollments e ON s.enrollment_id = e.id
  )
  SELECT
    c.course_id,
    c.account_id,
    c.enrollment_term_id,
    c.name,
    c.code,
    c.created_at,
    c.start_date,
    c.end_date,
    c.sis_source_id,
    c.publicly_visible,
    c.workflow_state,
    c.wiki_id,
    c.syllabus,
    q.quizzes,
    anno.announcements,
    d.discussion_topics,
    f.files,
    assn.assignments,
    m.modules,
    p.pages,
    g.gradebook
  FROM courses c
  LEFT OUTER JOIN quizzes q ON c.course_id = q.course_id
  LEFT OUTER JOIN announcements anno ON c.course_id = anno.course_id
  LEFT OUTER JOIN discussion_topics d ON c.course_id = d.course_id
  LEFT OUTER JOIN files f ON c.course_id = f.course_id
  LEFT OUTER JOIN assignments assn ON c.course_id = assn.course_id
  LEFT OUTER JOIN modules m ON c.course_id = m.course_id
  LEFT OUTER JOIN pages p ON c.course_id = p.course_id
  LEFT OUTER JOIN gradebook g ON c.course_id = g.course_id;


----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : "bcourses_assignments"
-- check for 'quiz', 'discussion_topic', 'non quiz'
--   via cd2_ext_dev.assignments.submission_types, which is formatted as json
--   ["discussion_topic"], "online_quiz"
----------------------------------------------------------------------------------------------------

CREATE TABLE {bi_redshift_schema_bcourses_service_cd2}.bcourses_assignments AS
  WITH
  subtypes AS (
    SELECT
      id,
      CASE
        WHEN submission_types ~ 'discussion_topics' THEN 'discussion_topics'
        WHEN submission_types ~ 'online_quiz' THEN 'quiz' 
        ELSE 'non quiz' END AS sub_type
    FROM {redshift_schema_canvas_data_2}.assignments
    WHERE workflow_state <> 'deleted'
  )
  SELECT
    a.id AS assignment_id,
    a.context_id AS course_id,
    c.enrollment_term_id,
    a.title,
    a.created_at,
    a.created_at AS corrected_created_at,
    a.updated_at,
    a.due_at,
    a.points_possible,
    a.grading_type,
    a.submission_types,
    a.workflow_state,
    DECODE(a.only_visible_to_overrides, TRUE, 'only_visible_to_overrides', 'everyone') AS visibility,
    ct.content_id AS external_tool_id,
    DECODE(s.sub_type, 'quiz', 'quiz', NULL) AS quizzes,
    DECODE(s.sub_type, 'discussion_topics', 'discussion_topics', NULL) AS discussion_topics,
    DECODE(s.sub_type, 'non quiz', 'non quiz', NULL) AS non_quiz_assignments
  FROM {redshift_schema_canvas_data_2}.assignments a
  JOIN {bi_redshift_schema_bcourses_service_cd2}.canvas_courses c ON a.context_id = c.course_id
  LEFT OUTER JOIN {redshift_schema_canvas_data_2}.content_tags ct ON (
    a.id = ct.context_id
    AND ct.context_type = 'Assignment'
    AND ct.content_type = 'ContextExternalTool'
    AND ct.workflow_state = 'active')
  LEFT OUTER JOIN subtypes s ON a.id = s.id
  WHERE a.workflow_state <> 'deleted';


----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : "bcourses_enrollment_terms"
-- Set padded start_at and end_at dates to -1 and +1 month from term start and end dates as follows
--   Fall: set start_at = 7/20 + term_year, end_at = 1/21 + (term_year + 1)
--   Spring: set start_at = 12/15 + (term_year - 1), end_at = 6/16 + term_year
--   Summer: set start_at = 4/26 + term_year, end_at = 9/16 + term_year
-- updated regex for four digit year to not user curly braces
----------------------------------------------------------------------------------------------------

CREATE TABLE {bi_redshift_schema_bcourses_service_cd2}.bcourses_enrollment_terms AS
  WITH
  termyear AS (
    SELECT
      id,
      SPLIT_PART(name,' ', 2) AS year,
      SPLIT_PART(name,' ', 1) AS term
    FROM {redshift_schema_canvas_data_2}.enrollment_terms
  )
  SELECT
    et.id AS enrollment_term_id,
    ty.year || ' ' || ty.term as term_name,
    ty.year,
    CASE WHEN ty.term IN ('Fall', 'Spring', 'Summer') THEN ty.term ELSE NULL END AS term,
    CASE
      WHEN ty.term = 'Fall' THEN ty.year || '-' || SUBSTRING((ty.year::int + 1)::text, 3, 2)
      WHEN ty.term IN ('Spring', 'Summer') THEN (ty.year::int - 1)::text || '-' || SUBSTRING(ty.year, 3, 2)
      ELSE NULL END AS academic_year,
    et.sis_source_id,
    et.start_at AS date_start,
    CASE
      WHEN ty.term = 'Fall' THEN TO_DATE(ty.year ||'0720', 'YYYYMMDD')
      WHEN ty.term = 'Spring' THEN DATEADD(YEAR, -1, TO_DATE(ty.year ||'1215', 'YYYYMMDD'))::DATE
      WHEN ty.term = 'Summer' THEN TO_DATE(ty.year ||'0426', 'YYYYMMDD')
    END AS date_start_range_padded,
    CASE
      WHEN ty.term = 'Fall' THEN DATEADD(YEAR, 1, TO_DATE(ty.year ||'0121', 'YYYYMMDD'))::DATE
      WHEN ty.term = 'Spring' THEN TO_DATE(ty.year ||'0616', 'YYYYMMDD')
      WHEN ty.term = 'Summer' THEN TO_DATE(ty.year ||'0916', 'YYYYMMDD')
    END AS date_end_range_padded
  FROM {redshift_schema_canvas_data_2}.enrollment_terms et
  JOIN termyear ty ON et.id = ty.id
  WHERE et.workflow_state <> 'deleted'
  AND ty.year ~ '[0-9][0-9][0-9][0-9]';


----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : "discussion_posts_daily_agg"
----------------------------------------------------------------------------------------------------

CREATE TABLE {bi_redshift_schema_bcourses_service_cd2}.discussion_posts_daily_agg AS
  SELECT
    et.year AS term_year,
    et.term,
    TRIM(et.year || ' ' || et.term) AS term_name,
    c.enrollment_term_id,
    a.name AS department,
    c.course_id,
    de.created_at::DATE AS corrected_created_at,
    count(de.id) AS discussion_entry_count
  FROM {redshift_schema_canvas_data_2}.discussion_entries de
  JOIN {redshift_schema_canvas_data_2}.discussion_topics dt ON de.discussion_topic_id = dt.id
  JOIN {bi_redshift_schema_bcourses_service_cd2}.canvas_courses c ON dt.context_id = c.course_id
  JOIN {bi_redshift_schema_bcourses_service_cd2}.bcourses_accounts a ON c.account_id = a.account_id
  JOIN {bi_redshift_schema_bcourses_service_cd2}.bcourses_enrollment_terms et ON c.enrollment_term_id = et.enrollment_term_id
  WHERE de.workflow_state <> 'deleted'
  GROUP BY
    et.year,
    et.term,
    et.term_name,
    c.enrollment_term_id,
    a.name,
    c.course_id,
    de.created_at::DATE;


----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : "discussion_topics_daily_agg"
----------------------------------------------------------------------------------------------------

CREATE TABLE {bi_redshift_schema_bcourses_service_cd2}.discussion_topics_daily_agg AS
  SELECT
    et.year AS term_year,
    et.term,
    TRIM(et.year || ' ' || et.term) AS term_name,
    c.enrollment_term_id,
    a.name AS department,
    c.course_id,
    dt.type AS announcements,
    CASE WHEN dt.type IS NULL then 'discussion_topic' END AS discussion_topic,
    dt.created_at::DATE AS corrected_created_at,
    count(dt.id) AS discussion_topic_count
  FROM {redshift_schema_canvas_data_2}.discussion_topics dt
  JOIN {bi_redshift_schema_bcourses_service_cd2}.canvas_courses c ON dt.context_id = c.course_id
  JOIN {bi_redshift_schema_bcourses_service_cd2}.bcourses_accounts a ON c.account_id = a.account_id
  JOIN {bi_redshift_schema_bcourses_service_cd2}.bcourses_enrollment_terms et ON c.enrollment_term_id = et.enrollment_term_id
  WHERE dt.workflow_state <> 'deleted'
  GROUP BY
    et.year,
    et.term,
    et.term_name,
    c.enrollment_term_id,
    dt.type,
    a.name,
    c.course_id,
    CASE WHEN dt.type IS NULL then 'discussion_topic' END,
    dt.created_at::DATE;


----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : "enrollment_agg_by_role"
----------------------------------------------------------------------------------------------------

CREATE TABLE {bi_redshift_schema_bcourses_service_cd2}.enrollment_agg_by_role AS
  SELECT
    et.year AS term_year,
    et.term,
    TRIM(et.year || ' ' || et.term) AS term_name,
    c.enrollment_term_id,
    c.course_id,
    r.base_role_type AS most_privileged_role,
    count(e.id) AS enrollment_count
  FROM {redshift_schema_canvas_data_2}.enrollments e
  JOIN {redshift_schema_canvas_data_2}.roles r ON e.role_id = r.id
  JOIN {bi_redshift_schema_bcourses_service_cd2}.canvas_courses c ON e.course_id = c.course_id
  JOIN {bi_redshift_schema_bcourses_service_cd2}.bcourses_enrollment_terms et ON c.enrollment_term_id = et.enrollment_term_id
  WHERE e.workflow_state <> 'deleted'
  GROUP BY
    et.year,
    et.term,
    et.term_name,
    c.enrollment_term_id,
    c.course_id,
    r.base_role_type;


----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : "file_upload_daily_agg"
----------------------------------------------------------------------------------------------------

CREATE TABLE {bi_redshift_schema_bcourses_service_cd2}.file_upload_daily_agg AS
  SELECT
    et.year AS term_year,
    et.term,
    TRIM(et.year || ' ' || et.term) AS term_name,
    c.enrollment_term_id,
    ba.name AS department,
    a.context_type AS owner_entity_type,
    c.course_id,
    a.created_at::DATE AS corrected_created_at,
    count(a.id) AS file_upload_count
  FROM {redshift_schema_canvas_data_2}.attachments a
  JOIN {bi_redshift_schema_bcourses_service_cd2}.canvas_courses c ON a.context_id = c.course_id
  JOIN {bi_redshift_schema_bcourses_service_cd2}.bcourses_accounts ba ON c.account_id = ba.account_id
  JOIN {bi_redshift_schema_bcourses_service_cd2}.bcourses_enrollment_terms et ON c.enrollment_term_id = et.enrollment_term_id
  WHERE a.workflow_state <> 'deleted'
  GROUP BY
    et.year,
    et.term,
    et.term_name,
    c.enrollment_term_id,
    ba.name,
    a.context_type,
    c.course_id,
    a.created_at::DATE;


----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : "submission_daily_agg"
----------------------------------------------------------------------------------------------------

CREATE TABLE {bi_redshift_schema_bcourses_service_cd2}.submission_daily_agg AS
  SELECT
    et.year AS term_year,
    et.term,
    TRIM(et.year || ' ' || et.term) AS term_name,
    c.enrollment_term_id,
    ba.name AS department,
    a.submission_types AS submission_type,
    a.workflow_state,
    a.course_id,
    a.created_at::DATE AS corrected_created_at,
    count(a.assignment_id) AS submission_count
  FROM {bi_redshift_schema_bcourses_service_cd2}.bcourses_assignments a
  JOIN {bi_redshift_schema_bcourses_service_cd2}.canvas_courses c ON a.course_id = c.course_id
  JOIN {bi_redshift_schema_bcourses_service_cd2}.bcourses_accounts ba ON c.account_id = ba.account_id
  JOIN {bi_redshift_schema_bcourses_service_cd2}.bcourses_enrollment_terms et ON c.enrollment_term_id = et.enrollment_term_id
  WHERE a.workflow_state <> 'deleted'
  GROUP BY
    et.year,
    et.term,
    et.term_name,
    c.enrollment_term_id,
    ba.name,
    a.submission_types,
    a.workflow_state,
    a.course_id,
    a.created_at::DATE;
  
----------------------------------------------------------------------------------------------------
-- END OF TABLES
----------------------------------------------------------------------------------------------------
