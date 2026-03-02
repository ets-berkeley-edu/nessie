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
 * AS IS. REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
 * ENHANCEMENTS, OR MODIFICATIONS.
 */


----------------------------------------------------------------------------------------------------
-- BEGIN script for creating/populating REDSHIFT schema/tables for bCourses Usage Dashboard
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- CREATE nessie INTERNAL REDSHIFT SCHEMA
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- INTERNAL TABLE : student_course_activity_post_completion
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {bi_rds_schema_bcourses_service_cd2}.student_course_activity_post_completion;

/*
 * This query builds a table with a row for each Canvas course site and then calculates the percent
 * of enrolled students who have accessed/viewed the site within 30/60/90 days of the end date.
 */

CREATE TABLE {bi_rds_schema_bcourses_service_cd2}.student_course_activity_post_completion AS
/*
 * Not all courses have an end date. If a course site does not have an end date listed, we build the
 * TermDefaults table to identify the most common end date for other courses in that term.
 */
WITH TermDefaults AS (
    SELECT sis_term_id, meeting_end_date
    FROM (
        SELECT
            sis_term_id,
            meeting_end_date,
            ROW_NUMBER() OVER (PARTITION BY sis_term_id ORDER BY COUNT(*) DESC) as rank
        FROM {redshift_schema_intermediate}.sis_sections
        WHERE meeting_end_date IS NOT NULL
        GROUP BY sis_term_id, meeting_end_date
    )
    WHERE rank = 1
),
/*
 * CourseDates gets the meeting end date for courses that do have that value listed. If a course site
 * is linked to multiple sections with different meeting end dates, we get the latest/max date.
 */
CourseDates AS (
    SELECT
        cs.canvas_course_id,
        MAX(cs.sis_term_id) AS best_sis_term_id,
        MAX(ss.meeting_end_date) AS max_meeting_end_date
    FROM {redshift_schema_intermediate}.course_sections cs
    LEFT JOIN {redshift_schema_intermediate}.sis_sections ss
        ON cs.sis_section_id = ss.sis_section_id
        AND cs.sis_term_id = ss.sis_term_id
    GROUP BY cs.canvas_course_id
)

SELECT
    c.id,
    c.account_id,
    c.enrollment_term_id,
    /*
     * Use the actual meeting end date, unless it's null, in which case use the most common end date for the term.
     */
    COALESCE(cd.max_meeting_end_date, td.meeting_end_date) AS anchor_date,
    COUNT(DISTINCT e.user_id) AS total_unique_students,

    /*
     * Each course has one or more student enrollment and each enrollment has a last_activity_at value.
     * The following three blocks calculate the percent of student enrollments that are within 30/60/90 days of
     * the meeting end date.
     */

    COALESCE(ROUND(100.0 * COUNT(DISTINCT CASE
        WHEN e.last_activity_at >= COALESCE(cd.max_meeting_end_date, td.meeting_end_date) + INTERVAL '30 days'
        THEN e.user_id END) / NULLIF(COUNT(DISTINCT e.user_id), 0), 2), 0) AS last_active_30_days_after_course_end,

    COALESCE(ROUND(100.0 * COUNT(DISTINCT CASE
        WHEN e.last_activity_at >= COALESCE(cd.max_meeting_end_date, td.meeting_end_date) + INTERVAL '60 days'
        THEN e.user_id END) / NULLIF(COUNT(DISTINCT e.user_id), 0), 2), 0) AS last_active_60_days_after_course_end,

    COALESCE(ROUND(100.0 * COUNT(DISTINCT CASE
        WHEN e.last_activity_at >= COALESCE(cd.max_meeting_end_date, td.meeting_end_date) + INTERVAL '90 days'
        THEN e.user_id END) / NULLIF(COUNT(DISTINCT e.user_id), 0), 2), 0) AS last_active_90_days_after_course_end

FROM {redshift_schema_canvas_data_2}.courses c
LEFT JOIN {redshift_schema_canvas_data_2}.enrollments e ON c.id = e.course_id
/*
 * Join the canvas sites to the bcourses_accounts and enrollment_terms tables to allow us to group by department
 * or college.
 */
LEFT JOIN {redshift_schema_canvas_data_2}.enrollment_terms et ON c.enrollment_term_id = et.id
LEFT JOIN {bi_redshift_schema_bcourses_service_cd2}.bcourses_accounts acc_meta ON c.account_id = acc_meta.account_id
LEFT JOIN CourseDates cd ON c.id = cd.canvas_course_id
LEFT JOIN TermDefaults td ON cd.best_sis_term_id = td.sis_term_id

/*
 * We want to exclude courses that belong to a non-standard term, such as PROJECTS, ignore non-student enrollments,
 * dropped/deleted enrollments, and any pre-Fall 2016 data.
 */

WHERE et.sis_source_id ~ '^TERM:[0-9]{{4}}-[A-Z]$'
  AND e.type = 'StudentEnrollment'
  AND e.workflow_state = 'active'
  AND (
      SUBSTRING(et.sis_source_id, 6, 4)::INT > 2016
      OR (SUBSTRING(et.sis_source_id, 6, 4)::INT = 2016 AND RIGHT(et.sis_source_id, 1) >= 'D')
  )
GROUP BY
    c.id,
    c.account_id,
    c.enrollment_term_id,
    cd.max_meeting_end_date,
    td.meeting_end_date;

--------------------------------------------------------------------------------------------------
END OF TABLES
--------------------------------------------------------------------------------------------------