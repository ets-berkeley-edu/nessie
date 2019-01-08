/**
 * Copyright ©2018. The Regents of the University of California (Regents). All Rights Reserved.
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

/*
 * Drop the existing schema and its tables before redefining them.
 */

DROP SCHEMA IF EXISTS {redshift_schema_intermediate} CASCADE;
CREATE SCHEMA {redshift_schema_intermediate};

/*
 * A final grade of 'W' signals Withdraw Without Academic Penalty, in which a student is allowed to withdraw from
 * a course after the drop deadline has passed.
 */

CREATE TABLE {redshift_schema_intermediate}.sis_dropped_classes
DISTKEY (sid)
SORTKEY (sid)
AS (
    SELECT
        en.term_id AS sis_term_id,
        en.section_id AS sis_section_id,
        en.ldap_uid,
        en.sis_id AS sid,
        en.enrollment_status AS sis_enrollment_status,
        TRIM(en.grade) AS grade,
        TRIM(en.grade_midterm) AS grade_midterm,
        sc.course_display_name AS sis_course_name,
        sc.course_title AS sis_course_title,
        sc.instruction_format AS sis_instruction_format,
        sc.section_num AS sis_section_num
    FROM {redshift_schema_sis}.enrollments en
    JOIN {redshift_schema_sis}.courses sc
        ON en.term_id = sc.term_id
        AND en.section_id = sc.section_id
    WHERE
        sc.is_primary = TRUE AND (
            en.enrollment_status = 'D'
            OR en.grade = 'W'
        )
    /* Clear out duplicates. */
    GROUP BY
        en.term_id, en.section_id, en.ldap_uid, en.sis_id, en.enrollment_status, en.grade, en.grade_midterm,
        sc.course_display_name, sc.course_title, sc.instruction_format, sc.section_num
);

CREATE TABLE {redshift_schema_intermediate}.sis_sections
INTERLEAVED SORTKEY (sis_term_id, sis_section_id)
AS (
    SELECT
        sc.term_id AS sis_term_id,
        sc.section_id AS sis_section_id,
        sc.is_primary,
        sc.course_display_name AS sis_course_name,
        sc.course_title AS sis_course_title,
        sc.instruction_format AS sis_instruction_format,
        sc.section_num AS sis_section_num,
        sc.allowed_units,
        sc.instructor_uid,
        sc.instructor_name,
        sc.instructor_role_code,
        sc.meeting_location,
        sc.meeting_days,
        sc.meeting_start_time,
        sc.meeting_end_time,
        sc.meeting_start_date,
        sc.meeting_end_date
    FROM {redshift_schema_sis}.courses sc
);

/*
 * NOTE: GPA data below is derived from the EDO DB and is less reliable than GPA data in the student
 * schema, which is derived from API polling. This data covers a wider population than API-sourced GPA
 * data and is intended as a source for aggregate statistics, not individually surfaced values.
 */

CREATE TABLE {redshift_schema_intermediate}.term_gpa
DISTKEY (sid)
SORTKEY (sid, term_id)
AS (
    SELECT
        tg.sid,
        tg.term_id,
        tg.gpa,
        tg.units_total,
        tg.units_taken_for_gpa
    FROM {redshift_schema_sis}.term_gpa tg
    WHERE tg.units_taken_for_gpa > 0
    AND term_id < '{current_term_id}'
);

CREATE TABLE {redshift_schema_intermediate}.cumulative_gpa
DISTKEY (sid)
SORTKEY (sid)
AS (
    SELECT sid,
    CAST((SUM(gpa * units_taken_for_gpa) / SUM(units_taken_for_gpa)) AS DECIMAL(4,3)) AS cumulative_gpa
    FROM {redshift_schema_intermediate}.term_gpa
    GROUP BY sid
);

/*
 * Use SIS integration IDs for Canvas sections to generate a master mapping between Canvas and SIS sections. A
 * FULL OUTER JOIN is used to include all sections from Canvas and SIS data, whether integrated or not.
 */

CREATE TABLE {redshift_schema_intermediate}.course_sections
INTERLEAVED SORTKEY (canvas_course_id, canvas_section_id, sis_term_id, sis_section_id)
AS (
    /*
     * Translate SIS section IDs from Canvas data, when parseable, to section and term ids as represented in SIS
     * data. Otherwise leave blank.
     */
    WITH extracted_section_ids AS (
        SELECT
            s.canvas_id AS canvas_section_id,
            CASE
                /*
                 * Note doubled curly braces in the regexp, escaped for Python string formatting.
                 */
                WHEN s.sis_source_id ~ '^SEC:20[0-9]{{2}}-[BCD]-[0-9]{{5}}' THEN
                    ('2' + substring(s.sis_source_id, 7, 2) + translate(substring(s.sis_source_id, 10, 1), 'BCD', '258'))::int
                ELSE NULL END
                AS sis_term_id,
            CASE
                WHEN s.sis_source_id ~ '^SEC:20[0-9]{{2}}-[BCD]-[0-9]{{5}}' THEN
                    SUBSTRING(s.sis_source_id, 12, 5)::int
                ELSE NULL END
                AS sis_section_id
        FROM {redshift_schema_canvas}.course_section_dim s
    )
    SELECT
        c.canvas_id AS canvas_course_id,
        s.canvas_id AS canvas_section_id,
        c.name AS canvas_course_name,
        c.code AS canvas_course_code,
        s.name AS canvas_section_name,
        et.name AS canvas_course_term,
        sc.term_id AS sis_term_id,
        sc.section_id AS sis_section_id,
        sc.course_display_name AS sis_course_name,
        sc.course_title AS sis_course_title,
        sc.instruction_format AS sis_instruction_format,
        sc.section_num AS sis_section_num,
        sc.is_primary AS sis_primary
    FROM {redshift_schema_canvas}.course_section_dim s
    JOIN {redshift_schema_canvas}.course_dim c
        ON s.course_id = c.id
    JOIN {redshift_schema_canvas}.enrollment_term_dim et
         ON c.enrollment_term_id = et.id
    LEFT JOIN extracted_section_ids ON s.canvas_id = extracted_section_ids.canvas_section_id
    FULL OUTER JOIN {redshift_schema_sis}.courses sc
        ON extracted_section_ids.sis_term_id = sc.term_id
        AND extracted_section_ids.sis_section_id = sc.section_id
    WHERE (s.workflow_state IS NULL OR s.workflow_state != 'deleted')
    /* Clear out duplicates, since SIS data will contain multiple rows for multiple meetings or instructor assignments. */
    GROUP BY
        c.canvas_id, s.canvas_id, c.name, c.code, s.name, et.name,
        sc.term_id, sc.section_id,
        sc.course_display_name, sc.course_title, sc.instruction_format, sc.section_num, sc.is_primary
);

CREATE TABLE {redshift_schema_intermediate}.sis_enrollments
INTERLEAVED SORTKEY (sis_term_id, sis_section_id, ldap_uid)
AS (
    SELECT
        en.term_id AS sis_term_id,
        en.section_id AS sis_section_id,
        en.ldap_uid,
        en.enrollment_status AS sis_enrollment_status,
        en.units,
        en.grading_basis,
        TRIM(en.grade) AS grade,
        TRIM(en.grade_midterm) AS grade_midterm,
        crs.sis_course_title,
        crs.sis_course_name,
        crs.sis_primary,
        crs.sis_instruction_format,
        crs.sis_section_num
    FROM {redshift_schema_sis}.enrollments en
    JOIN intermediate.course_sections crs
        ON crs.sis_section_id = en.section_id
        AND crs.sis_term_id = en.term_id
    WHERE
        en.enrollment_status != 'D'
        AND en.grade != 'W'
);

/*
 * Combine Canvas Data user_dim and pseudonym_dim tabeles to map Canvas and SIS user ids to global Canvas Data ids.
 * Since multiple mappings frequently exist for a given Canvas id, track only the user_ids which have an active
 * workflow_state. A handful (about 100) canvas_user_ids have multiple active sis_login_ids, represented in this
 * table by multiple rows.
 */

CREATE TABLE {redshift_schema_intermediate}.users
INTERLEAVED SORTKEY (canvas_id, uid, sis_user_id)
AS (
    SELECT
        u.id AS global_id,
        u.canvas_id,
        u.name,
        u.sortable_name,
        p.sis_user_id,
        p.unique_name AS sis_login_id,
        /* Extract a numeric-string UID from Canvas sis_login_id if possible, otherwise leave blank. */
        CASE
            WHEN REPLACE(p.unique_name, 'inactive-', '') !~ '[A-Za-z]'
            THEN REPLACE(p.unique_name, 'inactive-', '')
        ELSE NULL END
            AS uid
    FROM
        {redshift_schema_canvas}.user_dim u
    LEFT JOIN {redshift_schema_canvas}.pseudonym_dim p ON u.id = p.user_id
    WHERE
        p.workflow_state = 'active'
);

/*
 * Collect all active student Canvas course site enrollments and note SIS enrollment status, if any, in SIS sections integrated
 * with the course site.
 */

CREATE TABLE {redshift_schema_intermediate}.active_student_enrollments
INTERLEAVED SORTKEY (uid, canvas_course_id)
AS (
    SELECT
        {redshift_schema_intermediate}.users.uid AS uid,
        {redshift_schema_intermediate}.users.canvas_id AS canvas_user_id,
        {redshift_schema_canvas}.course_dim.canvas_id AS canvas_course_id,
        {redshift_schema_canvas}.enrollment_dim.id AS canvas_enrollment_id,
        {redshift_schema_canvas}.enrollment_dim.last_activity_at AS last_activity_at,
        /*
         * MIN ordering happens to match our desired precedence when reconciling enrollment status among multiple
         * sections: 'E', then 'W', then NULL.
         */
        MIN({redshift_schema_intermediate}.sis_enrollments.sis_enrollment_status) AS sis_enrollment_status,
        {redshift_schema_canvas}.course_dim.name AS canvas_course_name,
        {redshift_schema_canvas}.course_dim.code AS canvas_course_code,
        {redshift_schema_intermediate}.course_sections.canvas_course_term
    FROM
        {redshift_schema_canvas}.enrollment_fact
        JOIN {redshift_schema_canvas}.enrollment_dim
            ON {redshift_schema_canvas}.enrollment_dim.id = {redshift_schema_canvas}.enrollment_fact.enrollment_id
        JOIN {redshift_schema_intermediate}.users
            ON {redshift_schema_intermediate}.users.global_id = {redshift_schema_canvas}.enrollment_fact.user_id
        JOIN {redshift_schema_canvas}.course_dim
            ON {redshift_schema_canvas}.course_dim.id = {redshift_schema_canvas}.enrollment_fact.course_id
        LEFT JOIN {redshift_schema_intermediate}.course_sections
            ON {redshift_schema_canvas}.course_dim.canvas_id = {redshift_schema_intermediate}.course_sections.canvas_course_id
        LEFT JOIN {redshift_schema_intermediate}.sis_enrollments ON
            {redshift_schema_intermediate}.course_sections.sis_section_id = {redshift_schema_intermediate}.sis_enrollments.sis_section_id AND
            {redshift_schema_intermediate}.course_sections.sis_term_id = {redshift_schema_intermediate}.sis_enrollments.sis_term_id AND
            {redshift_schema_intermediate}.sis_enrollments.ldap_uid = {redshift_schema_intermediate}.users.uid
    WHERE
        {redshift_schema_canvas}.enrollment_dim.type = 'StudentEnrollment'
        AND {redshift_schema_canvas}.enrollment_dim.workflow_state in ('active', 'completed')
        AND {redshift_schema_canvas}.course_dim.workflow_state in ('available', 'completed')
    GROUP BY
        {redshift_schema_intermediate}.users.uid,
        {redshift_schema_intermediate}.users.canvas_id,
        {redshift_schema_canvas}.course_dim.canvas_id,
        {redshift_schema_canvas}.enrollment_dim.id,
        {redshift_schema_canvas}.enrollment_dim.last_activity_at,
        {redshift_schema_canvas}.course_dim.name,
        {redshift_schema_canvas}.course_dim.code,
        {redshift_schema_intermediate}.course_sections.canvas_course_term
);

/*
 * Approximate the Canvas API's Page Views metric using the Canvas Data loads.
 * NOTE: This table is not currently referred to by other code, but it is used to support ongoing
 * data research and monitoring.
 */

CREATE TABLE {redshift_schema_intermediate}.page_views_zscore
AS (
WITH
    /*
     * Get all active student enrollments on {redshift_schema_canvas}.
     * The aggregation helps in de-duplication of enrollments
     * as there can be multiple records arising due to StudentEnrollment
     * memberships to different sections. Also, aggregation is faster than
     * the regular DISTINCT clause as it invoke Spectrum layer
     */
    e1 AS (
        SELECT
            user_id,
            course_id,
            type,
            count(*)
        FROM
            {redshift_schema_canvas}.enrollment_dim
        WHERE
            workflow_state IN ('active', 'completed')
            AND type = 'StudentEnrollment'
        GROUP BY
            user_id,
            course_id,
            type
    ),
    /*
     * Filter out requests which were unlikely to correspond to a single student page view.
     * TODO: Add page_view and participation dictionary tables to further filter requests
     */
    w0 AS (
        SELECT
            e1.user_id,
            e1.course_id,
            r.url,
            e1.type AS enrollment_type
        FROM
            e1
        LEFT OUTER JOIN {redshift_schema_canvas}.requests r ON r.user_id = e1.user_id
            AND r.course_id = e1.course_id
            AND r.url LIKE '/courses/%'
            AND r.url NOT LIKE '/courses/%/files/%module_item_id%'
            AND r.url NOT LIKE '/courses/%/files/%/inline_view'
            AND r.url NOT LIKE '/courses/%/modules/items/assignment_info'
            AND r.url NOT LIKE '/courses/%/modules/progressions'
    ),
    /*
     * Calculates activity frequency for a user on a course level
     */
    w1 AS (
        SELECT
            course_id, user_id, COALESCE(count(url), 0) AS user_page_views
        FROM
            w0
        WHERE
            course_id IS NOT NULL
            AND user_id IS NOT NULL
        GROUP BY
            course_id,
            user_id
    ),
    /*
     * Calculates activity frequency for each course
     * TODO: There are a few courses that span enrollment terms
     * Check if the frequency is correctly calculated for them
     */
    w2 AS (
        SELECT
            course_id,
            COALESCE(AVG(user_page_views), 0) AS avg_course_page_views,
            CAST(STDDEV_POP(user_page_views) AS dec (14, 2)) stddev_course_page_views
        FROM
            w1
        GROUP BY
            course_id
    ),
    /*
     * Calculates Z-scores for users records having non zero std dev
     */
    w3 AS (
        SELECT
            w1.course_id AS course_id,
            w1.user_id AS user_id,
            w1.user_page_views,
            w2.avg_course_page_views,
            w2.stddev_course_page_views, (w1.user_page_views - w2.avg_course_page_views) / (w2.stddev_course_page_views) AS user_page_view_zscore
        FROM
            w1
            JOIN w2 ON w1.course_id = w2.course_id
        WHERE
            w2.stddev_course_page_views > 0.00
    ),
    /*
     * page_views_zscore calculation query, where nulls/divide by zero errors are handled
     */
    w4 AS (
        SELECT
            w1.course_id AS course_id, w1.user_id AS user_id, w1.user_page_views, w2.avg_course_page_views, w2.stddev_course_page_views, COALESCE(w3.user_page_view_zscore, 0) AS user_page_view_zscore
        FROM
            w1
            LEFT JOIN w3 ON w1.course_id = w3.course_id
                AND w1.user_id = w3.user_id
            LEFT JOIN w2 ON w1.course_id = w2.course_id
    )
    /*
     * Add user and courses related information
     * Instructure uses bigintegers internally as keys.
     * Switch with canvas course_ids and user_ids
     */
    SELECT
        w4.user_id,
        u.canvas_id AS canvas_user_id,
        u.sis_login_id,
        u.sis_user_id,
        w4.course_id,
        c.canvas_id AS canvas_course_id,
        c.code AS canvas_course_code,
        c.sis_source_id AS sis_course_id,
        w4.user_page_views,
        w4.avg_course_page_views,
        w4.stddev_course_page_views,
        w4.user_page_view_zscore AS user_page_views_zscore
    FROM
        w4
        LEFT JOIN {redshift_schema_intermediate}.users u ON w4.user_id = u.global_id
        LEFT JOIN {redshift_schema_canvas}.course_dim c ON w4.course_id = c.id
);
