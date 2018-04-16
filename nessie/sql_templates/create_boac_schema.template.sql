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

CREATE SCHEMA IF NOT EXISTS {redshift_schema_boac};

DROP TABLE IF EXISTS {redshift_schema_boac}.assignment_submissions_scores;

CREATE TABLE {redshift_schema_boac}.assignment_submissions_scores
INTERLEAVED SORTKEY (user_id, course_id, assignment_id)
AS (
    /*
     * Following Canvas code, in cases where multiple assignment overrides associate a student with an assignment,
     * we prefer the override with the latest due date.
     */
    WITH most_lenient_override AS (
        SELECT
            {redshift_schema_canvas}.assignment_override_user_rollup_fact.assignment_id AS assignment_id,
            {redshift_schema_canvas}.assignment_override_user_rollup_fact.user_id AS user_id,
            MAX({redshift_schema_canvas}.assignment_override_dim.due_at) AS due_at
        FROM {redshift_schema_canvas}.assignment_override_user_rollup_fact
        LEFT JOIN {redshift_schema_canvas}.assignment_override_dim
            ON {redshift_schema_canvas}.assignment_override_dim.id = {redshift_schema_canvas}.assignment_override_user_rollup_fact.assignment_override_id
            AND {redshift_schema_canvas}.assignment_override_dim.workflow_state = 'active'
        GROUP BY
            {redshift_schema_canvas}.assignment_override_user_rollup_fact.assignment_id,
            {redshift_schema_canvas}.assignment_override_user_rollup_fact.user_id
    )
    SELECT
        {redshift_schema_canvas}.user_dim.canvas_id AS user_id,
        {redshift_schema_canvas}.course_dim.canvas_id AS course_id,
        {redshift_schema_canvas}.assignment_dim.canvas_id AS assignment_id,
        CASE
            /*
             * An unsubmitted assignment is "missing" if it has a known due date in the past.
             * TODO : Canvas's recently added late_policy_status feature can override this logic.
             */
            WHEN {redshift_schema_canvas}.submission_dim.submission_type IS NULL
                AND {redshift_schema_canvas}.submission_dim.submitted_at IS NULL
                AND {redshift_schema_canvas}.submission_dim.excused != 'excused_submission'
                AND (
                    most_lenient_override.due_at < getdate()
                    OR (most_lenient_override.due_at IS NULL
                        AND {redshift_schema_canvas}.assignment_dim.due_at IS NOT NULL
                        AND {redshift_schema_canvas}.assignment_dim.due_at < getdate())
                )
                AND COALESCE(NULLIF({redshift_schema_canvas}.assignment_dim.submission_types, ''), 'none') not similar to
                    '%(none|not\_graded|on\_paper|wiki\_page|external\_tool)%'
            THEN
                'missing'
            /*
             * Other ungraded unsubmitted submittable assignments, with due dates in the future or unknown, are simply "unsubmitted".
             * (This seems to correspond to the usage of "floating" in the Canvas analytics API.)
             * We check for a grade because the instructor may have allowed the student to handle the assignment
             * outside expected digital-submission channels. If so, it would be misleading to flag it "unsubmitted".
             * TODO : The count will include any "excused" submittable assignments, which may not be what we want.
             */
            WHEN {redshift_schema_canvas}.submission_dim.submission_type IS NULL
                AND {redshift_schema_canvas}.submission_dim.submitted_at IS NULL
                AND COALESCE(NULLIF({redshift_schema_canvas}.assignment_dim.submission_types, ''), 'none') not similar to
                    '%(none|not\_graded|on\_paper|wiki\_page|external\_tool)%'
                AND (
                    {redshift_schema_canvas}.submission_fact.score IS NULL
                    OR {redshift_schema_canvas}.submission_fact.score = 0.0
                 )
            THEN
                'unsubmitted'
            /*
             * Submitted assignments with a known submission date after a known due date are late.
             * TODO : Canvas's recently added late_policy_status feature can override this logic.
             */
            WHEN {redshift_schema_canvas}.submission_dim.submitted_at IS NOT NULL
                AND {redshift_schema_canvas}.assignment_dim.due_at IS NOT NULL
                AND COALESCE(NULLIF({redshift_schema_canvas}.assignment_dim.submission_types, ''), 'none') not similar to
                    '%(none|not\_graded|on\_paper|wiki\_page|external\_tool)%'
                AND most_lenient_override.due_at < {redshift_schema_canvas}.submission_dim.submitted_at
                OR (
                    most_lenient_override.due_at IS NULL
                    AND {redshift_schema_canvas}.assignment_dim.due_at <
                        {redshift_schema_canvas}.submission_dim.submitted_at +
                        CASE {redshift_schema_canvas}.submission_dim.submission_type WHEN 'online_quiz' THEN interval '1 minute' ELSE interval '0 minutes' END
                )
            THEN
                'late'
            /*
             * Submitted assignments with a known submission date before or equal to a known due date are on time.
             */
            WHEN {redshift_schema_canvas}.submission_dim.submitted_at IS NOT NULL
                AND {redshift_schema_canvas}.assignment_dim.due_at IS NOT NULL
                AND COALESCE(NULLIF({redshift_schema_canvas}.assignment_dim.submission_types, ''), 'none') not similar to
                    '%(none|not\_graded|on\_paper|wiki\_page|external\_tool)%'
                AND most_lenient_override.due_at >= {redshift_schema_canvas}.submission_dim.submitted_at
                OR (
                    most_lenient_override.due_at IS NULL
                    AND {redshift_schema_canvas}.assignment_dim.due_at >=
                        {redshift_schema_canvas}.submission_dim.submitted_at +
                        CASE {redshift_schema_canvas}.submission_dim.submission_type WHEN 'online_quiz' THEN interval '1 minute' ELSE interval '0 minutes' END
                )
            THEN
                'on_time'
            /*
             * Remaining submittable assignments are simply "submitted."
             */
            WHEN COALESCE(NULLIF({redshift_schema_canvas}.assignment_dim.submission_types, ''), 'none') not similar to
                    '%(none|not\_graded|on\_paper|wiki\_page|external\_tool)%'
            THEN
                'submitted'
            /*
             * Non-digital (or unsubmittable) assignments are either graded or ungraded. They may have due_at dates,
             * and may even have submitted_at dates, but lag times are difficult to interpret. However, zero scores
             * generally indicate undone work.
             */
            WHEN {redshift_schema_canvas}.submission_fact.score = 0.0
            THEN
                'zero_graded'
            WHEN {redshift_schema_canvas}.submission_fact.score IS NOT NULL
            THEN
                'graded'
            ELSE
                'ungraded'
        END AS assignment_status,
        CASE
            WHEN most_lenient_override.due_at IS NULL THEN {redshift_schema_canvas}.assignment_dim.due_at
            ELSE most_lenient_override.due_at
        END AS due_at,
        {redshift_schema_canvas}.submission_dim.submitted_at AS submitted_at,
        {redshift_schema_canvas}.submission_fact.score AS score,
        {redshift_schema_canvas}.submission_dim.grade AS grade,
        {redshift_schema_canvas}.assignment_dim.points_possible AS points_possible
    FROM
        {redshift_schema_canvas}.submission_fact
        JOIN {redshift_schema_canvas}.submission_dim
            ON {redshift_schema_canvas}.submission_fact.submission_id = {redshift_schema_canvas}.submission_dim.id
        JOIN {redshift_schema_canvas}.user_dim
            ON {redshift_schema_canvas}.user_dim.id = {redshift_schema_canvas}.submission_fact.user_id
        JOIN {redshift_schema_canvas}.assignment_dim
            ON {redshift_schema_canvas}.assignment_dim.id = {redshift_schema_canvas}.submission_fact.assignment_id
        JOIN {redshift_schema_canvas}.course_dim
            ON {redshift_schema_canvas}.course_dim.id = {redshift_schema_canvas}.submission_fact.course_id
        LEFT JOIN most_lenient_override
            ON most_lenient_override.user_id = {redshift_schema_canvas}.submission_fact.user_id
            AND most_lenient_override.assignment_id = {redshift_schema_canvas}.submission_fact.assignment_id
    WHERE {redshift_schema_canvas}.assignment_dim.workflow_state = 'published'
        AND {redshift_schema_canvas}.submission_dim.workflow_state != 'deleted'
);

DROP TABLE IF EXISTS {redshift_schema_boac}.user_course_scores;

CREATE TABLE {redshift_schema_boac}.user_course_scores
SORTKEY (course_id)
AS (
    SELECT
        {redshift_schema_canvas}.user_dim.canvas_id AS user_id,
        {redshift_schema_canvas}.course_dim.canvas_id AS course_id,
        {redshift_schema_canvas}.course_score_fact.current_score AS current_score,
        {redshift_schema_canvas}.course_score_fact.final_score AS final_score
    FROM
        {redshift_schema_canvas}.enrollment_fact
        JOIN {redshift_schema_canvas}.enrollment_dim
            ON {redshift_schema_canvas}.enrollment_dim.id = {redshift_schema_canvas}.enrollment_fact.enrollment_id
        JOIN {redshift_schema_canvas}.course_score_fact
            ON {redshift_schema_canvas}.course_score_fact.enrollment_id = {redshift_schema_canvas}.enrollment_fact.enrollment_id
        JOIN {redshift_schema_canvas}.user_dim
            ON {redshift_schema_canvas}.user_dim.id = {redshift_schema_canvas}.enrollment_fact.user_id
        JOIN {redshift_schema_canvas}.course_dim
            ON {redshift_schema_canvas}.course_dim.id = {redshift_schema_canvas}.enrollment_fact.course_id
    WHERE
        {redshift_schema_canvas}.enrollment_dim.type = 'StudentEnrollment'
        AND {redshift_schema_canvas}.enrollment_dim.workflow_state = 'active'
);

DROP TABLE IF EXISTS {redshift_schema_boac}.page_views_zscore;

CREATE TABLE {redshift_schema_boac}.page_views_zscore
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
    ),
    /*
     * Combines user_dim and pseudonym_dim tables to retrieve correct
     * global_user_id, canvas_user_id, sis_login_id and sis_user_id
     * There are multiple entries in the table which maps to a single canvas_user_id.
     * So we track only the user_ids which have an active workflow_state.
     * This also occurs when there are multiple sis_login_ids mapped to the same canvas_user_id
     * and each of those entries are marked active. (Possible glitch while enrollment feed is sent to update Canvas tables)
     * The numbers are fairly small (about 109 enrollments). The query factors in this deviation and presents same
     * stats for these duplicate entries
     */
    w5 AS (
        SELECT
            u.id AS global_user_id,
            u.canvas_id,
            u.name,
            u.workflow_state,
            u.sortable_name,
            p.user_id AS pseudo_user_id,
            p.canvas_id as pseudo_canvas_id,
            p.sis_user_id,
            p.unique_name,
            p.workflow_state as active_state
        FROM
            {redshift_schema_canvas}.user_dim u
        LEFT JOIN {redshift_schema_canvas}.pseudonym_dim p ON u.id = p.user_id
        WHERE
            p.workflow_state = 'active'
    )
    /*
     * Add user and courses related information
     * Instructure uses bigintergers internally as keys.
     * Switch with canvas course_ids and user_ids
     */
    SELECT
        w4.user_id,
        w5.canvas_id AS canvas_user_id,
        w5.unique_name AS sis_login_id,
        w5.sis_user_id AS sis_user_id,
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
    LEFT JOIN w5 ON w4.user_id = w5.global_user_id
    LEFT JOIN {redshift_schema_canvas}.course_dim c ON w4.course_id = c.id
);
