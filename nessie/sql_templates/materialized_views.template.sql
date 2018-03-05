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

CREATE SCHEMA IF NOT EXISTS <%= boacAnalyticsSchema %>;

DROP TABLE IF EXISTS <%= boacAnalyticsSchema %>.page_views_zscore;

CREATE TABLE <%= boacAnalyticsSchema %>.page_views_zscore
AS (
WITH
    /*
     * Get all active student enrollments on <%= externalSchema %>.
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
            <%= externalSchema %>.enrollment_dim
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
        LEFT OUTER JOIN <%= externalSchema %>.requests r ON r.user_id = e1.user_id
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
            <%= externalSchema %>.user_dim u
        LEFT JOIN <%= externalSchema %>.pseudonym_dim p ON u.id = p.user_id
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
    LEFT JOIN <%= externalSchema %>.course_dim c ON w4.course_id = c.id
);

DROP TABLE IF EXISTS <%= boacAnalyticsSchema %>.assignment_submissions_scores;

CREATE TABLE <%= boacAnalyticsSchema %>.assignment_submissions_scores
INTERLEAVED SORTKEY (user_id, course_id, assignment_id)
AS (
    /*
     * Following Canvas code, in cases where multiple assignment overrides associate a student with an assignment,
     * the override with the latest due date controls.
     */
    WITH most_lenient_override AS (
        SELECT
            <%= externalSchema %>.assignment_override_user_rollup_fact.assignment_id AS assignment_id,
            <%= externalSchema %>.assignment_override_user_rollup_fact.user_id AS user_id,
            MAX(<%= externalSchema %>.assignment_override_dim.due_at) AS due_at
        FROM <%= externalSchema %>.assignment_override_user_rollup_fact
        LEFT JOIN <%= externalSchema %>.assignment_override_dim
            ON <%= externalSchema %>.assignment_override_dim.id = <%= externalSchema %>.assignment_override_user_rollup_fact.assignment_override_id
            AND <%= externalSchema %>.assignment_override_dim.workflow_state = 'active'
        GROUP BY
            <%= externalSchema %>.assignment_override_user_rollup_fact.assignment_id,
            <%= externalSchema %>.assignment_override_user_rollup_fact.user_id
    )
    SELECT
        <%= externalSchema %>.user_dim.canvas_id AS user_id,
        <%= externalSchema %>.course_dim.canvas_id AS course_id,
        <%= externalSchema %>.assignment_dim.canvas_id AS assignment_id,
        CASE
            /*
             * An unsubmitted assignment is "missing" if it has a known due date in the past.
             */
            WHEN <%= externalSchema %>.submission_dim.workflow_state = 'unsubmitted'
            AND (
                most_lenient_override.due_at < getdate() OR
                (most_lenient_override.due_at IS NULL AND <%= externalSchema %>.assignment_dim.due_at < getdate())
            )
            THEN
                'missing'
            /*
             * Other unsubmitted assignments, with due dates in the future or unknown, are simply "unsubmitted".
             * (This seems to correspond to the usage of "floating" in the Canvas analytics API.)
             */
            WHEN <%= externalSchema %>.submission_dim.workflow_state = 'unsubmitted'
            THEN
                'unsubmitted'
            /*
             * Submitted assignments with a known submission date after a known due date are late.
             */
            WHEN
                most_lenient_override.due_at < <%= externalSchema %>.submission_dim.submitted_at
                OR (
                    most_lenient_override.due_at IS NULL
                    AND <%= externalSchema %>.assignment_dim.due_at < <%= externalSchema %>.submission_dim.submitted_at
                )
            THEN
                'late'
            /*
             * Submitted assignments with an unknown submission date or unknown due date are simply "submitted."
             */
            WHEN <%= externalSchema %>.submission_dim.submitted_at IS NULL
                OR (most_lenient_override.due_at IS NULL AND <%= externalSchema %>.assignment_dim.due_at IS NULL)
            THEN
                'submitted'
            /*
             * Remaining assignments have a known submission date before or equal to a known due date, and are on time.
             */
            ELSE
                'on_time'
        END AS assignment_status,
        CASE
            WHEN most_lenient_override.due_at IS NULL THEN <%= externalSchema %>.assignment_dim.due_at
            ELSE most_lenient_override.due_at
        END AS due_at,
        <%= externalSchema %>.submission_dim.submitted_at AS submitted_at,
        <%= externalSchema %>.submission_fact.score AS score,
        <%= externalSchema %>.submission_dim.grade AS grade,
        <%= externalSchema %>.assignment_dim.points_possible AS points_possible
    FROM
        <%= externalSchema %>.submission_fact
        JOIN <%= externalSchema %>.submission_dim
            ON <%= externalSchema %>.submission_fact.submission_id = <%= externalSchema %>.submission_dim.id
        JOIN <%= externalSchema %>.user_dim
            ON <%= externalSchema %>.user_dim.id = <%= externalSchema %>.submission_fact.user_id
        JOIN <%= externalSchema %>.assignment_dim
            ON <%= externalSchema %>.assignment_dim.id = <%= externalSchema %>.submission_fact.assignment_id
        JOIN <%= externalSchema %>.course_dim
            ON <%= externalSchema %>.course_dim.id = <%= externalSchema %>.submission_fact.course_id
        LEFT JOIN most_lenient_override
            ON most_lenient_override.user_id = <%= externalSchema %>.submission_fact.user_id
            AND most_lenient_override.assignment_id = <%= externalSchema %>.submission_fact.assignment_id
    WHERE <%= externalSchema %>.assignment_dim.workflow_state = 'published'
);
