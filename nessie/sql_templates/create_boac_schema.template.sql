/**
 * Copyright ©2019. The Regents of the University of California (Regents). All Rights Reserved.
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

DROP SCHEMA IF EXISTS {redshift_schema_boac} CASCADE;
CREATE SCHEMA {redshift_schema_boac};
GRANT USAGE ON SCHEMA {redshift_schema_boac} TO GROUP {redshift_app_boa_user}_group;
ALTER default PRIVILEGES IN SCHEMA {redshift_schema_boac} GRANT SELECT ON TABLES TO GROUP {redshift_app_boa_user}_group;

CREATE TABLE {redshift_schema_boac}.assignment_submissions_scores
SORTKEY (canvas_user_id, assignment_id)
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
    ),
    assignment_type AS (
        SELECT
            id,
            CASE WHEN (
                COALESCE(NULLIF(submission_types, ''), 'none') NOT LIKE '%none%' AND
                COALESCE(NULLIF(submission_types, ''), 'none') NOT LIKE '%not_graded%' AND
                COALESCE(NULLIF(submission_types, ''), 'none') NOT LIKE '%on_paper%' AND
                COALESCE(NULLIF(submission_types, ''), 'none') NOT LIKE '%wiki_page%' AND
                COALESCE(NULLIF(submission_types, ''), 'none') NOT LIKE '%external_tool%'
              ) THEN 1
            ELSE NULL END AS submittable
        FROM {redshift_schema_canvas}.assignment_dim
    ),
    /*
     * We are interested in assignment submissions on a course_id level rather than a section level as determined by canvas_enrollment_id.
     * We will use a distinct to weed out any duplicates we might encountered when we ignore canvas_enrollment_id column.
     */
    distinct_user_enrollments AS (
        SELECT DISTINCT
            {redshift_schema_intermediate}.users.uid AS uid,
            {redshift_schema_intermediate}.users.canvas_id AS canvas_user_id,
            {redshift_schema_intermediate}.users.global_id AS canvas_global_user_id,
            {redshift_schema_canvas}.course_dim.canvas_id AS course_id,
            {redshift_schema_canvas}.course_dim.id AS canvas_global_course_id,
            {redshift_schema_intermediate}.active_student_enrollments.canvas_course_term AS term_name,
            {redshift_schema_intermediate}.active_student_enrollments.sis_enrollment_status AS sis_enrollment_status
        FROM
            {redshift_schema_intermediate}.active_student_enrollments
            LEFT JOIN {redshift_schema_intermediate}.users
                ON {redshift_schema_intermediate}.active_student_enrollments.uid = {redshift_schema_intermediate}.users.uid
            LEFT JOIN {redshift_schema_canvas}.course_dim
                ON {redshift_schema_intermediate}.active_student_enrollments.canvas_course_id = {redshift_schema_canvas}.course_dim.canvas_id
    )
    SELECT DISTINCT
        distinct_user_enrollments.uid AS uid,
        distinct_user_enrollments.canvas_user_id,
        distinct_user_enrollments.course_id,
        distinct_user_enrollments.term_name,
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
                AND assignment_type.submittable IS NOT NULL
                AND (
                    {redshift_schema_canvas}.submission_fact.score IS NULL
                    OR (
                        {redshift_schema_canvas}.submission_fact.score = 0.0
                      AND (
                          {redshift_schema_canvas}.assignment_dim.points_possible > 0.0
                          OR {redshift_schema_canvas}.submission_dim.grade is null
                          OR {redshift_schema_canvas}.submission_dim.grade in ('incomplete', '0', 'I', 'NP', 'F')
                        )
                    )
                 )
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
                AND assignment_type.submittable IS NOT NULL
                AND (
                    {redshift_schema_canvas}.submission_fact.score IS NULL
                    OR (
                        {redshift_schema_canvas}.submission_fact.score = 0.0
                      AND (
                          {redshift_schema_canvas}.assignment_dim.points_possible > 0.0
                          OR {redshift_schema_canvas}.submission_dim.grade is null
                          OR {redshift_schema_canvas}.submission_dim.grade in ('incomplete', '0', 'I', 'NP', 'F')
                        )
                    )
                )
            THEN
                'unsubmitted'
            /*
             * Submitted assignments with a known submission date after a known due date are late.
             * TODO : Canvas's recently added late_policy_status feature can override this logic.
             */
            WHEN {redshift_schema_canvas}.submission_dim.submitted_at IS NOT NULL
                AND assignment_type.submittable IS NOT NULL
                AND most_lenient_override.due_at < {redshift_schema_canvas}.submission_dim.submitted_at
                OR (
                    most_lenient_override.due_at IS NULL
                    AND {redshift_schema_canvas}.assignment_dim.due_at IS NOT NULL
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
                AND assignment_type.submittable IS NOT NULL
                AND most_lenient_override.due_at >= {redshift_schema_canvas}.submission_dim.submitted_at
                OR (
                    most_lenient_override.due_at IS NULL
                    AND {redshift_schema_canvas}.assignment_dim.due_at IS NOT NULL
                    AND {redshift_schema_canvas}.assignment_dim.due_at >=
                    {redshift_schema_canvas}.submission_dim.submitted_at +
                    CASE {redshift_schema_canvas}.submission_dim.submission_type WHEN 'online_quiz' THEN interval '1 minute' ELSE interval '0 minutes' END
                )
            THEN
                'on_time'
            /*
             * Remaining submittable assignments are simply "submitted."
             */
            WHEN
                assignment_type.submittable IS NOT NULL
            THEN
                'submitted'
            /*
             * Non-digital (or unsubmittable) assignments are either graded or ungraded. They may have due_at dates,
             * and may even have submitted_at dates, but lag times are difficult to interpret. However, zero scores
             * generally indicate undone work.
             */
            WHEN {redshift_schema_canvas}.submission_fact.score = 0.0
                AND (
                    {redshift_schema_canvas}.assignment_dim.points_possible > 0.0
                    OR {redshift_schema_canvas}.submission_dim.grade is null
                    OR {redshift_schema_canvas}.submission_dim.grade in ('incomplete', '0', 'I', 'NP', 'F')
                )
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
        {redshift_schema_canvas}.assignment_dim.points_possible AS points_possible,
        distinct_user_enrollments.sis_enrollment_status AS sis_enrollment_status
    FROM
        {redshift_schema_canvas}.submission_fact
        INNER JOIN {redshift_schema_canvas}.submission_dim
            ON {redshift_schema_canvas}.submission_fact.submission_id = {redshift_schema_canvas}.submission_dim.id
        INNER JOIN {redshift_schema_canvas}.assignment_dim
            ON {redshift_schema_canvas}.submission_fact.assignment_id = {redshift_schema_canvas}.assignment_dim.id
        INNER JOIN assignment_type
            ON {redshift_schema_canvas}.submission_fact.assignment_id = assignment_type.id
        LEFT JOIN most_lenient_override
            ON {redshift_schema_canvas}.submission_fact.user_id = most_lenient_override.user_id
            AND {redshift_schema_canvas}.submission_fact.assignment_id = most_lenient_override.assignment_id
        LEFT JOIN distinct_user_enrollments
            ON {redshift_schema_canvas}.submission_fact.user_id = distinct_user_enrollments.canvas_global_user_id
            AND {redshift_schema_canvas}.submission_fact.course_id = distinct_user_enrollments.canvas_global_course_id
    WHERE {redshift_schema_canvas}.assignment_dim.workflow_state = 'published'
        AND {redshift_schema_canvas}.submission_dim.workflow_state != 'deleted'
);


CREATE TABLE {redshift_schema_boac}.course_enrollments
SORTKEY (course_id)
AS (
    SELECT
        ase.uid,
        ase.canvas_user_id,
        ase.canvas_course_id AS course_id,
        ase.canvas_course_term AS course_term,
        /*
         * There must be only one summary row for each course site membership.
         *
         * API-derived activity timestamps are more current than those contained in Canvas Data dumps,
         * but we may not have them stored for all terms. Use the API timestamp only when present and
         * more recent.
         */
        GREATEST(MAX(ase.last_activity_at), MAX(api.last_activity_at), MAX(caliper.last_activity)) AS last_activity_at,
        MIN(ase.sis_enrollment_status) AS sis_enrollment_status,
        MAX(csf.current_score) AS current_score,
        MAX(csf.final_score) AS final_score
    FROM
        {redshift_schema_intermediate}.active_student_enrollments ase
        JOIN {redshift_schema_intermediate}.users
            ON ase.uid = {redshift_schema_intermediate}.users.uid
        JOIN {redshift_schema_canvas}.course_dim cd
            ON ase.canvas_course_id = cd.canvas_id
        LEFT JOIN {redshift_schema_canvas}.course_score_fact csf
            ON csf.enrollment_id = ase.canvas_enrollment_id
        LEFT JOIN {redshift_schema_student}.canvas_api_enrollments api
            ON ase.canvas_user_id = api.user_id
            AND ase.canvas_course_id = api.course_id
        LEFT JOIN {redshift_schema_caliper_analytics}.last_activity_caliper caliper
            ON ase.canvas_user_id = caliper.canvas_user_id
            AND ase.canvas_course_id = caliper.canvas_course_id
    GROUP BY
        ase.uid,
        ase.canvas_user_id,
        ase.canvas_course_id,
        ase.canvas_course_term
);


CREATE TABLE {redshift_schema_boac}.section_mean_gpas
SORTKEY(sis_term_id, sis_section_id)
AS (
    SELECT
        enr.sis_term_id,
        enr.sis_section_id,
        'cumulative' AS gpa_term_id,
        AVG(cg.cumulative_gpa) AS avg_gpa
    FROM {redshift_schema_intermediate}.sis_enrollments enr
    JOIN {redshift_schema_intermediate}.users u ON enr.ldap_uid = u.uid
    JOIN {redshift_schema_intermediate}.cumulative_gpa cg ON u.sis_user_id = cg.sid
    GROUP BY enr.sis_term_id, enr.sis_section_id
    UNION
    SELECT
        enr.sis_term_id,
        enr.sis_section_id,
        tg.term_id::varchar AS gpa_term_id,
        AVG(tg.gpa) AS avg_gpa
    FROM {redshift_schema_intermediate}.sis_enrollments enr
    JOIN {redshift_schema_intermediate}.users u ON enr.ldap_uid = u.uid
    JOIN {redshift_schema_intermediate}.term_gpa tg ON u.sis_user_id = tg.sid
        AND tg.term_id = ANY('{{{last_term_id},{previous_term_id}}}')
    GROUP BY enr.sis_term_id, enr.sis_section_id, gpa_term_id
);


CREATE TABLE {redshift_schema_boac}.student_demographics
(
    sid VARCHAR NOT NULL,
    feed VARCHAR(max) NOT NULL
)
DISTKEY (sid)
SORTKEY (sid);


/*
 * After boiled-down derived tables are generated, pull out data for the current term and store snapshots in S3.
 */

UNLOAD (
    'SELECT ce.uid, ce.canvas_user_id, ce.course_id, ce.sis_enrollment_status, ce.last_activity_at, ce.current_score, ce.final_score
    FROM {redshift_schema_boac}.course_enrollments ce
    JOIN {redshift_schema_intermediate}.course_sections cs
    ON ce.course_id = cs.canvas_course_id
    AND cs.sis_term_id = \'{current_term_id}\'
    GROUP BY ce.uid, ce.canvas_user_id, ce.course_id, ce.sis_enrollment_status, ce.last_activity_at, ce.current_score, ce.final_score'
)
TO '{boac_snapshot_daily_path}/course_scores/snapshot'
ACCESS_KEY_ID '{aws_access_key_id}'
SECRET_ACCESS_KEY '{aws_secret_access_key}'
DELIMITER AS '\t'
NULL AS ''
ALLOWOVERWRITE
GZIP;


UNLOAD (
    'SELECT ass.uid, ass.canvas_user_id, ass.course_id, ass.assignment_id, ass.assignment_status, ass.sis_enrollment_status,
        ass.due_at, ass.submitted_at, ass.score, ass.points_possible
    FROM {redshift_schema_boac}.assignment_submissions_scores ass
    JOIN {redshift_schema_intermediate}.course_sections cs
    ON ass.course_id = cs.canvas_course_id
    AND cs.sis_term_id = \'{current_term_id}\'
    GROUP BY ass.uid, ass.canvas_user_id, ass.course_id, ass.assignment_id, ass.assignment_status, ass.sis_enrollment_status,
        ass.due_at, ass.submitted_at, ass.score, ass.points_possible'
)
TO '{boac_snapshot_daily_path}/assignment_submissions_scores/snapshot'
ACCESS_KEY_ID '{aws_access_key_id}'
SECRET_ACCESS_KEY '{aws_secret_access_key}'
DELIMITER AS '\t'
NULL AS ''
ALLOWOVERWRITE
GZIP;
