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

CREATE SCHEMA IF NOT EXISTS {redshift_schema_intermediate};

DROP TABLE IF EXISTS {redshift_schema_intermediate}.course_sections;

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
        s.name AS canvas_section_name,
        extracted_section_ids.sis_term_id,
        extracted_section_ids.sis_section_id,
        sc.course_display_name AS sis_course_name,
        sc.course_title AS sis_course_title,
        sc.instruction_format AS sis_instruction_format,
        sc.section_num AS sis_section_num,
        sc.is_primary AS sis_primary
    FROM {redshift_schema_canvas}.course_section_dim s
    LEFT JOIN {redshift_schema_canvas}.course_dim c
        ON s.course_id = c.id
    LEFT JOIN extracted_section_ids ON s.canvas_id = extracted_section_ids.canvas_section_id
    FULL OUTER JOIN {redshift_schema_sis}.courses sc
        ON extracted_section_ids.sis_term_id = sc.term_id
        AND extracted_section_ids.sis_section_id = sc.section_id
    /* Clear out duplicates, since SIS data will contain multiple rows for multiple meetings or instructor assignments. */
    GROUP BY
        c.canvas_id, s.canvas_id, c.name, s.name,
        extracted_section_ids.sis_term_id, extracted_section_ids.sis_section_id,
        sc.course_display_name, sc.course_title, sc.instruction_format, sc.section_num, sc.is_primary
);

/*
 * Combine Canvas Data user_dim and pseudonym_dim tabeles to map Canvas and SIS user ids to global Canvas Data ids.
 * Since multiple mappings frequently exist for a given Canvas id, track only the user_ids which have an active
 * workflow_state. A handful (about 100) canvas_user_ids have multiple active sis_login_ids, represented in this
 * table by multiple rows.
 */

DROP TABLE IF EXISTS {redshift_schema_intermediate}.users;

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
 * Collect all active student enrollments and note SIS enrollment status, if any, in SIS sections integrated
 * with the course site.
 */
DROP TABLE IF EXISTS {redshift_schema_intermediate}.active_student_enrollments;

CREATE TABLE {redshift_schema_intermediate}.active_student_enrollments
INTERLEAVED SORTKEY (canvas_user_id, canvas_course_id)
AS (
    SELECT
        {redshift_schema_intermediate}.users.uid AS uid,
        {redshift_schema_intermediate}.users.canvas_id AS canvas_user_id,
        {redshift_schema_canvas}.course_dim.canvas_id AS canvas_course_id,
        {redshift_schema_canvas}.enrollment_dim.last_activity_at AS last_activity_at,
        /*
         * MIN ordering happens to match our desired precedence when reconciling enrollment status among multiple
         * sections: 'E', then 'W', then NULL.
         */
        MIN({redshift_schema_sis}.enrollments.enrollment_status) AS sis_enrollment_status
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
        LEFT JOIN {redshift_schema_sis}.enrollments ON
            {redshift_schema_intermediate}.course_sections.sis_section_id = {redshift_schema_sis}.enrollments.section_id AND
            {redshift_schema_intermediate}.course_sections.sis_term_id = {redshift_schema_sis}.enrollments.term_id AND
            {redshift_schema_sis}.enrollments.ldap_uid = {redshift_schema_intermediate}.users.uid
    WHERE
        {redshift_schema_canvas}.enrollment_dim.type = 'StudentEnrollment'
        AND {redshift_schema_canvas}.enrollment_dim.workflow_state in ('active', 'completed')
        AND {redshift_schema_canvas}.course_dim.workflow_state in ('available', 'completed')
    GROUP BY
        {redshift_schema_intermediate}.users.uid,
        {redshift_schema_intermediate}.users.canvas_id,
        {redshift_schema_canvas}.course_dim.canvas_id,
        {redshift_schema_canvas}.enrollment_dim.last_activity_at
);
