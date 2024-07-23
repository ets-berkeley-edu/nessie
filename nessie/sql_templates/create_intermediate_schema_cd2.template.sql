/**
 * Copyright ©2024. The Regents of the University of California (Regents). All Rights Reserved.
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
 * NOTE - Queries use Canvas Data 2 equvivalent tables to replace Canvas Data 1 references.
 * Drop the existing schema and its tables before redefining them.
 */

DROP SCHEMA IF EXISTS {redshift_schema_intermediate} CASCADE;
CREATE SCHEMA {redshift_schema_intermediate};
GRANT USAGE ON SCHEMA {redshift_schema_intermediate} TO GROUP {redshift_app_boa_user}_group;
ALTER default PRIVILEGES IN SCHEMA {redshift_schema_intermediate} GRANT SELECT ON TABLES TO GROUP {redshift_app_boa_user}_group;
GRANT USAGE ON SCHEMA {redshift_schema_intermediate} TO GROUP {redshift_dblink_group};
ALTER DEFAULT PRIVILEGES IN SCHEMA {redshift_schema_intermediate} GRANT SELECT ON TABLES TO GROUP {redshift_dblink_group};

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
        sc.section_num AS sis_section_num,
        sc.instruction_mode AS sis_instruction_mode
    FROM {redshift_schema_edl}.enrollments en
    JOIN {redshift_schema_edl}.courses sc
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
        sc.course_display_name, sc.course_title, sc.instruction_format, sc.section_num, sc.instruction_mode
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
        sc.cs_course_id,
        sc.session_code,
        sc.allowed_units,
        sc.instruction_mode,
        sc.instructor_uid,
        sc.instructor_name,
        sc.instructor_role_code,
        sc.meeting_location,
        sc.meeting_days,
        sc.meeting_start_time,
        sc.meeting_end_time,
        sc.meeting_start_date,
        sc.meeting_end_date,
        r.course_requirements
    FROM {redshift_schema_edl}.courses sc
    LEFT JOIN (
        SELECT
            cr.term_id,
            cr.section_id,
            SPLIT_TO_ARRAY(LISTAGG(cr.requirement_description, '|'), '|') AS course_requirements
            FROM edl_sis_data.course_requirements cr
            GROUP BY cr.term_id, cr.section_id
        ) AS r
    ON r.term_id = sc.term_id
    AND r.section_id = sc.section_id
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
    FROM {redshift_schema_edl}.term_gpa tg
    WHERE tg.units_taken_for_gpa > 0
    AND term_id < '{current_term_id}'
);

CREATE TABLE {redshift_schema_intermediate}.cumulative_gpa
DISTKEY (sid)
SORTKEY (sid)
AS (
    SELECT sid,
    CAST((SUM(gpa * units_taken_for_gpa) / SUM(units_taken_for_gpa)) AS DECIMAL(5,3)) AS cumulative_gpa
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
     * Translate SIS section IDs from Canvas data, when parseable and post-CS-transition, to section and term ids as
     * represented in SIS data. Otherwise leave blank.
     */
    WITH extracted_section_ids AS (
        SELECT
            s.id AS canvas_section_id,
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
        FROM {redshift_schema_canvas_data_2}.course_sections s
    )
    SELECT
        c.id AS canvas_course_id,
        s.id AS canvas_section_id,
        c.name AS canvas_course_name,
        c.course_code AS canvas_course_code,
        s.name AS canvas_section_name,
        et.name AS canvas_course_term,
        sc.term_id AS sis_term_id,
        sc.section_id AS sis_section_id,
        sc.course_display_name AS sis_course_name,
        sc.course_title AS sis_course_title,
        sc.instruction_format AS sis_instruction_format,
        sc.section_num AS sis_section_num,
        sc.is_primary AS sis_primary,
        sc.instruction_mode AS sis_instruction_mode
    FROM {redshift_schema_canvas_data_2}.course_sections s
    JOIN {redshift_schema_canvas_data_2}.courses c
        ON s.course_id = c.id
    JOIN {redshift_schema_canvas_data_2}.enrollment_terms et
         ON c.enrollment_term_id = et.id
    LEFT JOIN extracted_section_ids ON s.id = extracted_section_ids.canvas_section_id
    FULL OUTER JOIN {redshift_schema_edl}.courses sc
        ON extracted_section_ids.sis_term_id >= '{earliest_term_id}'
        AND extracted_section_ids.sis_term_id = sc.term_id::int
        AND extracted_section_ids.sis_section_id = sc.section_id::int
        AND c.workflow_state IN ('available', 'completed')
        AND (s.workflow_state IS NULL OR s.workflow_state != 'deleted')
    /* Clear out duplicates, since SIS data will contain multiple rows for multiple meetings or instructor assignments. */
    GROUP BY
        c.id, s.id, c.name, c.course_code, s.name, et.name,
        sc.term_id, sc.section_id,
        sc.course_display_name, sc.course_title, sc.instruction_format, sc.section_num, sc.is_primary, sc.instruction_mode
);

CREATE TABLE {redshift_schema_intermediate}.sis_enrollments
INTERLEAVED SORTKEY (sis_term_id, sis_section_id, ldap_uid)
AS (
    SELECT DISTINCT
        en.term_id AS sis_term_id,
        en.section_id AS sis_section_id,
        en.ldap_uid,
        en.academic_career,
        en.enrollment_status AS sis_enrollment_status,
        en.units,
        en.grading_basis,
        TRIM(en.grade) AS grade,
        TRIM(en.grade_midterm) AS grade_midterm,
        en.sis_id AS sid,
        crs.sis_course_title,
        crs.sis_course_name,
        crs.sis_primary,
        crs.sis_instruction_format,
        crs.sis_section_num,
        crs.sis_instruction_mode,
        en.incomplete_comments,
        en.incomplete_frozen_flag,
        en.incomplete_lapse_grade_date,
        en.incomplete_lapse_to_grade,
        en.incomplete_status_code,
        en.incomplete_status_description
    FROM {redshift_schema_edl}.enrollments en
    JOIN {redshift_schema_intermediate}.course_sections crs
        ON crs.sis_section_id = en.section_id
        AND crs.sis_term_id = en.term_id
    WHERE
        /* No dropped or withdrawn enrollments. */
        en.enrollment_status != 'D'
        AND en.grade != 'W'
        {where_clause_exclude_withdrawn}
        /* No waitlisted enrollments from past terms. */
        AND (en.enrollment_status != 'W' OR en.term_id >= '{current_term_id}')
);

/*
 * Combine Canvas Data user_dim and pseudonym_dim tables to map Canvas and SIS user ids to global Canvas Data ids.
 * Since multiple mappings frequently exist for a given Canvas id, track only the user_ids which have an active
 * workflow_state. A handful (about 100) canvas_user_ids have multiple active sis_login_ids, represented in this
 * table by multiple rows.
 *
 * Our Canvas ID integration can temporarily cause a single CalNet UID to be associated with both an active and an
 * inactive account. When that happens, prefer the SIS_LOGIN_ID which is not flagged by an "inactive-" prefix.
 */

CREATE TABLE {redshift_schema_intermediate}.users
INTERLEAVED SORTKEY (canvas_id, uid, sis_user_id)
AS (
  SELECT s.global_id, s.canvas_id, s.name, s.sortable_name, s.sis_user_id, s.sis_login_id, s.uid FROM (
    SELECT
        (1072 * 10000000000000 + u.id) AS global_id,
        u.id as canvas_id,
        u.name,
        u.sortable_name,
        p.sis_user_id,
        p.unique_id AS sis_login_id,
        /* Extract a numeric-string UID from Canvas sis_login_id if possible, otherwise leave blank. */
        CASE
            WHEN REPLACE(p.unique_id, 'inactive-', '') !~ '[A-Za-z]'
            THEN REPLACE(p.unique_id, 'inactive-', '')
        ELSE NULL END
            AS uid,
        ROW_NUMBER() OVER(PARTITION BY uid ORDER BY p.unique_id ASC) AS rk
    FROM
        {redshift_schema_canvas_data_2}.users u
    LEFT JOIN {redshift_schema_canvas_data_2}.pseudonyms p ON u.id = p.user_id
    WHERE
       p.workflow_state = 'active'
    ORDER BY uid
  ) s WHERE s.rk = 1
);

/*
 * Collect all active student Canvas course site enrollments and note SIS enrollment status, if any, in SIS sections integrated
 * with the course site.
 */
 
CREATE TABLE {redshift_schema_intermediate}.active_student_enrollments
INTERLEAVED SORTKEY (uid, canvas_course_id)
AS (
    SELECT
        u.uid AS uid,
        u.canvas_id AS canvas_user_id,
        c.id AS canvas_course_id,
        e.id AS canvas_enrollment_id,
        e.last_activity_at AS last_activity_at,
        /*
         * MIN ordering happens to match our desired precedence when reconciling enrollment status among multiple
         * sections: 'E', then 'W', then NULL.
         */
        MIN(se.sis_enrollment_status) AS sis_enrollment_status,
        c.name AS canvas_course_name,
        c.course_code AS canvas_course_code,
        cs.canvas_course_term,
        CASE LEFT(cs.canvas_course_term, 4)
          WHEN 'Spri' THEN '2' || RIGHT(cs.canvas_course_term, 2) || '2'
          WHEN 'Summ' THEN '2' || RIGHT(cs.canvas_course_term, 2) || '5'
          WHEN 'Fall' THEN '2' || RIGHT(cs.canvas_course_term, 2) || '8'
          ELSE NULL
        END AS term_id,
        LISTAGG(DISTINCT se.sis_section_id, ',')
          WITHIN GROUP (ORDER BY se.sis_section_id) AS sis_section_ids
    FROM
        {redshift_schema_canvas_data_2}.enrollments e
        JOIN {redshift_schema_intermediate}.users u 
            ON u.canvas_id = e.user_id
        JOIN {redshift_schema_canvas_data_2}.courses c
            ON c.id = e.course_id
        LEFT JOIN {redshift_schema_intermediate}.course_sections cs
            ON c.id = cs.canvas_course_id
        LEFT JOIN {redshift_schema_intermediate}.sis_enrollments se ON
            cs.sis_section_id = se.sis_section_id AND
            cs.sis_term_id = se.sis_term_id AND
            se.ldap_uid = u.uid
    WHERE
        e.type = 'StudentEnrollment'
        AND e.workflow_state in ('active', 'completed')
        AND c.workflow_state in ('available', 'completed')
    GROUP BY
        u.uid,
        u.canvas_id,
        c.id,
        e.id,
        e.last_activity_at,
        c.name,
        c.course_code,
        cs.canvas_course_term,
        term_id
);
