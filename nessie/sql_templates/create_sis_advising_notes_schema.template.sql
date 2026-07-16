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

CREATE SCHEMA IF NOT EXISTS {rds_schema_sis_advising_notes};
GRANT USAGE ON SCHEMA {rds_schema_sis_advising_notes} TO {rds_app_boa_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_sis_advising_notes} GRANT SELECT ON TABLES TO {rds_app_boa_user};

BEGIN TRANSACTION;

DROP TABLE IF EXISTS {rds_schema_sis_advising_notes}.advising_notes CASCADE;

CREATE TABLE {rds_schema_sis_advising_notes}.advising_notes (
  id VARCHAR NOT NULL,
  sid VARCHAR NOT NULL,
  student_note_nr INTEGER NOT NULL,
  advisor_sid VARCHAR NOT NULL,
  appointment_id VARCHAR,
  note_category VARCHAR,
  note_subcategory VARCHAR,
  note_body TEXT,
  created_by VARCHAR,
  updated_by VARCHAR,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL,
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
  PRIMARY KEY (id)
);

INSERT INTO {rds_schema_sis_advising_notes}.advising_notes (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT id, sid, student_note_nr, advisor_sid, appointment_id, note_category, note_subcategory, note_body,
           created_by, updated_by, created_at, updated_at
    FROM {redshift_schema_edl}.advising_notes
    WHERE note_category <> 'Appointment Type'
    ORDER BY updated_at DESC
  $REDSHIFT$)
  AS redshift_notes (
    id VARCHAR,
    sid VARCHAR,
    student_note_nr INTEGER,
    advisor_sid VARCHAR,
    appointment_id VARCHAR,
    note_category VARCHAR,
    note_subcategory VARCHAR,
    note_body TEXT,
    created_by VARCHAR,
    updated_by VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
  )
);

CREATE INDEX idx_sis_advising_notes_advisor_sid ON {rds_schema_sis_advising_notes}.advising_notes(advisor_sid);
CREATE INDEX idx_sis_advising_notes_created_at ON {rds_schema_sis_advising_notes}.advising_notes(created_at);
CREATE INDEX idx_sis_advising_notes_created_by ON {rds_schema_sis_advising_notes}.advising_notes(created_by);
CREATE INDEX idx_sis_advising_notes_sid ON {rds_schema_sis_advising_notes}.advising_notes(sid);
CREATE INDEX idx_sis_advising_notes_updated_at ON {rds_schema_sis_advising_notes}.advising_notes(updated_at);

--

DROP TABLE IF EXISTS {rds_schema_sis_advising_notes}.advising_note_attachments CASCADE;

CREATE TABLE {rds_schema_sis_advising_notes}.advising_note_attachments (
    advising_note_id VARCHAR,
    sid VARCHAR,
    student_note_nr VARCHAR,
    created_by VARCHAR,
    user_file_name VARCHAR,
    sis_file_name VARCHAR,
    -- TODO Get rid of the all-FALSE is_historical column after BOA 5.4 production release.
    is_historical BOOLEAN NOT NULL DEFAULT FALSE
);


INSERT INTO {rds_schema_sis_advising_notes}.advising_note_attachments (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT advising_note_id, sid, student_note_nr, created_by, user_file_name, sis_file_name
    FROM {redshift_schema_edl}.advising_note_attachments
  $REDSHIFT$)
  AS redshift_notes (
    advising_note_id VARCHAR,
    sid VARCHAR,
    student_note_nr VARCHAR,
    created_by VARCHAR,
    user_file_name VARCHAR,
    sis_file_name VARCHAR
  )
);

CREATE INDEX idx_sis_advising_note_attachments_advising_note_id
ON {rds_schema_sis_advising_notes}.advising_note_attachments(advising_note_id);

--

DROP TABLE IF EXISTS {rds_schema_sis_advising_notes}.advising_note_topics CASCADE;

CREATE TABLE {rds_schema_sis_advising_notes}.advising_note_topics (
  advising_note_id VARCHAR NOT NULL,
  sid VARCHAR,
  student_note_nr VARCHAR,
  note_topic VARCHAR NOT NULL,
  PRIMARY KEY (advising_note_id, note_topic)
);

INSERT INTO {rds_schema_sis_advising_notes}.advising_note_topics (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT DISTINCT advising_note_id, sid, student_note_nr, note_topic
    FROM {redshift_schema_edl}.advising_note_topics
    WHERE note_topic IS NOT NULL
  $REDSHIFT$)
  AS redshift_notes (
    advising_note_id VARCHAR,
    sid VARCHAR,
    student_note_nr VARCHAR,
    note_topic VARCHAR
  )
);

CREATE INDEX idx_sis_advising_note_topics_note_id ON {rds_schema_sis_advising_notes}.advising_note_topics(advising_note_id);
CREATE INDEX idx_sis_advising_note_topics_sid ON {rds_schema_sis_advising_notes}.advising_note_topics(sid);
CREATE INDEX idx_sis_advising_note_topics_topic ON {rds_schema_sis_advising_notes}.advising_note_topics(note_topic);

--

DROP MATERIALIZED VIEW IF EXISTS {rds_schema_sis_advising_notes}.advising_notes_search_index CASCADE;

CREATE MATERIALIZED VIEW {rds_schema_sis_advising_notes}.advising_notes_search_index AS (
  SELECT id, to_tsvector(
    'english',
    CASE
      WHEN note_body IS NOT NULL and TRIM(note_body) != '' THEN note_body
      WHEN note_subcategory IS NOT NULL THEN note_category || ' ' || note_subcategory
      ELSE note_category
    END
  ) AS fts_index
  FROM {rds_schema_sis_advising_notes}.advising_notes
);

CREATE INDEX idx_advising_notes_ft_search
ON {rds_schema_sis_advising_notes}.advising_notes_search_index
USING gin(fts_index);

--

DROP TABLE IF EXISTS {rds_schema_sis_advising_notes}.student_late_drop_eforms CASCADE;

CREATE TABLE {rds_schema_sis_advising_notes}.student_late_drop_eforms
(
    id VARCHAR,
    career_code VARCHAR,
    course_display_name VARCHAR,
    course_title VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE,
    edl_load_date VARCHAR,
    eform_id INTEGER,
    eform_status VARCHAR,
    eform_type VARCHAR,
    grading_basis_code VARCHAR,
    grading_basis_description VARCHAR,
    requested_action VARCHAR,
    requested_grading_basis_code VARCHAR,
    requested_grading_basis_description VARCHAR,
    requested_units_taken VARCHAR,
    section_id INTEGER,
    section_num VARCHAR,
    sid VARCHAR NOT NULL,
    student_name VARCHAR,
    term_id VARCHAR(4),
    units_taken VARCHAR,
    updated_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (id)
);

INSERT INTO {rds_schema_sis_advising_notes}.student_late_drop_eforms (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT
        id, career_code, course_display_name, course_title, created_at, edl_load_date, eform_id, eform_status, eform_type,
        grading_basis_code, grading_basis_description, requested_action, requested_grading_basis_code,
        requested_grading_basis_description, requested_units_taken, section_id, section_num, sid, student_name, term_id, units_taken,
        updated_at
    FROM {redshift_schema_edl}.student_late_drop_eforms
    ORDER BY created_at
  $REDSHIFT$)
  AS redshift_student_late_drop_eforms (
    id VARCHAR,
    career_code VARCHAR,
    course_display_name VARCHAR,
    course_title VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE,
    edl_load_date VARCHAR,
    eform_id INTEGER,
    eform_status VARCHAR,
    eform_type VARCHAR,
    grading_basis_code VARCHAR,
    grading_basis_description VARCHAR,
    requested_action VARCHAR,
    requested_grading_basis_code VARCHAR,
    requested_grading_basis_description VARCHAR,
    requested_units_taken VARCHAR,
    section_id INTEGER,
    section_num VARCHAR,
    sid VARCHAR,
    student_name VARCHAR,
    term_id VARCHAR(4),
    units_taken VARCHAR,
    updated_at TIMESTAMP WITH TIME ZONE
  )
);

CREATE INDEX idx_student_late_drop_eforms_id ON {rds_schema_sis_advising_notes}.student_late_drop_eforms(id);
CREATE INDEX idx_student_late_drop_eforms_created_at ON {rds_schema_sis_advising_notes}.student_late_drop_eforms(created_at);
CREATE INDEX idx_student_late_drop_eforms_sid ON {rds_schema_sis_advising_notes}.student_late_drop_eforms(sid);
CREATE INDEX idx_student_late_drop_eforms_updated_at ON {rds_schema_sis_advising_notes}.student_late_drop_eforms(updated_at);

--

DROP MATERIALIZED VIEW IF EXISTS {rds_schema_sis_advising_notes}.student_late_drop_eforms_search_index CASCADE;

CREATE MATERIALIZED VIEW {rds_schema_sis_advising_notes}.student_late_drop_eforms_search_index AS (
  SELECT id, to_tsvector('english',
      COALESCE(grading_basis_description, '') || ' ' ||
      COALESCE(requested_grading_basis_description, '') || ' ' ||
      course_display_name || ' ' || course_title || ' ' || requested_action || ' ' ||
      eform_type || ' ' || eform_id || ' ' || eform_status
    ) AS fts_index
  FROM {rds_schema_sis_advising_notes}.student_late_drop_eforms
);

CREATE INDEX idx_student_late_drop_eforms_ft_search
ON {rds_schema_sis_advising_notes}.student_late_drop_eforms_search_index
USING gin(fts_index);

--

DROP TABLE IF EXISTS {rds_schema_sis_advising_notes}.student_course_load_eforms CASCADE;

CREATE TABLE {rds_schema_sis_advising_notes}.student_course_load_eforms
(
    id VARCHAR,
    academic_career_code VARCHAR,
    academic_standing_status VARCHAR,
    academic_standing_description VARCHAR,
    eform_id INTEGER,
    eform_last_user_uid VARCHAR,
    eform_last_user_name VARCHAR,
    eform_orig_user_name VARCHAR,
    eform_status VARCHAR,
    eform_type VARCHAR,
    request_type VARCHAR,
    request_type_description VARCHAR,
    requested_reduced_units VARCHAR,
    sid VARCHAR NOT NULL,
    term_enrolled_units VARCHAR,
    term_id VARCHAR(4),
    term_waitlist_units VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (id)
);

INSERT INTO {rds_schema_sis_advising_notes}.student_course_load_eforms (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT
        id, academic_career_code, academic_standing_status, academic_standing_description, eform_id,
        eform_last_user_uid, eform_last_user_name, eform_orig_user_name, eform_status, eform_type, request_type,
        request_type_description, requested_reduced_units, sid, term_enrolled_units, term_id, term_waitlist_units,
        created_at, updated_at
    FROM {redshift_schema_edl}.student_course_load_eforms
    ORDER BY created_at
  $REDSHIFT$)
  AS redshift_student_course_load_eforms (
    id VARCHAR,
    academic_career_code VARCHAR,
    academic_standing_status VARCHAR,
    academic_standing_description VARCHAR,
    eform_id INTEGER,
    eform_last_user_uid VARCHAR,
    eform_last_user_name VARCHAR,
    eform_orig_user_name VARCHAR,
    eform_status VARCHAR,
    eform_type VARCHAR,
    request_type VARCHAR,
    request_type_description VARCHAR,
    requested_reduced_units VARCHAR,
    sid VARCHAR,
    term_enrolled_units VARCHAR,
    term_id VARCHAR(4),
    term_waitlist_units VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
  )
);

CREATE INDEX idx_student_course_load_eforms_id ON {rds_schema_sis_advising_notes}.student_course_load_eforms(id);
CREATE INDEX idx_student_course_load_eforms_created_at ON {rds_schema_sis_advising_notes}.student_course_load_eforms(created_at);
CREATE INDEX idx_student_course_load_eforms_sid ON {rds_schema_sis_advising_notes}.student_course_load_eforms(sid);
CREATE INDEX idx_student_course_load_eforms_updated_at ON {rds_schema_sis_advising_notes}.student_course_load_eforms(updated_at);

--

DROP MATERIALIZED VIEW IF EXISTS {rds_schema_sis_advising_notes}.student_course_load_eforms_search_index CASCADE;

CREATE MATERIALIZED VIEW {rds_schema_sis_advising_notes}.student_course_load_eforms_search_index AS (
  SELECT id, to_tsvector('english',
      COALESCE(academic_standing_description, '') || ' ' ||
      request_type || ' ' || request_type_description || ' ' ||
      eform_type || ' ' || eform_id || ' ' || eform_status
    ) AS fts_index
  FROM {rds_schema_sis_advising_notes}.student_course_load_eforms
);

CREATE INDEX idx_student_course_load_eforms_ft_search
ON {rds_schema_sis_advising_notes}.student_course_load_eforms_search_index
USING gin(fts_index);

--

DROP TABLE IF EXISTS {rds_schema_sis_advising_notes}.student_cpp_change_eforms CASCADE;

CREATE TABLE {rds_schema_sis_advising_notes}.student_cpp_change_eforms
(
    id VARCHAR,
    academic_career_code VARCHAR,
    academic_plan_code VARCHAR,
    academic_plan_name VARCHAR,
    academic_plan_type_description VARCHAR,
    academic_program_code VARCHAR,
    academic_program_name VARCHAR,
    academic_subplan_code VARCHAR,
    academic_subplan_name VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE,
    degree_expected_term_id VARCHAR(4),
    eform_action_code VARCHAR,
    eform_action_description VARCHAR,
    eform_id INTEGER,
    eform_status VARCHAR,
    eform_type VARCHAR,
    overlap_course_1 VARCHAR,
    overlap_course_2 VARCHAR,
    overlap_course_3 VARCHAR,
    overlap_course_4 VARCHAR,
    overlap_course_5 VARCHAR,
    requirement_term_id VARCHAR(4),
    sid VARCHAR NOT NULL,
    student_name VARCHAR,
    to_academic_plan_code VARCHAR,
    to_academic_plan_name VARCHAR,
    to_academic_plan_requirement_term_id VARCHAR(4),
    to_academic_program_code VARCHAR,
    to_academic_program_name VARCHAR,
    to_academic_subplan_code VARCHAR,
    to_academic_subplan_name VARCHAR,
    to_academic_subplan_requirement_term_id VARCHAR(4),
    to_degree_expected_term_id VARCHAR(4),
    to_requirement_term_id VARCHAR(4),
    updated_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (id)
);

INSERT INTO {rds_schema_sis_advising_notes}.student_cpp_change_eforms (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT
        id, academic_career_code, academic_plan_code, academic_plan_name, academic_plan_type_description,
        academic_program_code, academic_program_name, academic_subplan_code, academic_subplan_name, created_at,
        degree_expected_term_id, eform_action_code, eform_action_description, eform_id, eform_status, eform_type,
        overlap_course_1, overlap_course_2, overlap_course_3, overlap_course_4, overlap_course_5, requirement_term_id,
        sid, student_name, to_academic_plan_code, to_academic_plan_name, to_academic_plan_requirement_term_id,
        to_academic_program_code, to_academic_program_name, to_academic_subplan_code, to_academic_subplan_name,
        to_academic_subplan_requirement_term_id, to_degree_expected_term_id, to_requirement_term_id, updated_at
    FROM {redshift_schema_edl}.student_cpp_change_eforms
    ORDER BY created_at
  $REDSHIFT$)
  AS redshift_student_cpp_change_eforms (
    id VARCHAR,
    academic_career_code VARCHAR,
    academic_plan_code VARCHAR,
    academic_plan_name VARCHAR,
    academic_plan_type_description VARCHAR,
    academic_program_code VARCHAR,
    academic_program_name VARCHAR,
    academic_subplan_code VARCHAR,
    academic_subplan_name VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE,
    degree_expected_term_id VARCHAR(4),
    eform_action_code VARCHAR,
    eform_action_description VARCHAR,
    eform_id INTEGER,
    eform_status VARCHAR,
    eform_type VARCHAR,
    overlap_course_1 VARCHAR,
    overlap_course_2 VARCHAR,
    overlap_course_3 VARCHAR,
    overlap_course_4 VARCHAR,
    overlap_course_5 VARCHAR,
    requirement_term_id VARCHAR(4),
    sid VARCHAR,
    student_name VARCHAR,
    to_academic_plan_code VARCHAR,
    to_academic_plan_name VARCHAR,
    to_academic_plan_requirement_term_id VARCHAR(4),
    to_academic_program_code VARCHAR,
    to_academic_program_name VARCHAR,
    to_academic_subplan_code VARCHAR,
    to_academic_subplan_name VARCHAR,
    to_academic_subplan_requirement_term_id VARCHAR(4),
    to_degree_expected_term_id VARCHAR(4),
    to_requirement_term_id VARCHAR(4),
    updated_at TIMESTAMP WITH TIME ZONE
  )
);

CREATE INDEX idx_student_cpp_change_eforms_id ON {rds_schema_sis_advising_notes}.student_cpp_change_eforms(id);
CREATE INDEX idx_student_cpp_change_eforms_created_at ON {rds_schema_sis_advising_notes}.student_cpp_change_eforms(created_at);
CREATE INDEX idx_student_cpp_change_eforms_sid ON {rds_schema_sis_advising_notes}.student_cpp_change_eforms(sid);
CREATE INDEX idx_student_cpp_change_eforms_updated_at ON {rds_schema_sis_advising_notes}.student_cpp_change_eforms(updated_at);

--

DROP MATERIALIZED VIEW IF EXISTS {rds_schema_sis_advising_notes}.student_cpp_change_eforms_search_index CASCADE;

CREATE MATERIALIZED VIEW {rds_schema_sis_advising_notes}.student_cpp_change_eforms_search_index AS (
  SELECT id, to_tsvector('english',
      COALESCE(academic_program_name, '') || ' ' ||
      COALESCE(academic_plan_name, '') || ' ' ||
      COALESCE(academic_subplan_name, '') || ' ' ||
      COALESCE(to_academic_program_name, '') || ' ' ||
      COALESCE(to_academic_plan_name, '') || ' ' ||
      COALESCE(to_academic_subplan_name, '') || ' ' ||
      eform_type || ' ' || eform_action_description || ' ' || eform_id || ' ' || eform_status
    ) AS fts_index
  FROM {rds_schema_sis_advising_notes}.student_cpp_change_eforms
);

CREATE INDEX idx_student_cpp_change_eforms_ft_search
ON {rds_schema_sis_advising_notes}.student_cpp_change_eforms_search_index
USING gin(fts_index);

--

DROP TABLE IF EXISTS {rds_schema_sis_advising_notes}.advising_appointments CASCADE;

CREATE TABLE {rds_schema_sis_advising_notes}.advising_appointments (
  id VARCHAR NOT NULL,
  sid VARCHAR NOT NULL,
  student_note_nr INTEGER NOT NULL,
  advisor_sid VARCHAR NOT NULL,
  appointment_id VARCHAR,
  note_category VARCHAR,
  note_subcategory VARCHAR,
  note_body TEXT,
  created_by VARCHAR,
  updated_by VARCHAR,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL,
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
  PRIMARY KEY (id)
);

INSERT INTO {rds_schema_sis_advising_notes}.advising_appointments (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT id, sid, student_note_nr, advisor_sid, appointment_id, note_category, note_subcategory, note_body,
           created_by, updated_by, created_at, updated_at
    FROM {redshift_schema_edl}.advising_notes
    WHERE note_category = 'Appointment Type'
    ORDER BY updated_at DESC
  $REDSHIFT$)
  AS redshift_appointments (
    id VARCHAR,
    sid VARCHAR,
    student_note_nr INTEGER,
    advisor_sid VARCHAR,
    appointment_id VARCHAR,
    note_category VARCHAR,
    note_subcategory VARCHAR,
    note_body TEXT,
    created_by VARCHAR,
    updated_by VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
  )
);

CREATE INDEX idx_sis_advising_appointments_advisor_sid ON {rds_schema_sis_advising_notes}.advising_appointments(advisor_sid);
CREATE INDEX idx_sis_advising_appointments_created_at ON {rds_schema_sis_advising_notes}.advising_appointments(created_at);
CREATE INDEX idx_sis_advising_appointments_created_by ON {rds_schema_sis_advising_notes}.advising_appointments(created_by);
CREATE INDEX idx_sis_advising_appointments_sid ON {rds_schema_sis_advising_notes}.advising_appointments(sid);
CREATE INDEX idx_sis_advising_appointments_updated_at ON {rds_schema_sis_advising_notes}.advising_appointments(updated_at);

--

DROP MATERIALIZED VIEW IF EXISTS {rds_schema_sis_advising_notes}.advising_appointments_search_index CASCADE;

CREATE MATERIALIZED VIEW {rds_schema_sis_advising_notes}.advising_appointments_search_index AS (
  SELECT id, to_tsvector('english', COALESCE(note_body, '') || ' ' || note_subcategory) AS fts_index
  FROM {rds_schema_sis_advising_notes}.advising_appointments
);

CREATE INDEX idx_advising_appointments_ft_search
ON {rds_schema_sis_advising_notes}.advising_appointments_search_index
USING gin(fts_index);

--

DROP TABLE IF EXISTS {rds_schema_sis_advising_notes}.advisors CASCADE;

CREATE TABLE {rds_schema_sis_advising_notes}.advisors
(
    uid VARCHAR NOT NULL,
    sid VARCHAR,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    campus_email VARCHAR,
    PRIMARY KEY (uid)
);

CREATE INDEX IF NOT EXISTS advisors_sid_idx
ON {rds_schema_sis_advising_notes}.advisors (sid);

INSERT INTO {rds_schema_sis_advising_notes}.advisors (
  SELECT uid, MAX(sid) AS sid, first_name, last_name, MAX(campus_email) AS campus_email
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT DISTINCT ba.ldap_uid as uid, ba.sid, ba.first_name, ba.last_name, ba.email_address as campus_email
      FROM {redshift_schema_edl}.advising_notes an
      JOIN {redshift_schema_edl}.basic_attributes ba ON ba.sid = an.advisor_sid
    UNION
    SELECT ba.ldap_uid as uid, ba.sid, ba.first_name, ba.last_name, ba.email_address as campus_email
      FROM {redshift_schema_advisor_internal}.instructor_advisor ia
      JOIN {redshift_schema_edl}.basic_attributes ba ON ba.ldap_uid = ia.uid
    UNION
    SELECT ba.ldap_uid as uid, ba.sid, ba.first_name, ba.last_name, ba.email_address as campus_email
      FROM {redshift_schema_advisor_internal}.advisor_roles ar
      JOIN {redshift_schema_edl}.basic_attributes ba ON ba.ldap_uid = ar.uid
    UNION
    SELECT ldap_uid as uid, csid as sid, first_name, last_name, campus_email
      FROM {redshift_schema_advisor_internal}.advisor_attributes
  $REDSHIFT$)
  AS redshift_appointment_advisors (
    uid VARCHAR,
    sid VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    campus_email VARCHAR
  )
  GROUP BY uid, first_name, last_name
);

--

DROP TABLE IF EXISTS {rds_schema_sis_advising_notes}.advisor_names CASCADE;

CREATE TABLE {rds_schema_sis_advising_notes}.advisor_names
(
    uid VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    PRIMARY KEY (uid, name)
);

CREATE INDEX IF NOT EXISTS advisor_names_name_idx
ON {rds_schema_sis_advising_notes}.advisor_names (name);

INSERT INTO {rds_schema_sis_advising_notes}.advisor_names (
    SELECT DISTINCT uid, unnest(string_to_array(
        regexp_replace(upper(first_name), '[^\w ]', '', 'g'),
        ' '
    )) AS name FROM {rds_schema_sis_advising_notes}.advisors
    UNION
    SELECT DISTINCT uid, unnest(string_to_array(
        regexp_replace(upper(last_name), '[^\w ]', '', 'g'),
        ' '
    )) AS name FROM {rds_schema_sis_advising_notes}.advisors
);

COMMIT TRANSACTION;
