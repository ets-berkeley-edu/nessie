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
    FROM {redshift_schema}.advising_notes
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
    FROM {redshift_schema}.advising_note_attachments
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
    FROM {redshift_schema}.advising_note_topics
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
  SELECT id, to_tsvector('english', COALESCE(course_display_name || ' ' || course_title || ' ' || eform_type || ' ' || requested_action, '')) AS fts_index
  FROM {rds_schema_sis_advising_notes}.student_late_drop_eforms
);

CREATE INDEX idx_student_late_drop_eforms_ft_search
ON {rds_schema_sis_advising_notes}.student_late_drop_eforms_search_index
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
    FROM {redshift_schema}.advising_notes
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

DROP TABLE IF EXISTS {rds_schema_sis_advising_notes}.advising_appointment_advisors CASCADE;

CREATE TABLE {rds_schema_sis_advising_notes}.advising_appointment_advisors
(
    uid VARCHAR NOT NULL,
    sid VARCHAR NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    PRIMARY KEY (uid)
);

CREATE INDEX IF NOT EXISTS advising_appointment_advisors_sid_idx
ON {rds_schema_sis_advising_notes}.advising_appointment_advisors (sid);

DROP MATERIALIZED VIEW IF EXISTS {rds_schema_sis_advising_notes}.advising_appointments_search_index CASCADE;

CREATE MATERIALIZED VIEW {rds_schema_sis_advising_notes}.advising_appointments_search_index AS (
  SELECT id, to_tsvector(
    'english',
    CASE
      WHEN note_body IS NOT NULL and TRIM(note_body) != '' THEN note_body
      WHEN note_subcategory IS NOT NULL THEN note_category || ' ' || note_subcategory
      ELSE note_category
    END
  ) AS fts_index
  FROM {rds_schema_sis_advising_notes}.advising_appointments
);

CREATE INDEX idx_advising_appointments_ft_search
ON {rds_schema_sis_advising_notes}.advising_appointments_search_index
USING gin(fts_index);

COMMIT TRANSACTION;
