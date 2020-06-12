/**
 * Copyright ©2020. The Regents of the University of California (Regents). All Rights Reserved.
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

CREATE SCHEMA IF NOT EXISTS {rds_schema_data_science};
GRANT USAGE ON SCHEMA {rds_schema_data_science} TO {rds_app_boa_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_data_science} GRANT SELECT ON TABLES TO {rds_app_boa_user};

BEGIN TRANSACTION;

DROP TABLE IF EXISTS {rds_schema_data_science}.advising_notes CASCADE;

CREATE TABLE {rds_schema_data_science}.advising_notes (
  id VARCHAR NOT NULL,
  sid VARCHAR NOT NULL,
  student_first_name VARCHAR,
  student_last_name VARCHAR,
  advisor_email VARCHAR,
  reason_for_appointment VARCHAR,
  conversation_type VARCHAR,
  body VARCHAR,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL,
  PRIMARY KEY (id)
);

INSERT INTO {rds_schema_data_science}.advising_notes (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT DISTINCT id, sid, student_first_name, student_last_name,
    advisor_email, reason_for_appointment, conversation_type, body, created_at
    FROM {redshift_schema_data_science_advising_internal}.advising_notes N
    WHERE sid <> ''
  $REDSHIFT$)
  AS redshift_notes (
    id VARCHAR,
    sid VARCHAR,
    student_first_name VARCHAR,
    student_last_name VARCHAR,
    advisor_email VARCHAR,
    reason_for_appointment VARCHAR,
    conversation_type VARCHAR,
    body VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE
  )
);

CREATE INDEX idx_e_i_advising_notes_sid ON {rds_schema_data_science}.advising_notes(sid);
CREATE INDEX idx_e_i_advising_notes_advisor_email ON {rds_schema_data_science}.advising_notes(advisor_email);
CREATE INDEX idx_e_i_advising_notes_created_at ON {rds_schema_data_science}.advising_notes(created_at);

COMMIT TRANSACTION;
