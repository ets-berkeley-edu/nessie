/**
 * Copyright ©2023. The Regents of the University of California (Regents). All Rights Reserved.
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


DROP TABLE IF EXISTS {rds_schema_sis_internal}.edo_enrollment_updates CASCADE;

CREATE TABLE IF NOT EXISTS {rds_schema_sis_internal}.edo_enrollment_updates
(
    sis_term_id VARCHAR,
    sis_section_id VARCHAR,
    ldap_uid VARCHAR,
    sis_id VARCHAR,
    sis_enrollment_status VARCHAR,
    course_career VARCHAR,
    last_updated VARCHAR
);

INSERT INTO {rds_schema_sis_internal}.edo_enrollment_updates (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT sis_term_id, sis_section_id, ldap_uid, sis_id,
      sis_enrollment_status, course_career, last_updated
    FROM {redshift_schema_sisedo_internal}.enrollment_updates
  $REDSHIFT$)
  AS redshift_edo_enrollment_updates (
    sis_term_id VARCHAR,
    sis_section_id VARCHAR,
    ldap_uid VARCHAR,
    sis_id VARCHAR,
    sis_enrollment_status VARCHAR,
    course_career VARCHAR,
    last_updated VARCHAR
  )
);

CREATE INDEX idx_edo_enrollment_updates_term_id_section_id ON {rds_schema_sis_internal}.edo_enrollment_updates(sis_term_id, sis_section_id);
CREATE INDEX idx_edo_enrollment_updates_ldap_uid ON {rds_schema_sis_internal}.edo_enrollment_updates(ldap_uid);

DROP TABLE IF EXISTS {rds_schema_sis_internal}.edo_instructor_updates CASCADE;

CREATE TABLE IF NOT EXISTS {rds_schema_sis_internal}.edo_instructor_updates
(
    sis_term_id VARCHAR,
    sis_course_id VARCHAR,
    sis_section_id VARCHAR,
    ldap_uid VARCHAR,
    sis_id VARCHAR,
    role_code VARCHAR,
    is_primary BOOLEAN,
    last_updated VARCHAR
);

INSERT INTO {rds_schema_sis_internal}.edo_instructor_updates (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT sis_term_id, sis_course_id, sis_section_id,
      ldap_uid, sis_id, role_code, is_primary, last_updated
    FROM {redshift_schema_sisedo_internal}.instructor_updates
  $REDSHIFT$)
  AS redshift_edo_instructor_updates(
    sis_term_id VARCHAR,
    sis_course_id VARCHAR,
    sis_section_id VARCHAR,
    ldap_uid VARCHAR,
    sis_id VARCHAR,
    role_code VARCHAR,
    is_primary BOOLEAN,
    last_updated VARCHAR
  )
);

CREATE INDEX idx_edo_instructor_updates_term_id_section_id ON {rds_schema_sis_internal}.edo_instructor_updates(sis_term_id, sis_section_id);
CREATE INDEX idx_edo_instructor_updates_ldap_uid ON {rds_schema_sis_internal}.edo_instructor_updates(ldap_uid);
