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

DROP TABLE IF EXISTS {redshift_schema_sisedo_internal}.enrollment_updates;

CREATE TABLE {redshift_schema_sisedo_internal}.enrollment_updates
SORTKEY (sis_term_id, sis_section_id, ldap_uid)
AS (
  SELECT DISTINCT
    term_id AS sis_term_id,
    class_section_id AS sis_section_id,
    ldap_uid,
    sis_id,
    enroll_status AS sis_enrollment_status,
    course_career,
    last_updated
  FROM {redshift_schema_sisedo}.enrollment_updates
);

DROP TABLE IF EXISTS {redshift_schema_sisedo_internal}.instructor_updates;

CREATE TABLE {redshift_schema_sisedo_internal}.instructor_updates
SORTKEY (sis_term_id, sis_section_id, ldap_uid)
AS (
  SELECT DISTINCT
    term_id AS sis_term_id,
    course_id AS sis_course_id,
    section_id AS sis_section_id,
    ldap_uid,
    sis_id,
    role_code,
    is_primary,
    last_updated
  FROM {redshift_schema_sisedo}.instructor_updates
);
