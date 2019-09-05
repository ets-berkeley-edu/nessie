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

DROP TABLE IF EXISTS {rds_schema_student}.student_profiles_hist_enr CASCADE;
CREATE TABLE IF NOT EXISTS {rds_schema_student}.student_profiles_hist_enr
(
    sid VARCHAR NOT NULL PRIMARY KEY,
    uid VARCHAR,
    profile TEXT NOT NULL
);

INSERT INTO {rds_schema_student}.student_profiles_hist_enr (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT *
    FROM {redshift_schema_student}.student_profiles_hist_enr
  $REDSHIFT$)
  AS redshift_profiles (
    sid VARCHAR,
    uid VARCHAR,
    profile TEXT
  )
);

DROP TABLE IF EXISTS {rds_schema_student}.student_enrollment_terms_hist_enr CASCADE;
CREATE TABLE IF NOT EXISTS {rds_schema_student}.student_enrollment_terms_hist_enr
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    enrollment_term TEXT NOT NULL,
    PRIMARY KEY (sid, term_id)
);

INSERT INTO {rds_schema_student}.student_enrollment_terms_hist_enr (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT *
    FROM {redshift_schema_student}.student_enrollment_terms_hist_enr
  $REDSHIFT$)
  AS redshift_profiles (
    sid VARCHAR,
    term_id VARCHAR,
    enrollment_term TEXT
  )
);
