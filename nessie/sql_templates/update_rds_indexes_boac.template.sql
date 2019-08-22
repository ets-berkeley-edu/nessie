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

CREATE SCHEMA IF NOT EXISTS {rds_schema_boac};
GRANT USAGE ON SCHEMA {rds_schema_boac} TO {rds_app_boa_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_boac} GRANT SELECT ON TABLES TO {rds_app_boa_user};

DROP TABLE IF EXISTS {rds_schema_boac}.section_mean_gpas CASCADE;

CREATE TABLE {rds_schema_boac}.section_mean_gpas
(
    sis_term_id VARCHAR,
    sis_section_id VARCHAR,
    gpa_term_id VARCHAR,
    avg_gpa DOUBLE PRECISION
    PRIMARY KEY (sis_term_id, sis_section_id)
);

INSERT INTO {rds_schema_boac}.section_mean_gpas (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT sis_term_id, sis_section_id, gpa_term_id, avg_gpa
    FROM {redshift_schema_boac}.section_mean_gpas
  $REDSHIFT$)
  AS redshift_section_mean_gpas (
    sis_term_id VARCHAR,
    sis_section_id VARCHAR,
    gpa_term_id VARCHAR,
    avg_gpa DOUBLE PRECISION
  )
);
