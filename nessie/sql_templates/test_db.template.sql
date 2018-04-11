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

--------------------------------------------------------------------
-- Test template for standard (not EXTERNAL) SCHEMA and TABLE
--------------------------------------------------------------------

DROP SCHEMA IF EXISTS {redshift_schema_boac} CASCADE;

CREATE SCHEMA {redshift_schema_boac};

DROP TABLE IF EXISTS {redshift_schema_boac}.students;
CREATE TABLE {redshift_schema_boac}.students (
  sid character varying(80) NOT NULL,
  uid character varying(80),
  first_name character varying(255) NOT NULL,
  last_name character varying(255) NOT NULL,
  in_intensive_cohort boolean DEFAULT false NOT NULL,
  is_active_asc boolean DEFAULT true NOT NULL,
  status_asc character varying(80),
  created_at timestamp with time zone NOT NULL,
  updated_at timestamp with time zone NOT NULL
);
INSERT INTO {redshift_schema_boac}.students
  VALUES ('11667051', '61889', 'Brigitte', 'Lin', true, true, NULL,
		  '2018-03-07 15:19:17.18972-08', '2018-03-07 15:19:17.189727-08');
INSERT INTO {redshift_schema_boac}.students
  VALUES ('8901234567', '1022796', 'John', 'Crossman', true, true, NULL,
		  '2018-03-07 15:19:17.200198-08', '2018-03-07 15:19:17.200205-08');
INSERT INTO {redshift_schema_boac}.students
  VALUES ('2345678901', '2040', 'Oliver', 'Heyer', false, true, NULL,
		  '2018-03-07 15:19:17.204676-08', '2018-03-07 15:19:17.204682-08');
INSERT INTO {redshift_schema_boac}.students
  VALUES ('3456789012', '242881', 'Paul', 'Kerschen', true, true, NULL,
		  '2018-03-07 15:19:17.214502-08', '2018-03-07 15:19:17.214507-08');
INSERT INTO {redshift_schema_boac}.students
  VALUES ('5678901234', '1133399', 'Sandeep', 'Jayaprakash', false, true, NULL,
		  '2018-03-07 15:19:17.220223-08', '2018-03-07 15:19:17.220229-08');
INSERT INTO {redshift_schema_boac}.students
  VALUES ('7890123456', '1049291', 'Paul', 'Farestveit', true, true, NULL,
		  '2018-03-07 15:19:17.228017-08', '2018-03-07 15:19:17.228024-08');
INSERT INTO {redshift_schema_boac}.students
  VALUES ('838927492', '211159', 'Siegfried', 'Schlemiel', true, false, 'Trouble',
		  '2018-03-07 15:19:17.234875-08', '2018-03-07 15:19:17.240878-08');
ALTER TABLE {redshift_schema_boac}.students ADD CONSTRAINT students_pkey PRIMARY KEY (sid);
