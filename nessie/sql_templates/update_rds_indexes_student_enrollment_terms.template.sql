/**
 * Copyright ©2022. The Regents of the University of California (Regents). All Rights Reserved.
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


BEGIN TRANSACTION;

DROP INDEX IF EXISTS {rds_schema_student}.students_enrollment_terms_midpoint_deficient_grade;
DROP INDEX IF EXISTS {rds_schema_student}.students_enrollment_terms_incomplete_grade;
DROP INDEX IF EXISTS {rds_schema_student}.students_enrollment_terms_enrolled_units;
DROP INDEX IF EXISTS {rds_schema_student}.students_enrollment_terms_term_gpa;
DROP INDEX IF EXISTS {rds_schema_student}.students_enrollment_terms_epn_grading_option;

DROP INDEX IF EXISTS {rds_schema_student}.students_term_gpa_sid_idx;
DROP INDEX IF EXISTS {rds_schema_student}.students_term_gpa_term_idx;
DROP INDEX IF EXISTS {rds_schema_student}.students_term_gpa_gpa_idx;
DROP INDEX IF EXISTS {rds_schema_student}.students_term_gpa_units_idx;

TRUNCATE {rds_schema_student}.student_enrollment_terms;
TRUNCATE {rds_schema_student}.student_term_gpas;

CREATE INDEX students_enrollment_terms_midpoint_deficient_grade
ON {rds_schema_student}.student_enrollment_terms (midpoint_deficient_grade);
CREATE INDEX students_enrollment_terms_incomplete_grade
ON {rds_schema_student}.student_enrollment_terms (incomplete_grade);
CREATE INDEX students_enrollment_terms_enrolled_units
ON {rds_schema_student}.student_enrollment_terms (enrolled_units);
CREATE INDEX students_enrollment_terms_term_gpa
ON {rds_schema_student}.student_enrollment_terms (term_gpa);
CREATE INDEX students_enrollment_terms_epn_grading_option
ON {rds_schema_student}.student_enrollment_terms (epn_grading_option);

CREATE INDEX students_term_gpa_sid_idx ON {rds_schema_student}.student_term_gpas (sid);
CREATE INDEX students_term_gpa_term_idx ON {rds_schema_student}.student_term_gpas (term_id);
CREATE INDEX students_term_gpa_gpa_idx ON {rds_schema_student}.student_term_gpas (gpa);
CREATE INDEX students_term_gpa_units_idx ON {rds_schema_student}.student_term_gpas (units_taken_for_gpa);

INSERT INTO {rds_schema_student}.student_enrollment_terms (
  SELECT * FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
      SELECT sid, term_id, enrollment_term,
          CHARINDEX('"midtermGrade": "', enrollment_term) != 0 AS midpoint_deficient_grade,
          (CHARINDEX('"incompleteStatusCode": "I"', enrollment_term) != 0
              AND CHARINDEX ('"incompleteFrozenFlag": "Y"', enrollment_term) = 0)
              AS incomplete_grade,
          json_extract_path_text(enrollment_term, 'enrolledUnits')::decimal(3,1) AS enrolled_units,
          CASE NULLIF(json_extract_path_text(enrollment_term, 'termGpa', 'unitsTakenForGpa'), '')::decimal(4,1) > 0
              WHEN TRUE THEN NULLIF(json_extract_path_text(enrollment_term, 'termGpa', 'gpa'), '')::decimal(5,3)
              ELSE NULL END
              AS term_gpa,
          CHARINDEX('"gradingBasis": "EPN"', enrollment_term) != 0 AS epn_grading_option
      FROM {redshift_schema_student}.student_enrollment_terms
      $REDSHIFT$)
  AS redshift_enrollment_terms (
      sid VARCHAR,
      term_id VARCHAR,
      enrollment_term TEXT,
      midpoint_deficient_grade BOOLEAN,
      incomplete_grade BOOLEAN,
      enrolled_units DECIMAL,
      term_gpa DECIMAL,
      epn_grading_option BOOLEAN 
  )
);

INSERT INTO {rds_schema_student}.student_term_gpas
(sid, term_id, gpa, units_taken_for_gpa)
SELECT
    sid, term_id, term_gpa AS gpa,
    (enrollment_term::json->'termGpa'->>'unitsTakenForGpa')::decimal(4,1) AS units_taken_for_gpa
FROM {rds_schema_student}.student_enrollment_terms
WHERE term_gpa IS NOT NULL;

COMMIT TRANSACTION;
