BEGIN TRANSACTION;

ALTER TABLE student.student_enrollment_terms ADD COLUMN term_gpa DECIMAL (5,3);

CREATE INDEX students_enrollment_terms_term_gpa ON student.student_enrollment_terms (term_gpa);

UPDATE student.student_enrollment_terms
  SET term_gpa = (enrollment_term::json->'termGpa'->>'gpa')::numeric
  WHERE (enrollment_term::json->'termGpa'->>'unitsTakenForGpa')::numeric > 0;

COMMIT;
