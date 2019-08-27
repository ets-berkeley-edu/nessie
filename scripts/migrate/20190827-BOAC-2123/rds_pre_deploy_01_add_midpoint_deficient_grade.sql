BEGIN TRANSACTION;

ALTER TABLE student.student_enrollment_terms ADD COLUMN midpoint_deficient_grade BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX student_enrollment_terms_midpoint_deficient_grade ON student.student_enrollment_terms (midpoint_deficient_grade);

COMMIT;
