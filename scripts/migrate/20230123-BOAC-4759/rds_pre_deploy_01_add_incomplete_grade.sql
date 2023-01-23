BEGIN TRANSACTION;

ALTER TABLE student.student_enrollment_terms ADD COLUMN incomplete_grade BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX student_enrollment_terms_incomplete_grade ON student.student_enrollment_terms (incomplete_grade);

COMMIT;
