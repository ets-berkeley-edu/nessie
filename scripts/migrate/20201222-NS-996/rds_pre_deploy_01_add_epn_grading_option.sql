BEGIN TRANSACTION;

ALTER TABLE student.student_enrollment_terms ADD COLUMN epn_grading_option BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX student_enrollment_terms_epn_grading_option ON student.student_enrollment_terms (epn_grading_option);

COMMIT;
