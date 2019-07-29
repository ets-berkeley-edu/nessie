BEGIN TRANSACTION;

ALTER TABLE student.student_academic_status ALTER COLUMN first_name DROP NOT NULL;
ALTER TABLE student.student_academic_status ALTER COLUMN last_name DROP NOT NULL;

COMMIT;
