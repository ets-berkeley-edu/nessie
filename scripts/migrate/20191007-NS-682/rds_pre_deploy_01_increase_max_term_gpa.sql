BEGIN TRANSACTION;

ALTER TABLE student.student_academic_status ALTER COLUMN gpa TYPE DECIMAL(5,3);
ALTER TABLE student.student_term_gpas ALTER COLUMN gpa TYPE DECIMAL(5,3);

COMMIT;
