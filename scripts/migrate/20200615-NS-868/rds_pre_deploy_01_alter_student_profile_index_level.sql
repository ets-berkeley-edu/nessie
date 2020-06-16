BEGIN TRANSACTION;

ALTER TABLE student.student_academic_status ALTER COLUMN level TYPE VARCHAR;
ALTER TABLE student.student_profile_index ALTER COLUMN level TYPE VARCHAR;

COMMIT;
