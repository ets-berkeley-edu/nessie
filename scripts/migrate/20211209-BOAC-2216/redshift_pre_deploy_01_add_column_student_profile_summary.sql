BEGIN TRANSACTION;

ALTER TABLE student.student_profiles ADD COLUMN profile_summary VARCHAR(max) NOT NULL DEFAULT '{}';
ALTER TABLE student_staging.student_profiles ADD COLUMN profile_summary VARCHAR(max) NOT NULL DEFAULT '{}';

COMMIT;