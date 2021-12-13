BEGIN TRANSACTION;

ALTER TABLE student.student_profiles ADD COLUMN profile_summary TEXT;
UPDATE student.student_profiles SET profile_summary = '{}';
ALTER TABLE student.student_profiles ALTER COLUMN profile_summary SET NOT NULL;

COMMIT;