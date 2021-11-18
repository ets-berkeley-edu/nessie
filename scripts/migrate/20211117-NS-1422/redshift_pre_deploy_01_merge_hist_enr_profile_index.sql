ALTER TABLE student.student_profile_index ADD COLUMN hist_enr BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE student_staging.student_profile_index ADD COLUMN hist_enr BOOLEAN NOT NULL DEFAULT FALSE;
DROP TABLE student.student_profile_index_hist_enr;
DROP TABLE student.student_profiles_hist_enr;
DROP TABLE student_staging.student_profile_index_hist_enr;
DROP TABLE student_staging.student_profiles_hist_enr;
