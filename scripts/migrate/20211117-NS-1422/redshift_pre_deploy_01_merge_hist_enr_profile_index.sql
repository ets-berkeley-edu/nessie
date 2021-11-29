ALTER TABLE student.student_profile_index ADD COLUMN hist_enr BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE student_staging.student_profile_index ADD COLUMN hist_enr BOOLEAN NOT NULL DEFAULT FALSE;

DROP TABLE IF EXISTS student.hist_enr_last_registrations;
DROP TABLE IF EXISTS student.hist_enr_term_gpas;
DROP TABLE IF EXISTS student.sis_api_degree_progress;
DROP TABLE IF EXISTS student.sis_api_drops_and_midterms;
DROP TABLE IF EXISTS student.sis_api_profiles;
DROP TABLE IF EXISTS student.sis_api_profiles_v1;
DROP TABLE IF EXISTS student.sis_api_profiles_hist_enr;
DROP TABLE IF EXISTS student.sis_profiles;
DROP TABLE IF EXISTS student.sis_profiles_hist_enr;
DROP TABLE IF EXISTS student.student_api_demographics;
DROP TABLE IF EXISTS student.student_enrollment_terms_hist_enr;
DROP TABLE IF EXISTS student.student_last_registrations;
DROP TABLE IF EXISTS student.student_names_hist_enr;
DROP TABLE IF EXISTS student.student_profile_index_hist_enr;
DROP TABLE IF EXISTS student.student_profiles_hist_enr;

DROP TABLE IF EXISTS student_staging.hist_enr_last_registrations;
DROP TABLE IF EXISTS student_staging.hist_enr_term_gpas;
DROP TABLE IF EXISTS student_staging.sis_api_degree_progress;
DROP TABLE IF EXISTS student_staging.sis_api_drops_and_midterms;
DROP TABLE IF EXISTS student_staging.sis_api_profiles;
DROP TABLE IF EXISTS student_staging.sis_api_profiles_v1;
DROP TABLE IF EXISTS student_staging.sis_api_profiles_hist_enr;
DROP TABLE IF EXISTS student_staging.sis_profiles;
DROP TABLE IF EXISTS student_staging.sis_profiles_hist_enr;
DROP TABLE IF EXISTS student_staging.student_api_demographics;
DROP TABLE IF EXISTS student_staging.student_enrollment_terms_hist_enr;
DROP TABLE IF EXISTS student_staging.student_last_registrations;
DROP TABLE IF EXISTS student_staging.student_names_hist_enr;
DROP TABLE IF EXISTS student_staging.student_profile_index_hist_enr;
DROP TABLE IF EXISTS student_staging.student_profiles_hist_enr;

DROP SCHEMA edl_sis_data_staging CASCADE;
