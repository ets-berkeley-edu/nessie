DROP SCHEMA boac_advisee CASCADE;
DROP SCHEMA boac_advising_undergrads CASCADE;
DROP SCHEMA sis_data CASCADE;

ALTER TABLE student.student_profile_index DROP COLUMN hist_enr;
ALTER TABLE student_staging.student_profile_index DROP COLUMN hist_enr;
DROP TABLE student.student_term_gpas;
DROP TABLE student_staging.student_term_gpas;
