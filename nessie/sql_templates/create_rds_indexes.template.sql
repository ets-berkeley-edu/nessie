CREATE SCHEMA IF NOT EXISTS {rds_schema_asc};
GRANT USAGE ON SCHEMA {rds_schema_asc} TO {rds_app_boa_user};
GRANT SELECT ON ALL TABLES IN SCHEMA {rds_schema_asc} TO {rds_app_boa_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_asc} GRANT SELECT ON TABLES TO {rds_app_boa_user};

CREATE TABLE IF NOT EXISTS {rds_schema_asc}.students
(
    sid VARCHAR NOT NULL,
    active BOOLEAN NOT NULL,
    intensive BOOLEAN NOT NULL,
    status_asc VARCHAR,
    group_code VARCHAR,
    group_name VARCHAR,
    team_code VARCHAR,
    team_name VARCHAR,
    PRIMARY KEY (sid, group_code)
);

CREATE INDEX IF NOT EXISTS students_asc_sid_idx ON {rds_schema_asc}.students (sid);
CREATE INDEX IF NOT EXISTS students_asc_active_idx ON {rds_schema_asc}.students (active);
CREATE INDEX IF NOT EXISTS students_asc_intensive_idx ON {rds_schema_asc}.students (intensive);
CREATE INDEX IF NOT EXISTS students_asc_group_code_idx ON {rds_schema_asc}.students (group_code);

CREATE TABLE IF NOT EXISTS {rds_schema_asc}.student_profiles
(
    sid VARCHAR NOT NULL PRIMARY KEY,
    profile TEXT NOT NULL
);

CREATE SCHEMA IF NOT EXISTS {rds_schema_coe};
GRANT USAGE ON SCHEMA {rds_schema_coe} TO {rds_app_boa_user};
GRANT SELECT ON ALL TABLES IN SCHEMA {rds_schema_coe} TO {rds_app_boa_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_coe} GRANT SELECT ON TABLES TO {rds_app_boa_user};

CREATE TABLE IF NOT EXISTS {rds_schema_coe}.students
(
    sid VARCHAR NOT NULL,
    advisor_ldap_uid VARCHAR,
    gender VARCHAR,
    ethnicity VARCHAR,
    minority BOOLEAN,
    did_prep BOOLEAN,
    prep_eligible BOOLEAN,
    did_tprep BOOLEAN,
    tprep_eligible BOOLEAN,
    sat1read INT,
    sat1math INT,
    sat2math INT,
    in_met BOOLEAN,
    grad_term VARCHAR,
    grad_year VARCHAR,
    probation BOOLEAN,
    status VARCHAR,
    PRIMARY KEY (sid, advisor_ldap_uid)
);

CREATE INDEX IF NOT EXISTS students_coe_sid_idx ON {rds_schema_coe}.students (sid);
CREATE INDEX IF NOT EXISTS students_coe_advisor_ldap_uid_idx ON {rds_schema_coe}.students (advisor_ldap_uid);
CREATE INDEX IF NOT EXISTS students_coe_probation_idx ON {rds_schema_coe}.students (probation);
CREATE INDEX IF NOT EXISTS students_coe_status_idx ON {rds_schema_coe}.students (status);

CREATE TABLE IF NOT EXISTS {rds_schema_coe}.student_profiles
(
    sid VARCHAR NOT NULL PRIMARY KEY,
    profile TEXT NOT NULL
);

CREATE SCHEMA IF NOT EXISTS {rds_schema_undergrads};
GRANT USAGE ON SCHEMA {rds_schema_undergrads} TO {rds_app_boa_user};
GRANT SELECT ON ALL TABLES IN SCHEMA {rds_schema_undergrads} TO {rds_app_boa_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_undergrads} GRANT SELECT ON TABLES TO {rds_app_boa_user};

CREATE TABLE IF NOT EXISTS {rds_schema_undergrads}.students
(
    sid VARCHAR NOT NULL,
    acadprog_code VARCHAR,
    acadprog_descr VARCHAR,
    acadplan_code VARCHAR,
    acadplan_descr VARCHAR,
    acadplan_type_code VARCHAR,
    acadplan_ownedby_code VARCHAR,
    PRIMARY KEY (sid, acadprog_code, acadplan_code)
);

CREATE INDEX IF NOT EXISTS students_undergrads_sid_idx ON {rds_schema_undergrads}.students (sid);

CREATE SCHEMA IF NOT EXISTS {rds_schema_student};
GRANT USAGE ON SCHEMA {rds_schema_student} TO {rds_app_boa_user};
GRANT SELECT ON ALL TABLES IN SCHEMA {rds_schema_student} TO {rds_app_boa_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_student} GRANT SELECT ON TABLES TO {rds_app_boa_user};

CREATE TABLE IF NOT EXISTS {rds_schema_student}.student_academic_status
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    level VARCHAR(2),
    gpa DECIMAL(5,3),
    units DECIMAL (6,3),
    transfer BOOLEAN,
    expected_grad_term VARCHAR(4),
    terms_in_attendance INT,
    email_address VARCHAR,
    entering_term VARCHAR(4),
    PRIMARY KEY (sid)
);

CREATE INDEX IF NOT EXISTS students_academic_status_first_name_idx ON {rds_schema_student}.student_academic_status (first_name);
CREATE INDEX IF NOT EXISTS students_academic_status_last_name_idx ON {rds_schema_student}.student_academic_status (last_name);
CREATE INDEX IF NOT EXISTS students_academic_status_level_idx ON {rds_schema_student}.student_academic_status (level);
CREATE INDEX IF NOT EXISTS students_academic_status_gpa_idx ON {rds_schema_student}.student_academic_status (gpa);
CREATE INDEX IF NOT EXISTS students_academic_status_units_idx ON {rds_schema_student}.student_academic_status (units);
CREATE INDEX IF NOT EXISTS students_academic_status_transfer_idx ON {rds_schema_student}.student_academic_status (transfer);
CREATE INDEX IF NOT EXISTS students_academic_status_email_address_idx ON {rds_schema_student}.student_academic_status (email_address);
CREATE INDEX IF NOT EXISTS students_academic_status_entering_term_idx ON {rds_schema_student}.student_academic_status (entering_term);
CREATE INDEX IF NOT EXISTS students_academic_status_grad_term_idx ON {rds_schema_student}.student_academic_status (expected_grad_term);
CREATE INDEX IF NOT EXISTS students_academic_status_terms_in_attendance_idx ON {rds_schema_student}.student_academic_status (terms_in_attendance);

CREATE TABLE IF NOT EXISTS {rds_schema_student}.student_profile_index
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    level VARCHAR(3),
    gpa DECIMAL(5,3),
    units DECIMAL (6,3),
    transfer BOOLEAN,
    expected_grad_term VARCHAR(4),
    terms_in_attendance INT,
    email_address VARCHAR,
    entering_term VARCHAR(4),
    academic_career_status VARCHAR,
    hist_enr BOOLEAN NOT NULL,
    PRIMARY KEY (sid)
);

CREATE INDEX IF NOT EXISTS student_profile_index_first_name_idx ON {rds_schema_student}.student_profile_index (first_name);
CREATE INDEX IF NOT EXISTS student_profile_index_last_name_idx ON {rds_schema_student}.student_profile_index (last_name);
CREATE INDEX IF NOT EXISTS student_profile_index_level_idx ON {rds_schema_student}.student_profile_index (level);
CREATE INDEX IF NOT EXISTS student_profile_index_gpa_idx ON {rds_schema_student}.student_profile_index (gpa);
CREATE INDEX IF NOT EXISTS student_profile_index_units_idx ON {rds_schema_student}.student_profile_index (units);
CREATE INDEX IF NOT EXISTS student_profile_index_transfer_idx ON {rds_schema_student}.student_profile_index (transfer);
CREATE INDEX IF NOT EXISTS student_profile_index_email_address_idx ON {rds_schema_student}.student_profile_index (email_address);
CREATE INDEX IF NOT EXISTS student_profile_index_entering_term_idx ON {rds_schema_student}.student_profile_index (entering_term);
CREATE INDEX IF NOT EXISTS student_profile_index_grad_term_idx ON {rds_schema_student}.student_profile_index (expected_grad_term);
CREATE INDEX IF NOT EXISTS student_profile_index_terms_in_attendance_idx ON {rds_schema_student}.student_profile_index (terms_in_attendance);
CREATE INDEX IF NOT EXISTS student_profile_index_career_idx ON {rds_schema_student}.student_profile_index (academic_career_status);
CREATE INDEX IF NOT EXISTS student_profile_index_hist_enr_idx ON {rds_schema_student}.student_profile_index (hist_enr);

CREATE TABLE IF NOT EXISTS {rds_schema_student}.student_degrees
(
    sid VARCHAR NOT NULL,
    plan VARCHAR NOT NULL,
    date_awarded VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    hist_enr BOOLEAN NOT NULL,
    PRIMARY KEY (sid, plan)
);

CREATE INDEX IF NOT EXISTS student_degree_plan_idx ON {rds_schema_student}.student_degrees (plan);
CREATE INDEX IF NOT EXISTS student_degree_term_id_idx ON {rds_schema_student}.student_degrees (term_id);
CREATE INDEX IF NOT EXISTS student_degree_hist_enr_idx ON {rds_schema_student}.student_degrees (hist_enr);

CREATE TABLE IF NOT EXISTS {rds_schema_student}.student_names
(
    sid VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    PRIMARY KEY (sid, name)
);

CREATE INDEX IF NOT EXISTS student_names_name_idx ON {rds_schema_student}.student_names (name);

CREATE TABLE IF NOT EXISTS {rds_schema_student}.student_enrollment_terms
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    enrollment_term TEXT NOT NULL,
    midpoint_deficient_grade BOOLEAN NOT NULL,
    enrolled_units DECIMAL (3,1) NOT NULL DEFAULT 0,
    term_gpa DECIMAL(5,3),
    PRIMARY KEY (sid, term_id)
);

CREATE INDEX IF NOT EXISTS students_enrollment_terms_midpoint_deficient_grade
ON {rds_schema_student}.student_enrollment_terms (midpoint_deficient_grade);

CREATE INDEX IF NOT EXISTS students_enrollment_terms_enrolled_units
ON {rds_schema_student}.student_enrollment_terms (enrolled_units);

CREATE INDEX IF NOT EXISTS students_enrollment_terms_term_gpa
ON {rds_schema_student}.student_enrollment_terms (term_gpa);

CREATE TABLE IF NOT EXISTS {rds_schema_student}.student_majors
(
    sid VARCHAR NOT NULL,
    college VARCHAR NOT NULL,
    major VARCHAR NOT NULL,
    PRIMARY KEY (sid, college, major)
);

CREATE INDEX IF NOT EXISTS student_majors_sid_idx ON {rds_schema_student}.student_majors (sid);
CREATE INDEX IF NOT EXISTS student_majors_college_idx ON {rds_schema_student}.student_majors (college);
CREATE INDEX IF NOT EXISTS student_majors_major_idx ON {rds_schema_student}.student_majors (major);

CREATE TABLE IF NOT EXISTS {rds_schema_student}.demographics
(
    sid VARCHAR NOT NULL,
    gender VARCHAR,
    minority BOOLEAN,
    PRIMARY KEY (sid)
);

CREATE INDEX IF NOT EXISTS students_demographics_sid_idx ON {rds_schema_student}.demographics (sid);

CREATE TABLE IF NOT EXISTS {rds_schema_student}.ethnicities
(
    sid VARCHAR NOT NULL,
    ethnicity VARCHAR,
    PRIMARY KEY (sid, ethnicity)
);

CREATE INDEX IF NOT EXISTS students_ethnicities_sid_idx ON {rds_schema_student}.ethnicities (sid);
CREATE INDEX IF NOT EXISTS students_ethnicities_sid_idx ON {rds_schema_student}.ethnicities (ethnicity);

CREATE TABLE IF NOT EXISTS {rds_schema_student}.intended_majors
(
    sid VARCHAR NOT NULL,
    major VARCHAR NOT NULL,
    PRIMARY KEY (sid, major)
);

CREATE INDEX IF NOT EXISTS student_intended_majors_sid_idx ON {rds_schema_student}.intended_majors (sid);
CREATE INDEX IF NOT EXISTS student_intended_majors_major_idx ON {rds_schema_student}.intended_majors (major);

CREATE TABLE IF NOT EXISTS {rds_schema_student}.minors
(
    sid VARCHAR NOT NULL,
    minor VARCHAR NOT NULL,
    PRIMARY KEY (sid, minor)
);

CREATE INDEX IF NOT EXISTS student_minors_sid_idx ON {rds_schema_student}.minors (sid);
CREATE INDEX IF NOT EXISTS student_minors_minor_idx ON {rds_schema_student}.minors (minor);

CREATE TABLE IF NOT EXISTS {rds_schema_student}.visas
(
    sid VARCHAR NOT NULL,
    visa_status VARCHAR,
    visa_type VARCHAR,
    PRIMARY KEY (sid)
);

CREATE INDEX IF NOT EXISTS students_visa_status_type_idx ON {rds_schema_student}.visas (visa_status, visa_type);

CREATE TABLE IF NOT EXISTS {rds_schema_student}.student_holds
(
    sid VARCHAR NOT NULL,
    feed TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS student_holds_sid_idx ON {rds_schema_student}.student_holds (sid);

CREATE TABLE IF NOT EXISTS {rds_schema_student}.student_profiles
(
    sid VARCHAR NOT NULL PRIMARY KEY,
    profile TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {rds_schema_student}.student_term_gpas
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    gpa DECIMAL(5,3),
    units_taken_for_gpa DECIMAL(4,1)
);

CREATE INDEX IF NOT EXISTS students_term_gpa_sid_idx ON {rds_schema_student}.student_term_gpas (sid);
CREATE INDEX IF NOT EXISTS students_term_gpa_term_idx ON {rds_schema_student}.student_term_gpas (term_id);
CREATE INDEX IF NOT EXISTS students_term_gpa_gpa_idx ON {rds_schema_student}.student_term_gpas (gpa);
CREATE INDEX IF NOT EXISTS students_term_gpa_units_idx ON {rds_schema_student}.student_term_gpas (units_taken_for_gpa);

CREATE SCHEMA IF NOT EXISTS {rds_schema_sis_internal};
GRANT USAGE ON SCHEMA {rds_schema_sis_internal} TO {rds_app_boa_user};
GRANT SELECT ON ALL TABLES IN SCHEMA {rds_schema_sis_internal} TO {rds_app_boa_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_sis_internal} GRANT SELECT ON TABLES TO {rds_app_boa_user};

CREATE TABLE IF NOT EXISTS {rds_schema_sis_internal}.enrolled_primary_sections
(
    term_id VARCHAR(4) NOT NULL,
    sis_section_id VARCHAR(5) NOT NULL,
    sis_course_name VARCHAR NOT NULL,
    sis_course_name_compressed VARCHAR NOT NULL,
    sis_subject_area_compressed VARCHAR NOT NULL,
    sis_catalog_id VARCHAR NOT NULL,
    sis_course_title VARCHAR,
    sis_instruction_format VARCHAR NOT NULL,
    sis_section_num VARCHAR NOT NULL,
    instructors VARCHAR
);

CREATE INDEX IF NOT EXISTS enrolled_primary_sections_term_id_sis_course_name_compressed_idx
ON {rds_schema_sis_internal}.enrolled_primary_sections (term_id, sis_course_name_compressed);
CREATE INDEX IF NOT EXISTS enrolled_primary_sections_sis_subject_area_compressed_idx
ON {rds_schema_sis_internal}.enrolled_primary_sections (sis_subject_area_compressed);
CREATE INDEX IF NOT EXISTS enrolled_primary_sections_sis_catalog_id_idx
ON {rds_schema_sis_internal}.enrolled_primary_sections (sis_catalog_id);

CREATE SCHEMA IF NOT EXISTS {rds_schema_sis_terms};
GRANT USAGE ON SCHEMA {rds_schema_sis_terms} TO {rds_app_boa_user};
GRANT SELECT ON ALL TABLES IN SCHEMA {rds_schema_sis_terms} TO {rds_app_boa_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_sis_terms} GRANT SELECT ON TABLES TO {rds_app_boa_user};

CREATE TABLE IF NOT EXISTS {rds_schema_sis_terms}.term_definitions
(
    term_id VARCHAR(4) NOT NULL,
    term_name VARCHAR NOT NULL,
    term_begins DATE NOT NULL,
    term_ends DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS term_definitions_term_id_idx
ON {rds_schema_sis_terms}.term_definitions (term_id);

CREATE TABLE IF NOT EXISTS {rds_schema_sis_terms}.current_term_index
(
    current_term_name VARCHAR NOT NULL,
    future_term_name VARCHAR NOT NULL
);
