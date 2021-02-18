CREATE SCHEMA IF NOT EXISTS {redshift_schema_student};

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.canvas_api_enrollments
(
    course_id VARCHAR NOT NULL,
    user_id VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    last_activity_at TIMESTAMP,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.sis_api_degree_progress
(
    sid VARCHAR NOT NULL,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.sis_api_profiles
(
    sid VARCHAR NOT NULL,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_profile_index
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    level VARCHAR,
    gpa DECIMAL(5,3),
    units DECIMAL (6,3),
    transfer BOOLEAN,
    expected_grad_term VARCHAR(4),
    terms_in_attendance INT
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_profile_index_hist_enr
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    level VARCHAR,
    gpa DECIMAL(5,3),
    units DECIMAL (6,3)
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_api_demographics
(
    sid VARCHAR NOT NULL,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_demographics
(
    sid VARCHAR NOT NULL,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_enrollment_terms
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    enrollment_term TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_holds
(
    sid VARCHAR NOT NULL,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_last_registrations
(
    sid VARCHAR NOT NULL,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_majors
(
    sid VARCHAR NOT NULL,
    college VARCHAR NOT NULL,
    major VARCHAR NOT NULL,
    PRIMARY KEY (sid, college, major)
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_profiles
(
    sid VARCHAR NOT NULL,
    profile TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_term_gpas
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    gpa DECIMAL(5,3),
    units_taken_for_gpa DECIMAL(4,1)
);

CREATE SCHEMA IF NOT EXISTS {redshift_schema_student}_staging;

CREATE TABLE IF NOT EXISTS {redshift_schema_student}_staging.canvas_api_enrollments
(
    course_id VARCHAR NOT NULL,
    user_id VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    last_activity_at TIMESTAMP,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}_staging.sis_api_degree_progress
(
    sid VARCHAR NOT NULL,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}_staging.sis_api_profiles
(
    sid VARCHAR NOT NULL,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}_staging.student_profile_index
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    level VARCHAR,
    gpa DECIMAL(5,3),
    units DECIMAL (6,3)
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}_staging.student_profile_index_hist_enr
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    level VARCHAR,
    gpa DECIMAL(5,3),
    units DECIMAL (6,3),
    transfer BOOLEAN,
    expected_grad_term VARCHAR(4),
    terms_in_attendance INT
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}_staging.student_api_demographics
(
    sid VARCHAR NOT NULL,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}_staging.student_enrollment_terms
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    enrollment_term TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}_staging.student_holds
(
    sid VARCHAR NOT NULL,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}_staging.student_last_registrations
(
    sid VARCHAR NOT NULL,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}_staging.student_majors
(
    sid VARCHAR NOT NULL,
    college VARCHAR NOT NULL,
    major VARCHAR NOT NULL,
    PRIMARY KEY (sid, college, major)
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}_staging.student_profiles
(
    sid VARCHAR NOT NULL,
    profile TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}_staging.student_term_gpas
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    gpa DECIMAL(5,3),
    units_taken_for_gpa DECIMAL(4,1)
);

INSERT INTO {redshift_schema_student}.sis_api_degree_progress
(sid, feed)
VALUES
('11667051', %(sis_degree_progress_11667051)s);
