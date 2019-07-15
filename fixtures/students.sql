CREATE SCHEMA IF NOT EXISTS {redshift_schema_asc};

CREATE TABLE IF NOT EXISTS {redshift_schema_asc}.students
(
    sid VARCHAR NOT NULL,
    active BOOLEAN NOT NULL,
    intensive BOOLEAN NOT NULL,
    status_asc VARCHAR,
    group_code VARCHAR,
    group_name VARCHAR,
    team_code VARCHAR,
    team_name VARCHAR
);

CREATE TABLE IF NOT EXISTS {redshift_schema_asc}.student_profiles
(
    sid VARCHAR NOT NULL,
    profile TEXT NOT NULL
);

CREATE SCHEMA IF NOT EXISTS {redshift_schema_calnet};

CREATE TABLE {redshift_schema_calnet}.persons
(
    sid VARCHAR,
    ldap_uid VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    campus_email VARCHAR,
    email VARCHAR,
    affiliations VARCHAR
);

CREATE SCHEMA IF NOT EXISTS {redshift_schema_coe};

CREATE TABLE {redshift_schema_coe}.students
(
    sid VARCHAR NOT NULL,
    advisor_ldap_uid VARCHAR NOT NULL,
    gender VARCHAR NOT NULL,
    ethnicity VARCHAR NOT NULL,
    minority BOOLEAN NOT NULL,
    did_prep BOOLEAN NOT NULL,
    prep_eligible BOOLEAN NOT NULL,
    did_tprep BOOLEAN NOT NULL,
    tprep_eligible BOOLEAN NOT NULL,
    sat1read INT,
    sat1math INT,
    sat2math INT,
    in_met BOOLEAN NOT NULL,
    grad_term VARCHAR,
    grad_year VARCHAR,
    probation BOOLEAN NOT NULL,
    status VARCHAR
);

CREATE TABLE {redshift_schema_coe}.student_profiles
(
    sid VARCHAR NOT NULL,
    profile TEXT NOT NULL
);


CREATE SCHEMA IF NOT EXISTS {redshift_schema_l_s};

CREATE TABLE {redshift_schema_l_s}.students(
    sid VARCHAR
);

CREATE SCHEMA IF NOT EXISTS {redshift_schema_physics};

CREATE TABLE {redshift_schema_physics}.students
(
    sid VARCHAR NOT NULL
);

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

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_academic_status
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    level VARCHAR(2),
    gpa DECIMAL(4,3),
    units DECIMAL (6,3)
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
    major VARCHAR NOT NULL,
    PRIMARY KEY (sid, major)
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
    gpa DECIMAL(4,3),
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

CREATE TABLE IF NOT EXISTS {redshift_schema_student}_staging.student_academic_status
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    level VARCHAR(2),
    gpa DECIMAL(4,3),
    units DECIMAL (6,3)
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
    major VARCHAR NOT NULL,
    PRIMARY KEY (sid, major)
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
    gpa DECIMAL(4,3),
    units_taken_for_gpa DECIMAL(4,1)
);

INSERT INTO {redshift_schema_asc}.students
(sid, intensive, active, status_asc, group_code, group_name, team_code, team_name)
VALUES
('11667051', TRUE, TRUE, 'Compete', 'WFH', 'Women''s Field Hockey', 'FHW', 'Women''s Field Hockey'),
('11667051', TRUE, TRUE, 'Compete', 'WTE', 'Women''s Tennis', 'TNW', 'Women''s Tennis'),
('2345678901', FALSE, TRUE, 'Compete', 'MFB-DB', 'Football, Defensive Backs', 'FBM', 'Football'),
('2345678901', FALSE, TRUE, 'Compete', 'MFB-DL', 'Football, Defensive Line', 'FBM', 'Football'),
('3456789012', TRUE, TRUE, 'Compete', 'MFB-DL', 'Football, Defensive Line', 'FBM', 'Football'),
('5678901234', FALSE, TRUE, 'Compete', 'MFB-DB', 'Football, Defensive Backs', 'FBM', 'Football'),
('5678901234', FALSE, TRUE, 'Compete', 'MFB-DL', 'Football, Defensive Line', 'FBM', 'Football'),
('5678901234', FALSE, TRUE, 'Compete', 'MTE', 'Men''s Tennis', 'TNM', 'Men''s Tennis'),
('7890123456', TRUE, TRUE, 'Compete', 'MBB', 'Men''s Baseball', 'BAM', 'Men''s Baseball'),
('3456789012', TRUE, TRUE, 'Compete', 'MBB-AA', 'Men''s Baseball', 'BAM', 'Men''s Baseball'),
-- 'A mug is a mug in everything.' - Colonel Harrington
('890127492', TRUE, FALSE, 'Trouble', 'MFB-DB', 'Football, Defensive Backs', 'FBM', 'Football'),
('890127492', TRUE, FALSE, 'Trouble', 'MFB-DL', 'Football, Defensive Line', 'FBM', 'Football'),
('890127492', TRUE, FALSE, 'Trouble', 'MTE', 'Men''s Tennis', 'TNM', 'Men''s Tennis'),
('890127492', TRUE, FALSE, 'Trouble', 'WFH', 'Women''s Field Hockey', 'FHW', 'Women''s Field Hockey'),
('890127492', TRUE, FALSE, 'Trouble', 'WTE', 'Women''s Tennis', 'TNW', 'Women''s Tennis');

INSERT INTO {redshift_schema_calnet}.persons
(sid, ldap_uid, first_name, last_name, campus_email, email, affiliations)
VALUES
('11667051', '61889', 'Deborah', 'Davies', 'dd1@berkeley.edu', 'dd1@berkeley.edu', 'STUDENT-TYPE-REGISTERED'),
('1234567890', '12345', 'Osk', 'Bear', '', '', 'FORMER-STUDENT'),
('2345678901', '98765', 'Dave', 'Doolittle', 'dd2@berkeley.edu', 'dd2@berkeley', 'STUDENT-TYPE-REGISTERED'),
('3456789012', '242881', 'Paul', 'Kerschen', 'pk@berkeley.edu', 'pk@berkeley.edu', 'STUDENT-TYPE-REGISTERED'),
('5678901234', '9933311', 'Sandeep', 'Jayaprakash', 'sj@berkeley.edu', 'sj@berkeley.edu', 'STUDENT-TYPE-REGISTERED'),
('7890123456', '1049291', 'Paul', 'Farestveit', 'pf@berkeley.edu', 'pf@berkeley.edu', 'STUDENT-TYPE-REGISTERED'),
('8901234567', '123456', 'John David', 'Crossman', 'jdc@berkeley.edu', 'jdc@berkeley.edu', 'STUDENT-TYPE-REGISTERED'),
('890127492', '211159', 'Siegfried', 'Schlemiel', 'ss@berkeley.edu', 'ss@berkeley.edu', 'STUDENT-TYPE-REGISTERED'),
('9000000000', '300847', 'Wolfgang', 'Pauli-O''Rourke', 'wpo@berkeley.edu', 'wpo@berkeley.edu', 'STUDENT-TYPE-REGISTERED'),
('9100000000', '300848', 'Nora Stanton', 'Barney', 'nsb@berkeley.edu', 'nsb@berkeley.edu', 'STUDENT-TYPE-REGISTERED');

INSERT INTO {redshift_schema_coe}.students
(sid, advisor_ldap_uid, gender, ethnicity, minority, did_prep, prep_eligible, did_tprep, tprep_eligible,
  sat1read, sat1math, sat2math, in_met, grad_term, grad_year, probation, status)
VALUES
('11667051', '90412', 'M', 'H', FALSE, TRUE, FALSE, FALSE, FALSE, NULL, NULL, NULL, FALSE, NULL, NULL, FALSE, 'C'),
('7890123456', '1133399', 'F', 'B', TRUE, FALSE, TRUE, FALSE, FALSE, 510, 520, 620, FALSE, 'sp', '2020', FALSE, 'C'),
('9000000000', '1133399', 'F', 'B', TRUE, FALSE, TRUE, FALSE, FALSE, NULL, NULL, 720, FALSE, NULL, NULL, FALSE, 'Z'),
('9100000000', '90412', 'M', 'X', FALSE, FALSE, FALSE, FALSE, TRUE, 720, 760, 770, TRUE, 'fa', '2018', TRUE, 'N');

INSERT INTO {redshift_schema_l_s}.students (sid)
VALUES ('1234567890');

INSERT INTO {redshift_schema_physics}.students (sid)
VALUES ('2345678901');

INSERT INTO {redshift_schema_student}.sis_api_degree_progress
(sid, feed)
VALUES
('11667051', %(sis_degree_progress_11667051)s);
