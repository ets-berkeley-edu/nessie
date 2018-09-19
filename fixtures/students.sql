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

CREATE SCHEMA IF NOT EXISTS {redshift_schema_coe};

CREATE TABLE {redshift_schema_coe}.students
(
    sid VARCHAR NOT NULL,
    advisor_ldap_uid VARCHAR NOT NULL,
    gender VARCHAR NOT NULL,
    ethnicity VARCHAR NOT NULL,
    minority VARCHAR NOT NULL,
    did_prep VARCHAR NOT NULL,
    prep_eligible VARCHAR NOT NULL,
    did_tprep VARCHAR NOT NULL,
    tprep_eligible VARCHAR NOT NULL
);

CREATE TABLE {redshift_schema_coe}.student_profiles
(
    sid VARCHAR NOT NULL,
    profile TEXT NOT NULL
);

CREATE SCHEMA IF NOT EXISTS {redshift_schema_student};

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_academic_status
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    level VARCHAR(2),
    gpa DECIMAL(4,3),
    units DECIMAL (4,1)
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_majors
(
    sid VARCHAR NOT NULL,
    major VARCHAR NOT NULL,
    PRIMARY KEY (sid, major)
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_term_gpas
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    gpa DECIMAL(4,3),
    units_taken_for_gpa DECIMAL(4,1)
);

CREATE SCHEMA IF NOT EXISTS {redshift_schema_student}_staging;

CREATE TABLE IF NOT EXISTS {redshift_schema_student}_staging.student_academic_status
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    level VARCHAR(2),
    gpa DECIMAL(4,3),
    units DECIMAL (4,1)
);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}_staging.student_majors
(
    sid VARCHAR NOT NULL,
    major VARCHAR NOT NULL,
    PRIMARY KEY (sid, major)
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

INSERT INTO {redshift_schema_coe}.students
(sid, advisor_ldap_uid, gender, ethnicity, minority, did_prep, prep_eligible, did_tprep, tprep_eligible)
VALUES
('11667051', '90412', 'm', 'H', FALSE, TRUE, FALSE, FALSE, FALSE),
('7890123456', '1133399', 'f', 'B', TRUE, FALSE, TRUE, FALSE, FALSE),
('9000000000', '1133399', 'f', 'B', TRUE, FALSE, TRUE, FALSE, FALSE),
('9100000000', '90412', 'm', 'X', FALSE, FALSE, FALSE, FALSE, TRUE);
