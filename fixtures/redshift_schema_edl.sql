CREATE SCHEMA IF NOT EXISTS {redshift_schema_edl_external};

CREATE TABLE IF NOT EXISTS {redshift_schema_edl_external}.student_academic_plan_data (
    student_id VARCHAR NOT NULL,
    academic_career_cd VARCHAR,
    academic_program_status_cd VARCHAR,
    academic_plan_type_cd VARCHAR
);

CREATE SCHEMA IF NOT EXISTS {redshift_schema_edl};

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.academic_standing (
    sid VARCHAR NOT NULL,
    term_id VARCHAR NOT NULL,
    acad_standing_action VARCHAR NOT NULL,
    acad_standing_status VARCHAR NOT NULL,
    action_date DATE
);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.advising_note_attachments (
    advising_note_id VARCHAR NOT NULL,
    sid VARCHAR NOT NULL,
    student_note_nr VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    user_file_name VARCHAR NOT NULL,
    sis_file_name VARCHAR NOT NULL,
    edl_load_date DATE,
    is_historical BOOLEAN
);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.advising_note_topics (
    advising_note_id VARCHAR NOT NULL,
    sid VARCHAR NOT NULL,
    student_note_nr VARCHAR NOT NULL,
    note_topic VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.advising_notes (
    id VARCHAR NOT NULL,
    sid VARCHAR NOT NULL,
    student_note_nr VARCHAR NOT NULL,
    advisor_sid VARCHAR NOT NULL,
    appointment_id VARCHAR,
    note_category VARCHAR,
    note_subcategory VARCHAR,
    note_body VARCHAR,
    created_by VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_by VARCHAR,
    updated_at TIMESTAMP WITH TIME ZONE,
    edl_load_date DATE
);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.student_citizenships (
    sid VARCHAR NOT NULL,
    citizenship_country VARCHAR,
    edl_load_date DATE
);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.student_degree_progress
(
    sid VARCHAR NOT NULL,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.student_demographics
(
    sid VARCHAR NOT NULL,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.student_enrollment_terms
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    enrollment_term TEXT NOT NULL
);

CREATE TABLE {redshift_schema_edl}.student_ethnicities (
    sid VARCHAR NOT NULL,
    ethnicity VARCHAR,
    ethnic_group VARCHAR,
    edl_load_date DATE
);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.student_last_registrations
(
    sid VARCHAR NOT NULL,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.student_profile_index
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR,
    level VARCHAR,
    gpa DECIMAL(5,3),
    units DECIMAL (6,3),
    transfer BOOLEAN,
    expected_grad_term VARCHAR(4),
    terms_in_attendance INT,
    edl_load_date DATE
);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.student_profiles
(
    sid VARCHAR NOT NULL,
    feed TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.student_visas
(
    sid VARCHAR NOT NULL,
    visa_status VARCHAR,
    visa_type VARCHAR,
    edl_load_date DATE
);

CREATE TABLE IF NOT EXISTS {redshift_schema_edl}.term_gpas
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    gpa DECIMAL(5,3),
    units_taken_for_gpa DECIMAL(4,1)
);

INSERT INTO {redshift_schema_edl}.student_citizenships
(sid, citizenship_country, edl_load_date)
VALUES
('11667051', 'Singapore',  '2021-02-19'),
('1234567890', 'Iran (Islamic Republic Of)', '2021-02-19'),
('2345678901', 'Taiwan', '2021-02-19'),
('3456789012', 'Korea, Republic of', '2021-02-19'),
('9000000000', 'Saint Kitts and Nevis', '2021-02-19'),
('9000000000', 'Lao People''s Democratic Rep', '2021-02-19');

INSERT INTO {redshift_schema_edl}.student_degree_progress
(sid, feed)
VALUES
('11667051', %(edl_degree_progress_11667051)s);

INSERT INTO {redshift_schema_edl}.student_ethnicities
(sid, ethnicity, ethnic_group, edl_load_date)
VALUES
('11667051', 'Chinese', 'Asian', '2021-02-18'),
('11667051', 'African American/Black', 'Black/African American', '2021-02-18'),
('11667051', 'Asian IPEDS', 'Asian', '2021-02-18'),
('11667051', 'East Indian/Pakistani', 'Asian', '2021-02-18'),
('11667051', 'Other African American/Black', 'Black/African American', '2021-02-18'),
('1234567890', 'Mexican/Mexican American/Chicano', 'Hispanic/Latino', '2021-02-18'),
('1234567890', 'White IPEDS', 'White', '2021-02-18'),
('2345678901', 'European/European descent', 'White', '2021-02-18'),
('2345678901', 'Armenian', 'White', '2021-02-18'),
('3456789012', 'Other Asian (not including Middle Eastern)', 'Asian', '2021-02-18'),
('3456789012', 'American Indian/Alaska Native', 'American Indian/Alaska Native', '2021-02-18'),
('3456789012', 'Filipino/Filipino American', 'Asian', '2021-02-18'),
('8901234567', 'Not Hispanic/Latino', 'Not Specified', '2021-02-18'),
('9000000000', 'Other Asian (not including Middle Eastern)', 'Asian', '2021-02-18'),
('9000000000', 'Samoan', 'Native Hawaiian/Oth Pac Island', '2021-02-18'),
('9000000000', 'African American/Black', 'Black/African American', '2021-02-18');

INSERT INTO {redshift_schema_edl}.student_last_registrations
(sid, feed)
VALUES
('1234567890', %(edl_last_registration_1234567890)s);

INSERT INTO {redshift_schema_edl}.student_profile_index
(sid, uid, first_name, last_name, gender, level, gpa, units, transfer, expected_grad_term, terms_in_attendance, edl_load_date)
VALUES
('11667051', '61889', 'Oski', 'Bear', 'F', 10, 0, 3, FALSE, '2218', 1, '2021-02-17'),
('1234567890', '12345', 'Oski', 'Bear', 'M', 20, 0, 3, FALSE, '2012', NULL, '2021-02-17'),
('2345678901', '98765', 'Dave', 'Doolittle', 'F', 30, 0, 3, FALSE, '2132', 2, '2021-02-17'),
('3456789012', '242881', 'Paul', 'Kerschen', 'U', 40, 0, 3, FALSE, '2215', NULL, '2021-02-17'),
('5000000000', '505050', 'Moon Unit', 'Zappa', 'F', 10, 0, 3, FALSE, '2238', 4, '2021-02-17'),
('5678901234', '9933311', 'Sandeep', 'Jayaprakash', 'U', 20, 0, 3, FALSE, '2218', NULL, '2021-02-17'),
('7890123456', '1049291', 'Paul', 'Farestveit', 'U', 30, 0, 3, FALSE, '2152', 5, '2021-02-17'),
('8901234567', '123456', 'John David', 'Crossman', 'U', 40, 0, 3, FALSE, '2218', 3, '2021-02-17'),
('890127492', '211159', 'Siegfried', 'Schlemiel', 'X', 10, 0, 3, FALSE, '2002', NULL, '2021-02-17'),
('9000000000', '300847', 'Wolfgang', 'Pauli-O''Rourke', 'X', 20, 0, 3, FALSE, '2218', NULL, '2021-02-17'),
('9100000000', '300848', 'Nora Stanton', 'Barney', 'M', 30, 0, 3, FALSE, '2218', NULL, '2021-02-17');

INSERT INTO {redshift_schema_edl}.student_profiles
(sid, feed)
VALUES
('11667051', %(edl_profile_11667051)s),
('1234567890', %(edl_profile_1234567890)s),
('2345678901', %(edl_profile_2345678901)s),
('5000000000', %(edl_profile_5000000000)s);

INSERT INTO {redshift_schema_edl}.student_visas
(sid, visa_status, visa_type, edl_load_date)
VALUES
('11667051', 'A', 'PR', '2021-02-18'),
('1234567890', 'A', 'F1', '2021-02-18'),
('2345678901', NULL, NULL, '2021-02-18'),
('3456789012', 'G', 'J1', '2021-02-18'),
('5000000000', NULL, NULL, '2021-02-18');
