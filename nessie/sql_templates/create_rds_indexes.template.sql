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
    team_name VARCHAR,
    PRIMARY KEY (sid, group_code)
);

CREATE INDEX IF NOT EXISTS students_asc_sid_idx ON {redshift_schema_asc}.students (sid);
CREATE INDEX IF NOT EXISTS students_asc_active_idx ON {redshift_schema_asc}.students (active);
CREATE INDEX IF NOT EXISTS students_asc_intensive_idx ON {redshift_schema_asc}.students (intensive);
CREATE INDEX IF NOT EXISTS students_asc_group_code_idx ON {redshift_schema_asc}.students (group_code);

CREATE SCHEMA IF NOT EXISTS {redshift_schema_coe};

CREATE TABLE IF NOT EXISTS {redshift_schema_coe}.students
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
    PRIMARY KEY (sid, advisor_ldap_uid)
);

CREATE INDEX IF NOT EXISTS students_coe_sid_idx ON {redshift_schema_coe}.students (sid);
CREATE INDEX IF NOT EXISTS students_coe_advisor_ldap_uid_idx ON {redshift_schema_coe}.students (advisor_ldap_uid);

CREATE SCHEMA IF NOT EXISTS {redshift_schema_student};

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_academic_status
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    level VARCHAR(2),
    gpa DECIMAL(4,3),
    units DECIMAL (6,3),
    PRIMARY KEY (sid)
);

CREATE INDEX IF NOT EXISTS students_academic_status_first_name_idx ON {redshift_schema_student}.student_academic_status (first_name);
CREATE INDEX IF NOT EXISTS students_academic_status_last_name_idx ON {redshift_schema_student}.student_academic_status (last_name);
CREATE INDEX IF NOT EXISTS students_academic_status_level_idx ON {redshift_schema_student}.student_academic_status (level);
CREATE INDEX IF NOT EXISTS students_academic_status_gpa_idx ON {redshift_schema_student}.student_academic_status (gpa);
CREATE INDEX IF NOT EXISTS students_academic_status_units_idx ON {redshift_schema_student}.student_academic_status (units);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_majors
(
    sid VARCHAR NOT NULL,
    major VARCHAR NOT NULL,
    PRIMARY KEY (sid, major)
);

CREATE INDEX IF NOT EXISTS students_major_sid_idx ON {redshift_schema_student}.student_majors (sid);
CREATE INDEX IF NOT EXISTS students_major_major_idx ON {redshift_schema_student}.student_majors (major);

CREATE TABLE IF NOT EXISTS {redshift_schema_student}.student_term_gpas
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    gpa DECIMAL(4,3),
    units_taken_for_gpa DECIMAL(4,1)
);

CREATE INDEX IF NOT EXISTS students_term_gpa_sid_idx ON {redshift_schema_student}.student_term_gpas (sid);
CREATE INDEX IF NOT EXISTS students_term_gpa_term_idx ON {redshift_schema_student}.student_term_gpas (term_id);
CREATE INDEX IF NOT EXISTS students_term_gpa_gpa_idx ON {redshift_schema_student}.student_term_gpas (gpa);
CREATE INDEX IF NOT EXISTS students_term_gpa_units_idx ON {redshift_schema_student}.student_term_gpas (units_taken_for_gpa);

CREATE SCHEMA IF NOT EXISTS {redshift_schema_sis_internal};

CREATE TABLE IF NOT EXISTS {redshift_schema_sis_internal}.sis_terms
(
    term_id VARCHAR(4) NOT NULL,
    term_name VARCHAR NOT NULL,
    academic_career VARCHAR NOT NULL,
    term_begins DATE NOT NULL,
    term_ends DATE NOT NULL,
    session_id VARCHAR NOT NULL,
    session_name VARCHAR NOT NULL,
    session_begins DATE NOT NULL,
    session_ends DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS sis_terms_term_id_academic_career_idx ON {redshift_schema_sis_internal}.sis_terms (term_id, academic_career);
