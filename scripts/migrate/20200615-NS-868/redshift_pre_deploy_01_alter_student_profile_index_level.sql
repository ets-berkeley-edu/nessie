BEGIN TRANSACTION;

CREATE TABLE student.student_profile_index_new
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

INSERT INTO student.student_profile_index_new (
    SELECT sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term, terms_in_attendance FROM student.student_profile_index
);

DROP TABLE student.student_profile_index;

ALTER TABLE student.student_profile_index_new RENAME TO student_profile_index;

CREATE TABLE student.student_profile_index_hist_enr_new
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

INSERT INTO student.student_profile_index_hist_enr_new (
    SELECT sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term, terms_in_attendance FROM student.student_profile_index
);

DROP TABLE student.student_profile_index_hist_enr;

ALTER TABLE student.student_profile_index_hist_enr_new RENAME TO student_profile_index_hist_enr;

DROP TABLE student_staging.student_profile_index;

CREATE TABLE student_staging.student_profile_index
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

DROP TABLE student_staging.student_profile_index_hist_enr;

CREATE TABLE student_staging.student_profile_index_hist_enr
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

COMMIT;
