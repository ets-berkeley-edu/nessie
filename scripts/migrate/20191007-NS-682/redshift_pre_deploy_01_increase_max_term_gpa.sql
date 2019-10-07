BEGIN TRANSACTION;

-- Redshift does not support ALTER COLUMN.
CREATE TABLE IF NOT EXISTS student.student_term_gpas_new
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    gpa DECIMAL(5,3),
    units_taken_for_gpa DECIMAL(4,1)
)
DISTKEY (sid)
SORTKEY (sid, term_id);

INSERT INTO student.student_term_gpas_new (SELECT * FROM student.student_term_gpas);

DROP TABLE student.student_term_gpas;

ALTER TABLE student.student_term_gpas_new RENAME TO student_term_gpas;

CREATE TABLE IF NOT EXISTS student.student_academic_status_new
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    level VARCHAR(2),
    gpa DECIMAL(5,3),
    units DECIMAL (6,3),
    transfer BOOLEAN,
    expected_grad_term VARCHAR(4)
)
DISTKEY (units)
INTERLEAVED SORTKEY (sid, last_name, level, gpa, units, uid, first_name);

INSERT INTO student.student_academic_status_new (SELECT * FROM student.student_academic_status);

DROP TABLE student.student_academic_status;

ALTER TABLE student.student_academic_status_new RENAME TO student_academic_status;

DROP TABLE student_staging.student_term_gpas;
CREATE TABLE student_staging.student_term_gpas
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR(4) NOT NULL,
    gpa DECIMAL(5,3),
    units_taken_for_gpa DECIMAL(4,1)
)
DISTKEY (sid)
SORTKEY (sid, term_id);

DROP TABLE student_staging.student_academic_status;
CREATE TABLE student_staging.student_academic_status
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    level VARCHAR(2),
    gpa DECIMAL(5,3),
    units DECIMAL (6,3),
    transfer BOOLEAN,
    expected_grad_term VARCHAR(4)
)
DISTKEY (units)
INTERLEAVED SORTKEY (sid, last_name, level, gpa, units, uid, first_name);

COMMIT TRANSACTION;
