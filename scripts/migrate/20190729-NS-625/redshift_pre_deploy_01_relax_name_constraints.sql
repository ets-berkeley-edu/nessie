BEGIN TRANSACTION;

-- Redshift does not support ALTER COLUMN, and so we cannot directly DROP NOT NULL for columns in the
-- student.student_academic_status table.

CREATE TABLE IF NOT EXISTS student.student_academic_status_new
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    level VARCHAR(2),
    gpa DECIMAL(4,3),
    units DECIMAL (6,3),
    transfer BOOLEAN,
    expected_grad_term VARCHAR(4)
)
DISTKEY (units)
INTERLEAVED SORTKEY (sid, last_name, level, gpa, units, uid, first_name);

INSERT INTO student.student_academic_status_new (SELECT * FROM student.student_academic_status);

DROP TABLE student.student_academic_status;

ALTER TABLE student.student_academic_status_new RENAME TO student_academic_status;

DROP TABLE student_staging.student_academic_status;

CREATE TABLE student_staging.student_academic_status
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    level VARCHAR(2),
    gpa DECIMAL(4,3),
    units DECIMAL (6,3),
    transfer BOOLEAN,
    expected_grad_term VARCHAR(4)
)
DISTKEY (units)
INTERLEAVED SORTKEY (sid, last_name, level, gpa, units, uid, first_name);

COMMIT TRANSACTION;
