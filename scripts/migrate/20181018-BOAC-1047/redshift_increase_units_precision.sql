BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS student.student_academic_status_new
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    level VARCHAR(2),
    gpa DECIMAL(4,3),
    units DECIMAL (6,3)
)
DISTKEY (units)
INTERLEAVED SORTKEY (sid, last_name, level, gpa, units, uid, first_name);

INSERT INTO student.student_academic_status_new (SELECT * FROM student.student_academic_status);

DROP TABLE student.student_academic_status;

ALTER TABLE student.student_academic_status_new RENAME TO student_academic_status;

COMMIT TRANSACTION;
