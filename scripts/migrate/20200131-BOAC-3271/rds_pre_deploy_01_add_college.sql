BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS student.student_majors_new
(
    sid VARCHAR NOT NULL,
    college VARCHAR NOT NULL,
    major VARCHAR NOT NULL
);

INSERT INTO student.student_majors_new (
    SELECT sid, major FROM student.student_majors
);

DROP TABLE student.student_majors;

ALTER TABLE student.student_majors_new RENAME TO student_majors;

CREATE INDEX IF NOT EXISTS student_majors_sid_idx ON student.student_majors (sid);
CREATE INDEX IF NOT EXISTS student_majors_college_idx ON student.student_majors (college);
CREATE INDEX IF NOT EXISTS student_majors_major_idx ON student.student_majors (major);

COMMIT;
