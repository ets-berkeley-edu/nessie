BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS student.student_majors_new
(
  sid VARCHAR NOT NULL,
  college VARCHAR NOT NULL,
  major VARCHAR NOT NULL,
  division VARCHAR
)
DISTKEY (sid)
SORTKEY (college, major, division);

INSERT INTO student.student_majors_new (SELECT sid, college, major, NULL FROM student.student_majors);

DROP TABLE student.student_majors;

ALTER TABLE student.student_majors_new RENAME TO student_majors;

DROP TABLE student_staging.student_majors;

CREATE TABLE student_staging.student_majors 
(
  sid VARCHAR NOT NULL,
  college VARCHAR NOT NULL,
  major VARCHAR NOT NULL,
  division VARCHAR
)
DISTKEY (sid)
SORTKEY (college, major, division);

COMMIT TRANSACTION;