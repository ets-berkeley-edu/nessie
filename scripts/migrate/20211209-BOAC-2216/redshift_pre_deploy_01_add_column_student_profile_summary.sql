BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS student.student_profiles_new
(
    sid VARCHAR NOT NULL,
    profile VARCHAR(max) NOT NULL,
    profile_summary VARCHAR(max) NOT NULL
)
DISTKEY (sid)
SORTKEY (sid);

INSERT INTO student.student_profiles_new (SELECT sid, profile, '{}' FROM student.student_profiles);
DROP TABLE student.student_profiles;
ALTER TABLE student.student_profiles_new RENAME TO student_profiles;

CREATE TABLE IF NOT EXISTS student_staging.student_profiles_new
(
    sid VARCHAR NOT NULL,
    profile VARCHAR(max) NOT NULL,
    profile_summary VARCHAR(max) NOT NULL
)
DISTKEY (sid)
SORTKEY (sid);

INSERT INTO student_staging.student_profiles_new (SELECT sid, profile, '{}' FROM student_staging.student_profiles);
DROP TABLE student_staging.student_profiles;
ALTER TABLE student_staging.student_profiles_new RENAME TO student_profiles;

COMMIT;