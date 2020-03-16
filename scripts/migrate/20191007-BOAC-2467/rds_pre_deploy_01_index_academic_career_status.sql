BEGIN TRANSACTION;

ALTER TABLE student.student_academic_status ADD COLUMN academic_career_status VARCHAR;

CREATE INDEX students_academic_status_academic_career_status_idx ON student.student_academic_status (academic_career_status);

UPDATE student.student_academic_status sas
  SET academic_career_status = lower(p.profile::json->'sisProfile'->>'academicCareerStatus')
  FROM student.student_profiles p
  WHERE sas.sid = p.sid;

COMMIT;
