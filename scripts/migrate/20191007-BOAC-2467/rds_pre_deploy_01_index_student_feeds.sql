BEGIN TRANSACTION;

ALTER TABLE student.student_academic_status ADD COLUMN email_address VARCHAR;
ALTER TABLE student.student_academic_status ADD COLUMN entering_term VARCHAR(4);

CREATE INDEX students_academic_status_email_address_idx ON student.student_academic_status (email_address);
CREATE INDEX students_academic_status_entering_term_idx ON student.student_academic_status (entering_term);

UPDATE student.student_academic_status sas
  SET email_address = p.profile::json->'sisProfile'->>'emailAddress'
  FROM student.student_profiles p
  WHERE sas.sid = p.sid;

UPDATE student.student_academic_status sas
  SET entering_term =
  substr(split_part(p.profile::json->'sisProfile'->>'matriculation', ' ', 2), 1, 1)
  ||
  substr(split_part(p.profile::json->'sisProfile'->>'matriculation', ' ', 2), 3, 2)
  ||
  CASE split_part(p.profile::json->'sisProfile'->>'matriculation', ' ', 1)
  WHEN 'Winter' THEN 0 WHEN 'Spring' THEN 2 WHEN 'Summer' THEN 5 WHEN 'Fall' THEN 8 END
  FROM student.student_profiles p
  WHERE p.sid = sas.sid
  AND p.profile::json->'sisProfile'->>'matriculation' IS NOT NULL;

ALTER TABLE student.student_enrollment_terms ADD COLUMN enrolled_units DECIMAL (3,1) NOT NULL DEFAULT 0;

CREATE INDEX students_enrollment_terms_enrolled_units ON student.student_enrollment_terms (enrolled_units);

UPDATE student.student_enrollment_terms
  SET enrolled_units = (enrollment_term::json->>'enrolledUnits')::numeric
  WHERE enrollment_term::json->>'enrolledUnits' IS NOT NULL;

COMMIT;
