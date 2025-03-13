BEGIN TRANSACTION;

ALTER TABLE student.student_names ADD COLUMN email_address VARCHAR;

CREATE INDEX IF NOT EXISTS student_names_email_address_idx ON student.student_names (email_address);

COMMIT;
