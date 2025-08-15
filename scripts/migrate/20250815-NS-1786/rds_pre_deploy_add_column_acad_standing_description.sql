BEGIN TRANSACTION;

ALTER TABLE student.academic_standing ADD COLUMN IF NOT EXISTS acad_standing_description VARCHAR;

COMMIT;
