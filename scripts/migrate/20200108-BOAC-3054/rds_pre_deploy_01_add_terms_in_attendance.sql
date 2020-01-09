BEGIN TRANSACTION;

ALTER TABLE student.student_academic_status ADD COLUMN terms_in_attendance INT;

CREATE INDEX students_academic_status_terms_in_attendance_idx ON student.student_academic_status (terms_in_attendance);

COMMIT;
