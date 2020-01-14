BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS student.student_academic_status_new
(
    sid VARCHAR NOT NULL,
    uid VARCHAR NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    level VARCHAR(2),
    gpa DECIMAL(4,3),
    units DECIMAL (6,3),
    transfer BOOLEAN,
    expected_grad_term VARCHAR(4),
    terms_in_attendance INT,
    email_address VARCHAR,
    entering_term VARCHAR(4)
);

INSERT INTO student.student_academic_status_new (
    SELECT sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term, NULL, email_address, entering_term FROM student.student_academic_status
);

DROP TABLE student.student_academic_status;

ALTER TABLE student.student_academic_status_new RENAME TO student_academic_status;

CREATE INDEX students_academic_status_first_name_idx ON student.student_academic_status (first_name);
CREATE INDEX students_academic_status_last_name_idx ON student.student_academic_status (last_name);
CREATE INDEX students_academic_status_level_idx ON student.student_academic_status (level);
CREATE INDEX students_academic_status_gpa_idx ON student.student_academic_status (gpa);
CREATE INDEX students_academic_status_units_idx ON student.student_academic_status (units);
CREATE INDEX students_academic_status_transfer_idx ON student.student_academic_status (transfer);
CREATE INDEX students_academic_status_email_address_idx ON student.student_academic_status (email_address);
CREATE INDEX students_academic_status_entering_term_idx ON student.student_academic_status (entering_term);
CREATE INDEX students_academic_status_grad_term_idx ON student.student_academic_status (expected_grad_term);
CREATE INDEX students_academic_status_terms_in_attendance_idx ON student.student_academic_status (terms_in_attendance);

COMMIT;
