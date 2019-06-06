BEGIN TRANSACTION;

ALTER TABLE student.student_academic_status ADD COLUMN transfer BOOLEAN;
ALTER TABLE student.student_academic_status ADD COLUMN expected_grad_term VARCHAR(4);

CREATE INDEX students_academic_status_transfer_idx ON student.student_academic_status (transfer);
CREATE INDEX students_academic_status_grad_term_idx ON student.student_academic_status (expected_grad_term);

COMMIT;
