BEGIN TRANSACTION;

INSERT INTO student.student_profile_index
SELECT * FROM student.student_academic_status;

DROP TABLE student.student_academic_status;
DROP TABLE student_staging.student_academic_status;

COMMIT TRANSACTION;
