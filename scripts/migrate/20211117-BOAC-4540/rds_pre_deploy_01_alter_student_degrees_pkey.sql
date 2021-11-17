ALTER TABLE student.student_degrees DROP CONSTRAINT student_degrees_pkey;
ALTER TABLE student.student_degrees ADD CONSTRAINT student_degrees_pkey PRIMARY KEY (sid, plan, term_id);
