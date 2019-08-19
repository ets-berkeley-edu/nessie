BEGIN TRANSACTION;

CREATE TABLE sis_advising_notes.advising_note_topic_mappings (
  boa_topic VARCHAR NOT NULL,
  sis_topic VARCHAR NOT NULL,
  PRIMARY KEY (boa_topic, sis_topic)
);

INSERT INTO sis_advising_notes.advising_note_topic_mappings
(boa_topic, sis_topic)
VALUES
('Academic Progress','Academic Progress'),
('Academic Progress Report (APR)','APR'),
('Change of College','Change of College'),
('Concurrent Enrollment','Concurrent Enrollment'),
('Continued After Dismissal','Continued after Dismissal'),
('Course Add','Course Add'),
('Course Drop','Course Drop'),
('Course Grade Option','Course Grade Option'),
('Course Unit Change','Course Unit Change'),
('Dean Appointment','Dean Appt/DC Preparation'),
('Degree Check','Degree Check'),
('Degree Check Preparation','Dean Appt/DC Preparation'),
('Degree Requirements','Degree Requirements'),
('Dismissal','Probation/Dismissal'),
('Double Major','Dbl Major/Simultaneous Degree'),
('Education Abroad Program (EAP) Reciprocity','EAP Reciprocity'),
('Education Abroad Program (EAP)','EAP/Study Abroad'),
('Excess Units','Excess Units'),
('Incompletes','Incompletes'),
('Late Enrollment','Late Enrollment'),
('Majors','Majors'),
('Minimum Unit Program','Minimum Unit Program'),
('Pass / Not Pass (PNP)','Pass/Not Pass'),
('Pre-Med Advising','Pre-Med Advising'),
('Probation','Probation/Dismissal'),
('Program Planning','Program Planning'),
('Reading & Composition','Reading & Composition'),
('Readmission','Readmission'),
('Refer to Academic Department','Refer to Academic Department'),
('Refer to Career Center','Refer to Career Center'),
('Refer to Resources','Refer to Resources'),
('Refer to The Tang Center','Refer to The Tang Center'),
('Retroactive Addition','Retro Add'),
('Retroactive Drop','Retro Drop'),
('Retroactive Grading Option','Retro Grading Option'),
('Retroactive Unit Change','Retro Unit Change'),
('Retroactive Withdrawal','Retro Withdrawal'),
('Satisfactory Academic Progress (SAP) Appeal','SAP Appeal'),
('Semester Out Rule','Semester Out Rule'),
('Senior Residency','Senior Residency'),
('Simultaneous Degree','Dbl Major/Simultaneous Degree'),
('Special Studies','Special Studies'),
('Student Coduct','Student Coduct'),
('Study Abroad','EAP/Study Abroad'),
('Transfer Coursework','Transfer Coursework'),
('Waive College Requirement','Waive College Requirement'),
('Withdrawal','Withdrawal');

COMMIT TRANSACTION;
