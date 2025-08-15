BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS student.academic_standing_new
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR NOT NULL,
    acad_standing_action VARCHAR,
    acad_standing_description VARCHAR,
    acad_standing_status VARCHAR,
    action_date VARCHAR
)
DISTKEY (sid)
SORTKEY (sid);

INSERT INTO student.academic_standing_new (
    SELECT sid, term_id, acad_standing_action, NULL AS acad_standing_description, acad_standing_status, action_date
    FROM student.academic_standing
);

DROP TABLE student.academic_standing;
ALTER TABLE student.academic_standing_new RENAME TO academic_standing;

-- Drop and (re)create the staging table.
DROP TABLE student_staging.academic_standing;

CREATE TABLE student_staging.academic_standing
(
    sid VARCHAR NOT NULL,
    term_id VARCHAR NOT NULL,
    acad_standing_action VARCHAR,
    acad_standing_description VARCHAR,
    acad_standing_status VARCHAR,
    action_date VARCHAR
)
DISTKEY (sid)
SORTKEY (sid);

COMMIT;
