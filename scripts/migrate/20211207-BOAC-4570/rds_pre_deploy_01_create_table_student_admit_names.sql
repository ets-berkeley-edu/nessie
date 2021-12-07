CREATE TABLE boac_advising_oua.student_admit_names
(
    sid VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    PRIMARY KEY (sid, name)
);

INSERT INTO boac_advising_oua.student_admit_names (
  SELECT DISTINCT cs_empl_id,
    unnest(string_to_array(regexp_replace(upper(first_name), '[^\w ]', '', 'g'),' ')) AS name
  FROM boac_advising_oua.student_admits
  UNION
  SELECT DISTINCT cs_empl_id,
    unnest(string_to_array(regexp_replace(upper(middle_name), '[^\w ]', '', 'g'),' ')) AS name
  FROM boac_advising_oua.student_admits
  UNION
  SELECT DISTINCT cs_empl_id,
    unnest(string_to_array(regexp_replace(upper(last_name), '[^\w ]', '', 'g'),' ')) AS name
  FROM boac_advising_oua.student_admits
);