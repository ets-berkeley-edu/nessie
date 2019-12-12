/**
 * Copyright ©2020. The Regents of the University of California (Regents). All Rights Reserved.
 *
 * Permission to use, copy, modify, and distribute this software and its documentation
 * for educational, research, and not-for-profit purposes, without fee and without a
 * signed licensing agreement, is hereby granted, provided that the above copyright
 * notice, this paragraph and the following two paragraphs appear in all copies,
 * modifications, and distributions.
 *
 * Contact The Office of Technology Licensing, UC Berkeley, 2150 Shattuck Avenue,
 * Suite 510, Berkeley, CA 94720-1620, (510) 643-7201, otl@berkeley.edu,
 * http://ipira.berkeley.edu/industry-info for commercial licensing opportunities.
 *
 * IN NO EVENT SHALL REGENTS BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 * THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF REGENTS HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
 * SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED
 * "AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
 * ENHANCEMENTS, OR MODIFICATIONS.
 */


--------------------------------------------------------------------
-- DROP & RE-CREATE EXTERNAL SCHEMA
--------------------------------------------------------------------

DROP SCHEMA IF EXISTS {redshift_schema_gradescope} CASCADE;

CREATE EXTERNAL SCHEMA {redshift_schema_gradescope}
FROM data catalog
DATABASE '{redshift_schema_gradescope}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- CREATE EXTERNAL TABLES FOR Gradescope
--------------------------------------------------------------------


CREATE EXTERNAL TABLE {redshift_schema_gradescope}.assignments (
  id INT,
  lms_id INT,
  course_id INT,
  course_lms_id INT,
  title VARCHAR,
  created_at VARCHAR,
  release_date VARCHAR,
  due_date VARCHAR,
  hard_due_date VARCHAR,
  total_points VARCHAR,
  student_submission BOOLEAN,
  regrades_enabled BOOLEAN,
  regrade_request_start VARCHAR,
  regrade_request_end VARCHAR,
  group_submission BOOLEAN,
  group_size INT,
  rubric_visibility_setting VARCHAR,
  type VARCHAR,
  assignment_container_id INT,
  assignment_container_lms_id INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '{loch_s3_gradescope_data_path}/assignments/'
;


CREATE EXTERNAL TABLE {redshift_schema_gradescope}.assignment_containers (
  id INT,
  lms_id INT,
  course_id INT,
  course_lms_id INT,
  title VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '{loch_s3_gradescope_data_path}/assignment_containers/'
;


CREATE EXTERNAL TABLE {redshift_schema_gradescope}.assignment_submissions (
  id INT,
  assignment_id INT,
  assignment_lms_id INT,
  graded BOOLEAN,
  created_at VARCHAR,
  score VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '{loch_s3_gradescope_data_path}/assignment_submissions/'
;


CREATE EXTERNAL TABLE {redshift_schema_gradescope}.assignment_submission_ownerships (
  id INT,
  user_id INT,
  user_lms_id INT,
  assignment_id INT,
  assignment_lms_id INT,
  created_at VARCHAR,
  view_count INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '{loch_s3_gradescope_data_path}/assignment_submission_ownerships/'
;


CREATE EXTERNAL TABLE {redshift_schema_gradescope}.courses (
  id INT,
  lms_id INT,
  shortname VARCHAR,
  name VARCHAR,
  term VARCHAR,
  year INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '{loch_s3_gradescope_data_path}/courses/'
;


CREATE EXTERNAL TABLE {redshift_schema_gradescope}.course_memberships (
  id INT,
  course_id INT,
  course_lms_id INT,
  user_id INT,
  user_lms_id INT,
  role VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '{loch_s3_gradescope_data_path}/course_memberships/'
;


CREATE EXTERNAL TABLE {redshift_schema_gradescope}.questions (
  id INT,
  assignment_id INT,
  assignment_lms_id INT,
  course_id INT,
  course_lms_id INT,
  title VARCHAR,
  type VARCHAR,
  parent_id INT,
  index INT,
  weight VARCHAR,
  scoring_type VARCHAR,
  "floor" BOOLEAN,
  "ceiling" BOOLEAN
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '{loch_s3_gradescope_data_path}/questions/'
;


CREATE EXTERNAL TABLE {redshift_schema_gradescope}.question_submissions (
  id INT,
  assignment_submission_id INT,
  graded BOOLEAN,
  created_at VARCHAR,
  score VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '{loch_s3_gradescope_data_path}/question_submissions/'
;


CREATE EXTERNAL TABLE {redshift_schema_gradescope}.question_submission_evaluations (
  id INT,
  points VARCHAR,
  comments VARCHAR,
  user_id INT,
  user_lms_id VARCHAR,
  created_at VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '{loch_s3_gradescope_data_path}/question_submission_evaluations/'
;


CREATE EXTERNAL TABLE {redshift_schema_gradescope}.rubric_items (
  id INT,
  description VARCHAR,
  weight VARCHAR,
  assignment_id INT,
  assignment_lms_id INT,
  question_id INT,
  created_at VARCHAR,
  position INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '{loch_s3_gradescope_data_path}/rubric_items/'
;


CREATE EXTERNAL TABLE {redshift_schema_gradescope}.rubric_item_evaluations (
  id INT,
  user_id INT,
  user_lms_id VARCHAR,
  question_submission_id INT,
  rubric_item_id INT,
  present BOOLEAN,
  created_at VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '{loch_s3_gradescope_data_path}/rubric_item_evaluations/'
;


CREATE EXTERNAL TABLE {redshift_schema_gradescope}.users (
  id INT,
  lms_id INT,
  email VARCHAR,
  first_name VARCHAR,
  last_name VARCHAR,
  name VARCHAR,
  sid VARCHAR,
  last_session_started_at VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '{loch_s3_gradescope_data_path}/users/'
;
