/**
 * Copyright ©2024. The Regents of the University of California (Regents). All Rights Reserved.
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

--------------------------------------------------------------------------------------
-- CREATE BOAC RDS EXTERNAL SCHEMA & DATABASE 
--------------------------------------------------------------------------------------

CREATE EXTERNAL SCHEMA {redshift_schema_boa_rds_data}
FROM data catalog
DATABASE '{redshift_schema_boa_rds_data}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;


--------------------------------------------------------------------------------------
-- CREATE BOAC RDS EXTERNAL TABLES 
--------------------------------------------------------------------------------------


--------------------------------------------------------------------------------------
-- External Table : "alert_views"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."alert_views" (
    "alert_id" INTEGER,
    "viewer_id" INTEGER,
    "created_at" TIMESTAMP,
    "dismissed_at" TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/alert_views/';


--------------------------------------------------------------------------------------
-- External Table : "alerts"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."alerts" (
    "id" INTEGER,
    "sid" VARCHAR(80),
    "alert_type" VARCHAR(80),
    "key" VARCHAR(255),
    "message" VARCHAR(65535),
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP,
    "deleted_at" TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/alerts/';


--------------------------------------------------------------------------------------
-- External Table : "appointments_read"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."appointments_read" (
    "appointment_id" VARCHAR(255),
    "viewer_id" INTEGER,
    "created_at" TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/appointments_read/';


--------------------------------------------------------------------------------------
-- External Table : "authorized_users"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."authorized_users" (
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP,
    "id" INTEGER,
    "uid" VARCHAR(255),
    "is_admin" BOOLEAN,
    "in_demo_mode" BOOLEAN,
    "deleted_at" TIMESTAMP,
    "can_access_canvas_data" BOOLEAN,
    "created_by" VARCHAR(255),
    "is_blocked" BOOLEAN,
    "search_history" ARRAY<VARCHAR(255)>,
    "can_access_advising_data" BOOLEAN,
    "degree_progress_permission" VARCHAR(40),
    "automate_degree_progress_permission" BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/authorized_users/';


--------------------------------------------------------------------------------------
-- External Table : "cohort_filter_events"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."cohort_filter_events" (
    "id" INTEGER,
    "cohort_filter_id" INTEGER,
    "sid" VARCHAR(80),
    "event_type" VARCHAR(40),
    "created_at" TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/cohort_filter_events/';


--------------------------------------------------------------------------------------
-- External Table : "cohort_filters"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."cohort_filters" (
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP,
    "id" INTEGER,
    "name" VARCHAR(255),
    "filter_criteria" VARCHAR(65535),
    "student_count" INTEGER,
    "alert_count" INTEGER,
    "sids" ARRAY<VARCHAR(255)>,
    "domain" VARCHAR(40),
    "owner_id" INTEGER
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/cohort_filters/';


--------------------------------------------------------------------------------------
-- External Table : "degree_progress_categories"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."degree_progress_categories" (
    "id" INTEGER,
    "parent_category_id" INTEGER,
    "template_id" INTEGER,
    "category_type" VARCHAR(40),
    "description" VARCHAR(65535),
    "name" VARCHAR(255),
    "position" INTEGER,
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP,
    "course_units" VARCHAR(255),
    "is_recommended" BOOLEAN,
    "note" VARCHAR(65535),
    "grade" VARCHAR(50),
    "accent_color" VARCHAR(255),
    "is_ignored" BOOLEAN,
    "is_satisfied_by_transfer_course" BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/degree_progress_categories/';


--------------------------------------------------------------------------------------
-- External Table : "degree_progress_category_unit_requirements"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."degree_progress_category_unit_requirements" (
    "category_id" INTEGER,
    "unit_requirement_id" INTEGER
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/degree_progress_category_unit_requirements/';


--------------------------------------------------------------------------------------
-- External Table : "degree_progress_course_unit_requirements"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."degree_progress_course_unit_requirements" (
    "course_id" INTEGER,
    "unit_requirement_id" INTEGER
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/degree_progress_course_unit_requirements/';


--------------------------------------------------------------------------------------
-- External Table : "degree_progress_courses"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."degree_progress_courses" (
    "section_id" INTEGER,
    "sid" VARCHAR(80),
    "term_id" INTEGER,
    "category_id" INTEGER,
    "grade" VARCHAR(50),
    "display_name" VARCHAR(255),
    "note" VARCHAR(65535),
    "units" NUMERIC,
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP,
    "id" INTEGER,
    "degree_check_id" INTEGER,
    "ignore" BOOLEAN,
    "accent_color" VARCHAR(255),
    "manually_created_at" TIMESTAMP,
    "manually_created_by" INTEGER
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/degree_progress_courses/';


--------------------------------------------------------------------------------------
-- External Table : "degree_progress_notes"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."degree_progress_notes" (
    "template_id" INTEGER,
    "body" VARCHAR(65535),
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP,
    "updated_by" INTEGER
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/degree_progress_notes/';


--------------------------------------------------------------------------------------
-- External Table : "degree_progress_templates"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."degree_progress_templates" (
    "id" INTEGER,
    "degree_name" VARCHAR(255),
    "advisor_dept_codes" ARRAY<VARCHAR(255)>,
    "student_sid" VARCHAR(80),
    "created_at" TIMESTAMP,
    "created_by" INTEGER,
    "updated_at" TIMESTAMP,
    "updated_by" INTEGER,
    "deleted_at" TIMESTAMP,
    "parent_template_id" INTEGER
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/degree_progress_templates/';


--------------------------------------------------------------------------------------
-- External Table : "degree_progress_unit_requirements"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."degree_progress_unit_requirements" (
    "id" INTEGER,
    "template_id" INTEGER,
    "name" VARCHAR(255),
    "min_units" NUMERIC,
    "created_at" TIMESTAMP,
    "created_by" INTEGER,
    "updated_at" TIMESTAMP,
    "updated_by" INTEGER
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/degree_progress_unit_requirements/';


--------------------------------------------------------------------------------------
-- External Table : "json_cache"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."json_cache" (
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP,
    "id" INTEGER,
    "key" VARCHAR(255),
    "json" VARCHAR(65535)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/json_cache/';


--------------------------------------------------------------------------------------
-- External Table : "json_cache_staging" -- NOT IN BOAC schema.sql
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."json_cache_staging" (
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP,
    "id" INTEGER,
    "key" VARCHAR(255),
    "json" VARCHAR(65535)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/json_cache_staging/';


--------------------------------------------------------------------------------------
-- External Table : "manually_added_advisees" -- NOT IN BOAC schema.sql
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."manually_added_advisees" (
    "sid" VARCHAR(80),
    "created_at" TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/manually_added_advisees/';


--------------------------------------------------------------------------------------
-- External Table : "note_attachments"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."note_attachments" (
    "id" INTEGER,
    "note_id" INTEGER,
    "path_to_attachment" VARCHAR(255),
    "created_at" TIMESTAMP,
    "deleted_at" TIMESTAMP,
    "uploaded_by_uid" VARCHAR(255)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/note_attachments/';


-- External Table : "note_template_attachments"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."note_template_attachments" (
    "id" INTEGER,
    "note_template_id" INTEGER,
    "path_to_attachment" VARCHAR(255),
    "uploaded_by_uid" VARCHAR(255),
    "created_at" TIMESTAMP,
    "deleted_at" TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/note_template_attachments/';


--------------------------------------------------------------------------------------
-- External Table : "note_template_topics"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."note_template_topics" (
    "id" INTEGER,
    "note_template_id" INTEGER,
    "topic" VARCHAR(50)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/note_template_topics/';


--------------------------------------------------------------------------------------
-- External Table : "note_templates"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."note_templates" (
    "id" INTEGER,
    "creator_id" INTEGER,
    "title" VARCHAR(255),
    "subject" VARCHAR(255),
    "body" VARCHAR(65535),
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP,
    "deleted_at" TIMESTAMP,
    "is_private" BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/note_templates/';


--------------------------------------------------------------------------------------
-- External Table : "note_topics"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."note_topics" (
    "id" INTEGER,
    "note_id" INTEGER,
    "topic" VARCHAR(50),
    "author_uid" VARCHAR(255),
    "deleted_at" TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/note_topics/';


--------------------------------------------------------------------------------------
-- External Table : "notes"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."notes" (
    "id" INTEGER,
    "author_uid" VARCHAR(255),
    "author_name" VARCHAR(255),
    "author_role" VARCHAR(255),
    "author_dept_codes" ARRAY<VARCHAR(255)>,
    "sid" VARCHAR(80),
    "subject" VARCHAR(255),
    "body" VARCHAR(65535),
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP,
    "deleted_at" TIMESTAMP,
    "is_private" BOOLEAN,
    "contact_type" VARCHAR(40),
    "set_date" DATE,
    "is_draft" BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/notes/';


--------------------------------------------------------------------------------------
-- External Table : "notes_read"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."notes_read" (
    "note_id" VARCHAR(255),
    "viewer_id" INTEGER,
    "created_at" TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/notes_read/';


--------------------------------------------------------------------------------------
-- External Table : "student_group_members"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."student_group_members" (
    "student_group_id" INTEGER,
    "sid" VARCHAR(80)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/student_group_members/';


--------------------------------------------------------------------------------------
-- External Table : "student_groups"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."student_groups" (
    "id" INTEGER,
    "owner_id" INTEGER,
    "name" VARCHAR(255),
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP,
    "domain" VARCHAR(40)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/student_groups/';


--------------------------------------------------------------------------------------
-- External Table : "tool_settings"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."tool_settings" (
    "id" INTEGER,
    "key" VARCHAR(255),
    "value" VARCHAR(65535),
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/tool_settings/';


--------------------------------------------------------------------------------------
-- External Table : "topics"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."topics" (
    "id" INTEGER,
    "topic" VARCHAR(50),
    "created_at" TIMESTAMP,
    "deleted_at" TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/topics/';


--------------------------------------------------------------------------------------
-- External Table : "university_dept_members"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."university_dept_members" (
    "university_dept_id" INTEGER,
    "authorized_user_id" INTEGER,
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP,
    "automate_membership" BOOLEAN,
    "role" VARCHAR(50)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/university_dept_members/';


--------------------------------------------------------------------------------------
-- External Table : "university_depts"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."university_depts" (
    "id" INTEGER,
    "dept_code" VARCHAR(80),
    "dept_name" VARCHAR(255),
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/university_depts/';


--------------------------------------------------------------------------------------
-- External Table : "user_logins"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_boa_rds_data}"."user_logins" (
    "id" INTEGER,
    "uid" VARCHAR(255),
    "created_at" TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '{boa_rds_data_path}/user_logins/';

--------------------------------------------------------------------------------------
-- END OF BOAC DATA EXT SCHEMA DDL STATEMENTS
--------------------------------------------------------------------------------------
