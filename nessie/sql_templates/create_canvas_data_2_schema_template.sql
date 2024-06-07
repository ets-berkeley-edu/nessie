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
-- CREATE EXTERNAL SCHEMA & DATABASE 
--------------------------------------------------------------------------------------

CREATE EXTERNAL SCHEMA {redshift_schema_canvas_data_2}
FROM data catalog
DATABASE '{redshift_schema_canvas_data_2}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;


--------------------------------------------------------------------------------------
-- CREATE CANVAS DATA 2 EXTERNAL TABLES 
--------------------------------------------------------------------------------------


--------------------------------------------------------------------------------------
-- External Table : "access_tokens"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."access_tokens" (
	"id" BIGINT,
	"developer_key_id" BIGINT,
	"user_id" BIGINT,
	"real_user_id" BIGINT,
	"last_used_at" TIMESTAMP,
	"expires_at" TIMESTAMP,
	"purpose" VARCHAR(255),
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"scopes" VARCHAR,
	"workflow_state" VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/access_tokens/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "accounts" 
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."accounts" (
	"id" BIGINT,
	"name" VARCHAR(255),
	"deleted_at" TIMESTAMP,
	"parent_account_id" BIGINT,
	"current_sis_batch_id" BIGINT,
	"storage_quota" BIGINT,
	"default_storage_quota" BIGINT,
	"default_locale" VARCHAR(255),
	"default_user_storage_quota" BIGINT,
	"default_group_storage_quota" BIGINT,
	"integration_id" VARCHAR(255),
	"lti_context_id" VARCHAR(255),
	"consortium_parent_account_id" BIGINT,
	"course_template_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"default_time_zone" VARCHAR(255),
	"uuid" VARCHAR(255),
	"sis_source_id" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/accounts/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "account_users"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."account_users" (
	"id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"account_id" BIGINT,
	"role_id" BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/account_users/'
TABLE PROPERTIES('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "courses" 
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."courses" (
	"id" BIGINT,
	"storage_quota" BIGINT,
	"integration_id" VARCHAR(255),
	"lti_context_id" VARCHAR(255),
	"sis_batch_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"account_id" BIGINT,
	"grading_standard_id" BIGINT,
	"start_at" TIMESTAMP,
	"sis_source_id" VARCHAR(255),
	"group_weighting_scheme" VARCHAR(255),
	"conclude_at" TIMESTAMP,
	"is_public" BOOLEAN,
	"allow_student_wiki_edits" BOOLEAN,
	"syllabus_body" VARCHAR(65535),
	"default_wiki_editing_roles" VARCHAR(255),
	"wiki_id" BIGINT,
	"allow_student_organized_groups" BOOLEAN,
	"course_code" VARCHAR(255),
	"default_view" VARCHAR(255),
	"abstract_course_id" BIGINT,
	"enrollment_term_id" BIGINT,
	"open_enrollment" BOOLEAN,
	"tab_configuration" VARCHAR,
	"turnitin_comments" VARCHAR,
	"self_enrollment" BOOLEAN,
	"license" VARCHAR(255),
	"indexed" BOOLEAN,
	"restrict_enrollments_to_course_dates" BOOLEAN,
	"template_course_id" BIGINT,
	"replacement_course_id" BIGINT,
	"public_description" VARCHAR,
	"self_enrollment_code" VARCHAR(255),
	"self_enrollment_limit" INTEGER,
	"turnitin_id" BIGINT,
	"show_announcements_on_home_page" BOOLEAN,
	"home_page_announcement_limit" INTEGER,
	"latest_outcome_import_id" BIGINT,
	"grade_passback_setting" VARCHAR(255),
	"template" BOOLEAN,
	"homeroom_course" BOOLEAN,
	"sync_enrollments_from_homeroom" BOOLEAN,
	"homeroom_course_id" BIGINT,
	"locale" VARCHAR(255),
	"name" VARCHAR(255),
	"time_zone" VARCHAR(255),
	"uuid" VARCHAR(255),
	"settings" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/courses/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "course_sections"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."course_sections" (
	"id" BIGINT,
	"name" VARCHAR(255),
	"course_id" BIGINT,
	"integration_id" VARCHAR(255),
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"sis_batch_id" BIGINT,
	"start_at" TIMESTAMP,
	"end_at" TIMESTAMP,
	"sis_source_id" VARCHAR(255),
	"default_section" BOOLEAN,
	"accepting_enrollments" BOOLEAN,
	"restrict_enrollments_to_section_dates" BOOLEAN,
	"nonxlist_course_id" BIGINT,
	"enrollment_term_id" BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/course_sections/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "users" 
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."users" (
	"id" BIGINT ,
	"deleted_at" TIMESTAMP,
	"storage_quota" BIGINT,
	"lti_context_id" VARCHAR(255),
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"sortable_name" VARCHAR(255),
	"avatar_image_url" VARCHAR(255),
	"avatar_image_source" VARCHAR(255),
	"avatar_image_updated_at" TIMESTAMP,
	"short_name" VARCHAR(255),
	"last_logged_out" TIMESTAMP,
	"pronouns" VARCHAR(255),
	"merged_into_user_id" BIGINT,
	"locale" VARCHAR(255),
	"name" VARCHAR(255),
	"time_zone" VARCHAR(255),
	"uuid" VARCHAR(255),
	"school_name" VARCHAR(255),
	"school_position" VARCHAR(255),
	"public" BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/users/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "assignments"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."assignments" (
	"id" BIGINT,
	"integration_id" VARCHAR(255),
	"lti_context_id" VARCHAR(255),
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"due_at" TIMESTAMP,
	"unlock_at" TIMESTAMP,
	"lock_at" TIMESTAMP,
	"points_possible" DOUBLE PRECISION,
	"grading_type" VARCHAR(255),
	"submission_types" VARCHAR,
	"assignment_group_id" BIGINT,
	"grading_standard_id" BIGINT,
	"submissions_downloads" INTEGER,
	"peer_review_count" INTEGER,
	"peer_reviews_due_at" TIMESTAMP,
	"peer_reviews_assigned" BOOLEAN,
	"peer_reviews" BOOLEAN,
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"automatic_peer_reviews" BOOLEAN,
	"all_day" BOOLEAN,
	"all_day_date" DATE,
	"could_be_locked" BOOLEAN,
	"migration_id" VARCHAR(255),
	"grade_group_students_individually" BOOLEAN,
	"anonymous_peer_reviews" BOOLEAN,
	"turnitin_enabled" BOOLEAN,
	"allowed_extensions" VARCHAR(255),
	"group_category_id" BIGINT,
	"freeze_on_copy" BOOLEAN,
	"only_visible_to_overrides" BOOLEAN,
	"post_to_sis" BOOLEAN,
	"moderated_grading" BOOLEAN,
	"grades_published_at" TIMESTAMP,
	"omit_from_final_grade" BOOLEAN,
	"intra_group_peer_reviews" BOOLEAN,
	"vericite_enabled" BOOLEAN,
	"anonymous_instructor_annotations" BOOLEAN,
	"duplicate_of_id" BIGINT,
	"anonymous_grading" BOOLEAN,
	"graders_anonymous_to_graders" BOOLEAN,
	"grader_count" INTEGER,
	"grader_comments_visible_to_graders" BOOLEAN,
	"grader_section_id" BIGINT,
	"final_grader_id" BIGINT,
	"grader_names_visible_to_final_grader" BOOLEAN,
	"allowed_attempts" INTEGER,
	"sis_source_id" VARCHAR(255),
	"annotatable_attachment_id" BIGINT,
	"important_dates" BOOLEAN,
	"description" VARCHAR,
	"position" INTEGER,
	"title" VARCHAR(255),
	"turnitin_settings" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/assignments/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "assignment_groups"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."assignment_groups" (
	"id" BIGINT,
	"name" VARCHAR(255),
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"default_assignment_name" VARCHAR(255),
	"group_weight" DOUBLE PRECISION,
	"migration_id" VARCHAR(255),
	"sis_source_id" VARCHAR(255),
	"position" INTEGER,
	"rules" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/assignment_groups/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "assignment_override_students"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."assignment_override_students" (
	"id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"assignment_id" BIGINT,
	"quiz_id" BIGINT,
	"assignment_override_id" BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/assignment_override_students/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "assignment_overrides"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."assignment_overrides" (
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"due_at" TIMESTAMP,
	"unlock_at" TIMESTAMP,
	"lock_at" TIMESTAMP,
	"all_day" BOOLEAN,
	"assignment_version" INTEGER,
	"set_type" VARCHAR(255),
	"set_id" BIGINT,
	"due_at_overridden" BOOLEAN,
	"unlock_at_overridden" BOOLEAN,
	"lock_at_overridden" BOOLEAN,
	"quiz_id" BIGINT,
	"quiz_version" INTEGER,
	"assignment_id" BIGINT,
	"all_day_date" DATE,
	"title" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/assignment_overrides/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "attachments"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."attachments" (
	"id" BIGINT,
	"deleted_at" TIMESTAMP,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"unlock_at" TIMESTAMP,
	"lock_at" TIMESTAMP,
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"folder_id" BIGINT,
	"filename" VARCHAR(255),
	"locked" BOOLEAN,
	"file_state" VARCHAR(255),
	"media_entry_id" VARCHAR(255),
	"md5" VARCHAR,
	"replacement_attachment_id" BIGINT,
	"usage_rights_id" BIGINT,
	"modified_at" TIMESTAMP,
	"viewed_at" TIMESTAMP,
	"could_be_locked" BOOLEAN,
	"migration_id" VARCHAR(255),
	"namespace" VARCHAR(255),
	"size" BIGINT,
	"display_name" VARCHAR,
	"content_type" VARCHAR(255),
	"uuid" VARCHAR(255),
	"root_attachment_id" BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/attachments/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "context_external_tools"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."context_external_tools"
(
	"id" BIGINT,
	"developer_key_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"migration_id" VARCHAR(255),
	"consumer_key" VARCHAR,
	"cloned_item_id" BIGINT,
	"tool_id" VARCHAR(255),
	"not_selectable" BOOLEAN,
	"app_center_id" VARCHAR(255),
	"allow_membership_service_access" BOOLEAN,
	"description" VARCHAR(65535),
	"name" VARCHAR(255),
	"domain" VARCHAR(255),
	"url" VARCHAR(4096),
	"settings" varchar
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/context_external_tools/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "attachment_associations"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."attachment_associations"
(
	"id" BIGINT,
	"attachment_id" BIGINT,
	"context_id" BIGINT,
	"context_type" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/attachment_associations/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "pseudonyms"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."pseudonyms"
(
	"id" BIGINT,
	"deleted_at" TIMESTAMP,
	"integration_id" VARCHAR(255),
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"account_id" BIGINT,
	"sis_batch_id" BIGINT,
	"unique_id" VARCHAR(255),
	"login_count" INTEGER,
	"failed_login_count" INTEGER,
	"last_request_at" TIMESTAMP,
	"last_login_at" TIMESTAMP,
	"current_login_at" TIMESTAMP,
	"last_login_ip" VARCHAR(255),
	"current_login_ip" VARCHAR(255),
	"sis_user_id" VARCHAR(255),
	"authentication_provider_id" BIGINT,
	"position" INTEGER
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/pseudonyms/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "submissions"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."submissions" (
	"id" BIGINT,
	"attachment_id" BIGINT,
	"course_id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"assignment_id" BIGINT,
	"media_comment_id" VARCHAR(255),
	"media_comment_type" VARCHAR(255),
	"attachment_ids" VARCHAR(255),
	"posted_at" TIMESTAMP,
	"group_id" BIGINT,
	"score" DOUBLE PRECISION,
	"attempt" INTEGER,
	"submitted_at" TIMESTAMP,
	"quiz_submission_id" BIGINT,
	"extra_attempts" INTEGER,
	"grading_period_id" BIGINT,
	"grade" VARCHAR(255),
	"submission_type" VARCHAR(255),
	"processed" BOOLEAN,
	"grade_matches_current_submission" BOOLEAN,
	"published_score" DOUBLE PRECISION,
	"published_grade" VARCHAR(255),
	"graded_at" TIMESTAMP,
	"student_entered_score" DOUBLE PRECISION,
	"grader_id" BIGINT,
	"submission_comments_count" INTEGER,
	"media_object_id" BIGINT,
	"turnitin_data" VARCHAR,
	"cached_due_date" TIMESTAMP,
	"excused" BOOLEAN,
	"graded_anonymously" BOOLEAN,
	"late_policy_status" VARCHAR(16),
	"points_deducted" decimal(6, 2),
	"seconds_late_override" BIGINT,
	"lti_user_id" VARCHAR(255),
	"anonymous_id" VARCHAR(5),
	"last_comment_at" TIMESTAMP,
	"cached_quiz_lti" BOOLEAN,
	"cached_tardiness" VARCHAR(16),
	"resource_link_lookup_uuid" VARCHAR(255),
	"redo_request" BOOLEAN,
	"body" VARCHAR(65535),
	"url" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/submissions/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "submission_comments"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."submission_comments" 
(
	"id" BIGINT,
	"comment" VARCHAR,
	"submission_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"author_id" BIGINT,
	"media_comment_id" VARCHAR(255),
	"media_comment_type" VARCHAR(255),
	"attachment_ids" VARCHAR(255),
	"attempt" INTEGER,
	"hidden" BOOLEAN,
	"author_name" VARCHAR(255),
	"group_comment_id" VARCHAR(255),
	"assessment_request_id" BIGINT,
	"anonymous" BOOLEAN,
	"teacher_only_comment" BOOLEAN,
	"provisional_grade_id" BIGINT,
	"draft" BOOLEAN,
	"edited_at" TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/submission_comments/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "submission_versions"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."submission_versions"
(
	"id" BIGINT,
	"user_id" BIGINT,
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"assignment_id" BIGINT,
	"version_id" BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/submission_versions/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "enrollments"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."enrollments" (
	"id" BIGINT,
	"sis_batch_id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"role_id" BIGINT,
	"start_at" TIMESTAMP,
	"end_at" TIMESTAMP,
	"course_id" BIGINT,
	"completed_at" TIMESTAMP,
	"course_section_id" BIGINT,
	"grade_publishing_status" VARCHAR(255),
	"associated_user_id" BIGINT,
	"self_enrolled" BOOLEAN,
	"limit_privileges_to_course_section" BOOLEAN,
	"last_activity_at" TIMESTAMP,
	"total_activity_time" INTEGER,
	"sis_pseudonym_id" BIGINT,
	"last_attended_at" TIMESTAMP,
	"type" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/enrollments/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "enrollment_terms"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."enrollment_terms" 
(
	"id" BIGINT,
	"name" VARCHAR(255),
	"integration_id" VARCHAR(255),
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"sis_batch_id" BIGINT,
	"start_at" TIMESTAMP,
	"end_at" TIMESTAMP,
	"sis_source_id" VARCHAR(255),
	"term_code" VARCHAR(255),
	"grading_period_group_id" BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/enrollment_terms/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "discussion_entries"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."discussion_entries"
(
	"id" BIGINT,
	"message" VARCHAR(65535),
	"attachment_id" BIGINT,
	"deleted_at" TIMESTAMP,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"migration_id" VARCHAR(255),
	"discussion_topic_id" BIGINT,
	"parent_id" BIGINT,
	"editor_id" BIGINT,
	"root_entry_id" BIGINT,
	"depth" INTEGER,
	"rating_count" INTEGER,
	"rating_sum" INTEGER
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/discussion_entries/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "discussion_topics"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."discussion_topics" (
	"id" BIGINT,
	"message" VARCHAR(65535),
	"type" VARCHAR(255),
	"attachment_id" BIGINT,
	"deleted_at" TIMESTAMP,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"lock_at" TIMESTAMP,
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"locked" BOOLEAN,
	"assignment_id" BIGINT,
	"migration_id" VARCHAR(255),
	"group_category_id" BIGINT,
	"cloned_item_id" BIGINT,
	"last_reply_at" TIMESTAMP,
	"delayed_post_at" TIMESTAMP,
	"posted_at" TIMESTAMP,
	"root_topic_id" BIGINT,
	"old_assignment_id" BIGINT,
	"subtopics_refreshed_at" TIMESTAMP,
	"external_feed_id" BIGINT,
	"podcast_enabled" BOOLEAN,
	"podcast_has_student_posts" BOOLEAN,
	"require_initial_post" BOOLEAN,
	"editor_id" BIGINT,
	"discussion_type" VARCHAR(255),
	"pinned" BOOLEAN,
	"allow_rating" BOOLEAN,
	"only_graders_can_rate" BOOLEAN,
	"sort_by_rating" BOOLEAN,
	"todo_date" TIMESTAMP,
	"is_section_specific" BOOLEAN,
	"position" INTEGER,
	"title" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/discussion_topics/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "discussion_entry_participants"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."discussion_entry_participants"
(
	"id" BIGINT,
	"user_id" BIGINT,
	"workflow_state" VARCHAR(255),
	"forced_read_state" BOOLEAN,
	"discussion_entry_id" BIGINT,
	"rating" INTEGER
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/discussion_entry_participants/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "discussion_topic_participants"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."discussion_topic_participants"
(
	"id" BIGINT,
	"user_id" BIGINT,
	"workflow_state" VARCHAR(255),
	"subscribed" BOOLEAN,
	"unread_entry_count" INTEGER,
	"discussion_topic_id" BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/discussion_topic_participants/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "scores"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."scores"
(
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"assignment_group_id" BIGINT,
	"enrollment_id" BIGINT,
	"grading_period_id" BIGINT,
	"current_score" DOUBLE PRECISION,
	"final_score" DOUBLE PRECISION,
	"course_score" BOOLEAN,
	"unposted_current_score" DOUBLE PRECISION,
	"unposted_final_score" DOUBLE PRECISION,
	"current_points" DOUBLE PRECISION,
	"unposted_current_points" DOUBLE PRECISION,
	"final_points" DOUBLE PRECISION,
	"unposted_final_points" DOUBLE PRECISION,
	"override_score" DOUBLE PRECISION
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/scores/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "roles"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."roles"
(
	"id" BIGINT,
	"name" VARCHAR(255),
	"deleted_at" TIMESTAMP,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"account_id" BIGINT,
	"base_role_type" VARCHAR(255) 
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/roles/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "score_statistics"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."score_statistics" (
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"assignment_id" BIGINT,
	"mean" DOUBLE PRECISION,
	"count" INTEGER,
	"minimum" DOUBLE PRECISION,
	"maximum" DOUBLE PRECISION
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/score_statistics/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "developer_keys"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."developer_keys" (
	"id" BIGINT,
	"name" VARCHAR(255),
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"scopes" VARCHAR,
	"workflow_state" VARCHAR(255),
	"account_id" BIGINT,
	"redirect_uri" VARCHAR(255),
	"icon_url" VARCHAR(255),
	"redirect_uris" VARCHAR(255),
	"notes" VARCHAR,
	"access_token_count" INTEGER,
	"require_scopes" BOOLEAN,
	"test_cluster_only" BOOLEAN,
	"public_jwk" VARCHAR,
	"allow_includes" BOOLEAN,
	"is_lti_key" BOOLEAN,
	"client_credentials_audience" VARCHAR,
	"email" VARCHAR(255),
	"user_name" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/developer_keys/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "developer_key_account_bindings"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."developer_key_account_bindings" (
	"id" BIGINT,
	"account_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"developer_key_id" BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/developer_key_account_bindings/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "enrollment_dates_overrides"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."enrollment_dates_overrides" (
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"start_at" TIMESTAMP,
	"end_at" TIMESTAMP,
	"enrollment_term_id" BIGINT,
	"enrollment_type" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/enrollment_dates_overrides/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "enrollment_states"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."enrollment_states" (
	"enrollment_id" BIGINT,
	"updated_at" TIMESTAMP,
	"state_is_current" BOOLEAN,
	"state_started_at" TIMESTAMP,
	"state_valid_until" TIMESTAMP,
	"restricted_access" BOOLEAN,
	"access_is_current" BOOLEAN,
	"state" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/enrollment_states/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "folders"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."folders" (
	"id" BIGINT,
	"name" VARCHAR(255),
	"full_name" VARCHAR,
	"deleted_at" TIMESTAMP,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"unlock_at" TIMESTAMP,
	"lock_at" TIMESTAMP,
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"locked" BOOLEAN,
	"cloned_item_id" BIGINT,
	"submission_context_code" VARCHAR(255),
	"parent_folder_id" BIGINT,
	"unique_type" VARCHAR(255),
	"position" INTEGER
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/folders/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "favorites"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."favorites" (
	"id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"context_id" BIGINT,
	"context_type" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/favorites/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "grading_period_groups"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."grading_period_groups" 
(
	"id" BIGINT,
	"account_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"course_id" BIGINT,
	"weighted" BOOLEAN,
	"display_totals_for_all_grading_periods" BOOLEAN,
	"title" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/grading_period_groups/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "grading_periods"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."grading_periods" 
(
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"grading_period_group_id" BIGINT,
	"start_date" TIMESTAMP,
	"end_date" TIMESTAMP,
	"close_date" TIMESTAMP,
	"title" VARCHAR(255),
	"weight" DOUBLE PRECISION
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/grading_periods/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "grading_standards"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."grading_standards" (
	"id" BIGINT,
	"version" INTEGER,
	"context_code" VARCHAR(255),
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"migration_id" VARCHAR(255),
	"title" VARCHAR(255),
	"data" varchar
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/grading_standards/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "quiz_groups"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."quiz_groups" 
(
	"id" BIGINT,
	"name" VARCHAR(255),
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"assessment_question_bank_id" BIGINT,
	"quiz_id" BIGINT,
	"migration_id" VARCHAR(255),
	"pick_count" INTEGER,
	"question_points" DOUBLE PRECISION,
	"position" INTEGER
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/quiz_groups/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "quiz_questions"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."quiz_questions" 
(
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"quiz_id" BIGINT,
	"migration_id" VARCHAR(255),
	"quiz_group_id" BIGINT,
	"assessment_question_id" BIGINT,
	"assessment_question_version" INTEGER,
	"position" INTEGER,
	"question_data" varchar
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/quiz_questions/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "quiz_submissions"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."quiz_submissions" (
	"id" BIGINT,
	"submission_id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"quiz_id" BIGINT,
	"quiz_version" INTEGER,
	"started_at" TIMESTAMP,
	"finished_at" TIMESTAMP,
	"end_at" TIMESTAMP,
	"score" DOUBLE PRECISION,
	"attempt" INTEGER,
	"submission_data" VARCHAR,
	"kept_score" DOUBLE PRECISION,
	"fudge_points" DOUBLE PRECISION,
	"quiz_points_possible" DOUBLE PRECISION,
	"extra_attempts" INTEGER,
	"temporary_user_code" VARCHAR(255),
	"extra_time" INTEGER,
	"manually_scored" BOOLEAN,
	"manually_unlocked" BOOLEAN,
	"was_preview" BOOLEAN,
	"score_before_regrade" DOUBLE PRECISION,
	"has_seen_results" BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/quiz_submissions/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "quizzes"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."quizzes" (
	"id" BIGINT,
	"deleted_at" TIMESTAMP,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"due_at" TIMESTAMP,
	"unlock_at" TIMESTAMP,
	"lock_at" TIMESTAMP,
	"points_possible" DOUBLE PRECISION,
	"assignment_group_id" BIGINT,
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"assignment_id" BIGINT,
	"could_be_locked" BOOLEAN,
	"migration_id" VARCHAR(255),
	"only_visible_to_overrides" BOOLEAN,
	"allowed_attempts" INTEGER,
	"published_at" TIMESTAMP,
	"shuffle_answers" BOOLEAN,
	"show_correct_answers" BOOLEAN,
	"time_limit" INTEGER,
	"scoring_policy" VARCHAR(255),
	"quiz_type" VARCHAR(255),
	"access_code" VARCHAR(255),
	"question_count" INTEGER,
	"anonymous_submissions" BOOLEAN,
	"hide_results" VARCHAR(255),
	"ip_filter" VARCHAR(255),
	"require_lockdown_browser" BOOLEAN,
	"require_lockdown_browser_for_results" BOOLEAN,
	"one_question_at_a_time" BOOLEAN,
	"cant_go_back" BOOLEAN,
	"show_correct_answers_at" TIMESTAMP,
	"hide_correct_answers_at" TIMESTAMP,
	"require_lockdown_browser_monitor" BOOLEAN,
	"one_time_results" BOOLEAN,
	"show_correct_answers_last_attempt" BOOLEAN,
	"unpublished_question_count" INTEGER,
	"description" VARCHAR,
	"title" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/quizzes/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "role_overrides"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."role_overrides" (
	"id" BIGINT,
	"permission" VARCHAR(255),
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"role_id" BIGINT,
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"locked" BOOLEAN,
	"enabled" BOOLEAN,
	"applies_to_self" BOOLEAN,
	"applies_to_descendants" BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/role_overrides/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "assessment_question_banks"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."assessment_question_banks" (
	"id" BIGINT,
	"deleted_at" TIMESTAMP,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"migration_id" VARCHAR(255),
	"title" varchar
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/assessment_question_banks/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "assessment_questions"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."assessment_questions" (
	"id" BIGINT,
	"name" VARCHAR,
	"deleted_at" TIMESTAMP,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"question_data" VARCHAR,
	"assessment_question_bank_id" BIGINT,
	"migration_id" VARCHAR(255),
	"position" INTEGER
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/assessment_questions/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "calendar_events"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."calendar_events" (
	"id" BIGINT,
	"deleted_at" TIMESTAMP,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"location_address" VARCHAR,
	"start_at" TIMESTAMP,
	"end_at" TIMESTAMP,
	"context_code" VARCHAR(255),
	"time_zone_edited" VARCHAR(255),
	"parent_calendar_event_id" BIGINT,
	"effective_context_code" VARCHAR(255),
	"participants_per_appointment" INTEGER,
	"comments" VARCHAR(65535),
	"web_conference_id" BIGINT,
	"all_day" BOOLEAN,
	"all_day_date" DATE,
	"migration_id" VARCHAR(255),
	"important_dates" BOOLEAN,
	"location_name" VARCHAR,
	"description" VARCHAR,
	"title" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/calendar_events/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "canvadocs_annotation_contexts"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."canvadocs_annotation_contexts"
(
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"attachment_id" BIGINT,
	"submission_id" BIGINT,
	"launch_id" VARCHAR,
	"submission_attempt" INTEGER
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/canvadocs_annotation_contexts/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "comment_bank_items"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."comment_bank_items" (
	"id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP(6),
	"updated_at" TIMESTAMP(6),
	"workflow_state" VARCHAR(255),
	"course_id" BIGINT,
	"comment" VARCHAR(65535)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/comment_bank_items/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "communication_channels"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."communication_channels" (
	"id" BIGINT,
	"path" VARCHAR(255),
	"path_type" VARCHAR(255),
	"pseudonym_id" BIGINT,
	"bounce_count" INTEGER,
	"confirmation_code_expires_at" TIMESTAMP,
	"confirmation_sent_count" INTEGER,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"position" INTEGER
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/communication_channels/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "content_migrations"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."content_migrations" (
	"id" BIGINT,
	"attachment_id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"overview_attachment_id" BIGINT,
	"exported_attachment_id" BIGINT,
	"source_course_id" BIGINT,
	"migration_type" VARCHAR(255),
	"child_subscription_id" BIGINT,
	"migration_settings" VARCHAR(65535),
	"started_at" TIMESTAMP,
	"finished_at" TIMESTAMP,
	"progress" DOUBLE PRECISION
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/content_migrations/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "content_participation_counts"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."content_participation_counts" (
	"id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"unread_count" INTEGER,
	"content_type" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/content_participation_counts/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "content_participations"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."content_participations" (
	"id" BIGINT,
	"user_id" BIGINT,
	"workflow_state" VARCHAR(255),
	"content_id" BIGINT,
	"content_type" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/content_participations/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "content_shares"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."content_shares" (
	"id" BIGINT,
	"name" VARCHAR(255),
	"type" VARCHAR(255),
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"content_export_id" BIGINT,
	"sender_id" BIGINT,
	"read_state" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/content_shares/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "content_tags"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."content_tags" (
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"context_code" VARCHAR(255),
	"comments" VARCHAR(65535),
	"migration_id" VARCHAR(255),
	"content_id" BIGINT,
	"tag_type" VARCHAR(255),
	"context_module_id" BIGINT,
	"learning_outcome_id" BIGINT,
	"mastery_score" DOUBLE PRECISION,
	"rubric_association_id" BIGINT,
	"associated_asset_id" BIGINT,
	"associated_asset_type" VARCHAR(255),
	"link_settings" VARCHAR(255),
	"new_tab" BOOLEAN,
	"position" INTEGER,
	"content_type" VARCHAR(255),
	"url" VARCHAR,
	"title" VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/content_tags/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "context_module_progressions"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."context_module_progressions" 
(
	"id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"requirements_met" VARCHAR,
	"collapsed" BOOLEAN,
	"current_position" INTEGER,
	"completed_at" TIMESTAMP,
	"current" BOOLEAN,
	"evaluated_at" TIMESTAMP,
	"incomplete_requirements" VARCHAR,
	"context_module_id" BIGINT,
	"lock_version" INTEGER
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/context_module_progressions/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "context_modules"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."context_modules" 
(
	"id" BIGINT,
	"name" VARCHAR,
	"deleted_at" TIMESTAMP,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"unlock_at" TIMESTAMP,
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"migration_id" VARCHAR(255),
	"prerequisites" VARCHAR,
	"completion_requirements" VARCHAR,
	"require_sequential_progress" BOOLEAN,
	"completion_events" VARCHAR,
	"requirement_count" INTEGER,
	"position" INTEGER
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/context_modules/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "conversation_message_participants"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."conversation_message_participants" (
	"id" BIGINT,
	"deleted_at" TIMESTAMP,
	"user_id" BIGINT,
	"workflow_state" VARCHAR(255),
	"conversation_message_id" BIGINT,
	"conversation_participant_id" BIGINT,
	"tags" VARCHAR(1000)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/conversation_message_participants/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "conversation_messages"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."conversation_messages" (
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"conversation_id" BIGINT,
	"author_id" BIGINT,
	"generated" BOOLEAN,
	"forwarded_message_ids" VARCHAR,
	"media_comment_id" VARCHAR(255),
	"media_comment_type" VARCHAR(255),
	"asset_id" BIGINT,
	"asset_type" VARCHAR(255),
	"attachment_ids" VARCHAR,
	"has_attachments" BOOLEAN,
	"has_media_objects" BOOLEAN,
	"body" VARCHAR(65535)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/conversation_messages/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "conversation_participants"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."conversation_participants" (
	"id" BIGINT,
	"user_id" BIGINT,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"has_attachments" BOOLEAN,
	"has_media_objects" BOOLEAN,
	"last_message_at" TIMESTAMP,
	"subscribed" BOOLEAN,
	"message_count" INTEGER,
	"label" VARCHAR(255),
	"tags" VARCHAR(1000)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/conversation_participants/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "conversations"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."conversations" (
	"id" BIGINT,
	"updated_at" TIMESTAMP,
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"has_attachments" BOOLEAN,
	"has_media_objects" BOOLEAN,
	"subject" VARCHAR(255),
	"tags" VARCHAR(1000)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/conversations/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "course_account_associations"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."course_account_associations" (
	"id" BIGINT,
	"course_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"account_id" BIGINT,
	"course_section_id" BIGINT,
	"depth" INTEGER
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/course_account_associations/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "custom_gradebook_column_data"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."custom_gradebook_column_data" (
	"id" BIGINT,
	"content" VARCHAR(255),
	"user_id" BIGINT,
	"custom_gradebook_column_id" BIGINT 
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/custom_gradebook_column_data/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "custom_gradebook_columns"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."custom_gradebook_columns" (
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"course_id" BIGINT,
	"teacher_notes" BOOLEAN,
	"position" INTEGER,
	"read_only" BOOLEAN,
	"title" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/custom_gradebook_columns/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "Wiki Pages"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."wiki_pages" (
	"id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR(255),
	"context_id" BIGINT,
	"context_type" VARCHAR(255),
	"assignment_id" BIGINT,
	"migration_id" VARCHAR(255),
	"wiki_id" BIGINT,
	"old_assignment_id" BIGINT,
	"todo_date" TIMESTAMP,
	"editing_roles" VARCHAR(255),
	"revised_at" TIMESTAMP,
	"body" VARCHAR(65535),
	"url" VARCHAR,
	"title" VARCHAR(255),
	"protected_editing" BOOLEAN,
	"could_be_locked" BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/wiki_pages/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "wikis"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."wikis" (
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"front_page_url" VARCHAR,
	"has_no_front_page" BOOLEAN,
	"title" VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/wikis/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "group_memberships"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."group_memberships" (
	"id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"sis_batch_id" BIGINT,
	"group_id" BIGINT,
	"moderator" BOOLEAN,
	"uuid" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/group_memberships/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "groups"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."groups" (
	"id" BIGINT,
	"name" VARCHAR(255),
	"deleted_at" TIMESTAMP,
	"storage_quota" BIGINT,
	"lti_context_id" VARCHAR(255),
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"account_id" BIGINT,
	"sis_batch_id" BIGINT,
	"context_id" BIGINT,
	"context_type" VARCHAR,
	"migration_id" VARCHAR(255),
	"group_category_id" BIGINT,
	"sis_source_id" VARCHAR(255),
	"is_public" BOOLEAN,
	"wiki_id" BIGINT,
	"max_membership" INTEGER,
	"join_level" VARCHAR(255),
	"avatar_attachment_id" BIGINT,
	"leader_id" BIGINT,
	"description" VARCHAR(65535),
	"uuid" VARCHAR(255),
	"default_view" VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/groups/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "group_categories"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."group_categories" (
	"id" BIGINT,
	"name" VARCHAR(255),
	"deleted_at" TIMESTAMP,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"sis_batch_id" BIGINT,
	"context_id" BIGINT,
	"context_type" VARCHAR,
	"sis_source_id" VARCHAR,
	"role" VARCHAR,
	"self_signup" VARCHAR(255),
	"group_limit" INTEGER,
	"auto_leader" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/group_categories/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "late_policies"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."late_policies" (
	"id" BIGINT,
	"course_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"missing_submission_deduction_enabled" BOOLEAN,
	"missing_submission_deduction" DECIMAL(5, 2),
	"late_submission_deduction_enabled" BOOLEAN,
	"late_submission_deduction" DECIMAL(5, 2),
	"late_submission_interval" VARCHAR(16),
	"late_submission_minimum_percent_enabled" BOOLEAN,
	"late_submission_minimum_percent" DECIMAL(5, 2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/late_policies/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : learning_outcome_groups"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."learning_outcome_groups" (
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"context_id" BIGINT,
	"context_type" VARCHAR,
	"migration_id" VARCHAR(255),
	"learning_outcome_group_id" BIGINT,
	"root_learning_outcome_group_id" BIGINT,
	"vendor_guid" VARCHAR(255),
	"outcome_import_id" BIGINT,
	"source_outcome_group_id" BIGINT,
	"description" VARCHAR(65535),
	"title" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/learning_outcome_groups/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "learning_outcome_question_results"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."learning_outcome_question_results" (
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"learning_outcome_id" BIGINT,
	"associated_asset_id" BIGINT,
	"associated_asset_type" VARCHAR,
	"learning_outcome_result_id" BIGINT,
	"score" DOUBLE PRECISION,
	"possible" DOUBLE PRECISION,
	"mastery" BOOLEAN,
	"attempt" INTEGER,
	"original_score" DOUBLE PRECISION,
	"original_possible" DOUBLE PRECISION,
	"original_mastery" BOOLEAN,
	"assessed_at" TIMESTAMP,
	"submitted_at" TIMESTAMP,
	"percent" DOUBLE PRECISION,
	"title" VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/learning_outcome_question_results/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "learning_outcome_results"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."learning_outcome_results" (
	"id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"context_id" BIGINT,
	"context_type" VARCHAR,
	"context_code" VARCHAR(255),
	"learning_outcome_id" BIGINT,
	"associated_asset_id" BIGINT,
	"associated_asset_type" VARCHAR,
	"score" DOUBLE PRECISION,
	"possible" DOUBLE PRECISION,
	"mastery" BOOLEAN,
	"attempt" INTEGER,
	"original_score" DOUBLE PRECISION,
	"original_possible" DOUBLE PRECISION,
	"original_mastery" BOOLEAN,
	"assessed_at" TIMESTAMP,
	"submitted_at" TIMESTAMP,
	"association_id" BIGINT,
	"association_type" VARCHAR,
	"content_tag_id" BIGINT,
	"user_uuid" VARCHAR(255),
	"artifact_id" BIGINT,
	"artifact_type" VARCHAR,
	"hide_points" BOOLEAN,
	"hidden" BOOLEAN,
	"percent" DOUBLE PRECISION,
	"title" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/learning_outcome_results/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "learning_outcomes"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."learning_outcomes" (
	"id" BIGINT,
	"display_name" VARCHAR(255),
	"context_code" VARCHAR(255),
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"context_id" BIGINT,
	"context_type" VARCHAR,
	"migration_id" VARCHAR(255),
	"vendor_guid" VARCHAR(255),
	"outcome_import_id" BIGINT,
	"calculation_method" VARCHAR,
	"calculation_int" smallint,
	"short_description" VARCHAR(255),
	"description" VARCHAR(65535),
	"data" VARCHAR(65535)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/learning_outcomes/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "lti_line_items"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."lti_line_items" (
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"assignment_id" BIGINT,
	"client_id" BIGINT,
	"coupled" BOOLEAN,
	"score_maximum" DOUBLE PRECISION,
	"resource_id" VARCHAR,
	"lti_resource_link_id" BIGINT,
	"label" VARCHAR,
	"extensions" VARCHAR,
	"tag" VARCHAR(2000)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/lti_line_items/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "lti_resource_links"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."lti_resource_links" (
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"context_id" BIGINT,
	"context_type" VARCHAR,
	"context_external_tool_id" BIGINT,
	"custom" VARCHAR,
	"resource_link_uuid" VARCHAR,
	"lookup_uuid" VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/lti_resource_links/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "lti_results"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."lti_results" (
	"id" BIGINT,
	"extensions" VARCHAR(65535),
	"comment" VARCHAR(65535),
	"submission_id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"result_score" DOUBLE PRECISION,
	"result_maximum" DOUBLE PRECISION,
	"activity_progress" VARCHAR,
	"grading_progress" VARCHAR,
	"lti_line_item_id" BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/lti_results/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "master_courses_child_content_tags"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."master_courses_child_content_tags" (
	"id" BIGINT,
	"child_subscription_id" BIGINT,
	"content_id" BIGINT,
	"migration_id" VARCHAR,
	"downstream_changes" VARCHAR,
	"content_type" VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/master_courses_child_content_tags/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "master_courses_child_subscriptions"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."master_courses_child_subscriptions" (
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"use_selective_copy" BOOLEAN,
	"master_template_id" BIGINT,
	"child_course_id" BIGINT 
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/master_courses_child_subscriptions/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "master_courses_master_content_tags"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."master_courses_master_content_tags" (
	"id" BIGINT,
	"content_id" BIGINT,
	"migration_id" VARCHAR,
	"restrictions" VARCHAR,
	"use_default_restrictions" BOOLEAN,
	"master_template_id" BIGINT,
	"content_type" VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/master_courses_master_content_tags/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "master_courses_master_migrations"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."master_courses_master_migrations" (
	"id" BIGINT,
	"comment" VARCHAR(65535),
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"migration_settings" VARCHAR(65535),
	"export_results" VARCHAR(65535),
	"exports_started_at" TIMESTAMP,
	"imports_queued_at" TIMESTAMP,
	"imports_completed_at" TIMESTAMP,
	"send_notification" BOOLEAN,
	"master_template_id" BIGINT 
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/master_courses_master_migrations/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "master_courses_master_templates"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."master_courses_master_templates" (
	"id" BIGINT,
	"course_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"full_course" BOOLEAN,
	"active_migration_id" BIGINT,
	"default_restrictions" VARCHAR,
	"use_default_restrictions_by_type" BOOLEAN,
	"default_restrictions_by_type" VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/master_courses_master_templates/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "master_courses_migration_results"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."master_courses_migration_results" (
	"id" BIGINT,
	"state" VARCHAR,
	"child_subscription_id" BIGINT,
	"master_migration_id" BIGINT,
	"content_migration_id" BIGINT,
	"import_type" VARCHAR,
	"results" VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/master_courses_migration_results/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "originality_reports"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."originality_reports" (
	"id" BIGINT,
	"error_message" VARCHAR(65535),
	"attachment_id" BIGINT,
	"submission_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"originality_score" DOUBLE PRECISION,
	"originality_report_url" VARCHAR,
	"originality_report_lti_url" VARCHAR,
	"link_id" VARCHAR,
	"submission_time" TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/originality_reports/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "outcome_proficiencies"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."outcome_proficiencies" (
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"context_id" BIGINT,
	"context_type" VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/outcome_proficiencies/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "outcome_proficiency_ratings"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."outcome_proficiency_ratings" (
	"id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"mastery" BOOLEAN,
	"points" DOUBLE PRECISION,
	"outcome_proficiency_id" BIGINT,
	"color" VARCHAR,
	"description" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/outcome_proficiency_ratings/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "post_policies"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."post_policies" (
	"id" BIGINT,
	"course_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"assignment_id" BIGINT,
	"post_manually" BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/post_policies/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "rubric_assessments"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."rubric_assessments" (
	"id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"rubric_association_id" BIGINT,
	"artifact_id" BIGINT,
	"artifact_type" VARCHAR,
	"hide_points" BOOLEAN,
	"score" DOUBLE PRECISION,
	"rubric_id" BIGINT,
	"assessment_type" VARCHAR,
	"assessor_id" BIGINT,
	"artifact_attempt" INTEGER,
	"data" VARCHAR(65535)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/rubric_assessments/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "rubric_associations"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."rubric_associations" (
	"id" BIGINT,
	"purpose" VARCHAR(255),
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"context_id" BIGINT,
	"context_type" VARCHAR,
	"association_id" BIGINT,
	"association_type" VARCHAR,
	"hide_points" BOOLEAN,
	"rubric_id" BIGINT,
	"use_for_grading" BOOLEAN,
	"summary_data" VARCHAR(65535),
	"hide_score_total" BOOLEAN,
	"bookmarked" BOOLEAN,
	"hide_outcome_results" BOOLEAN,
	"title" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/rubric_associations/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "rubrics"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."rubrics" (
	"id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"points_possible" DOUBLE PRECISION,
	"context_id" BIGINT,
	"context_type" VARCHAR,
	"migration_id" VARCHAR(255),
	"hide_score_total" BOOLEAN,
	"association_count" INTEGER,
	"free_form_criterion_comments" BOOLEAN,
	"title" VARCHAR(255),
	"data" VARCHAR(65535)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/rubrics/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "user_account_associations"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."user_account_associations" (
	"id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"account_id" BIGINT,
	"depth" INTEGER
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/user_account_associations/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "user_notes"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."user_notes" (
	"id" BIGINT,
	"deleted_at" TIMESTAMP,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"workflow_state" VARCHAR,
	"note" VARCHAR(65535),
	"created_by_id" BIGINT,
	"title" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/user_notes/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "web_conference_participants"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."web_conference_participants" (
	"id" BIGINT,
	"web_conference_id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"participation_type" VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/web_conference_participants/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- External Table : "web_conferences"
--------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE "{redshift_schema_canvas_data_2}"."web_conferences" (
	"id" BIGINT,
	"user_id" BIGINT,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"context_id" BIGINT,
	"context_type" VARCHAR,
	"start_at" TIMESTAMP,
	"end_at" TIMESTAMP,
	"context_code" VARCHAR(255),
	"started_at" TIMESTAMP,
	"user_ids" VARCHAR(255),
	"ended_at" TIMESTAMP,
	"recording_ready" BOOLEAN,
	"conference_type" VARCHAR,
	"conference_key" VARCHAR(255),
	"description" text,
	"duration" DOUBLE PRECISION,
	"settings" VARCHAR(65535),
	"title" VARCHAR(255),
	"uuid" VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_canvas_data_2_path_today}/web_conferences/'
TABLE PROPERTIES ('skip.header.line.count'='1');


--------------------------------------------------------------------------------------
-- END OF CANVAS DATA 2 DDL STATEMENTS
--------------------------------------------------------------------------------------