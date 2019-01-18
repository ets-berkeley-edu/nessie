/**
 * Copyright ©2018. The Regents of the University of California (Regents). All Rights Reserved.
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

-- Contrary to the documentation, this statement does not actually -- DROP external database tables.
-- When the external schema is re-created, the table definitions will return as they were.


DROP SCHEMA IF EXISTS {redshift_berkeleyx_ext_schema} CASCADE;

CREATE EXTERNAL SCHEMA {redshift_berkeleyx_ext_schema}
FROM data catalog
DATABASE '{redshift_berkeleyx_ext_schema}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- email opt in
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.email_opt_in (
	user_id INT,
	username VARCHAR,
	email VARCHAR,
	full_name VARCHAR,
	course_id VARCHAR,
	is_opted_in_for_email VARCHAR,
	preference_set_datetime TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/email_opt_in'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- auth_userprofile
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.auth_userprofile (
	id INT,
	user_id INT,
	name  VARCHAR,
	language VARCHAR,
	location VARCHAR,
	meta  VARCHAR(MAX),
	courseware VARCHAR,
	gender VARCHAR(6),
	mailing_address VARCHAR(MAX),
	year_of_birth INT,
	level_of_education VARCHAR(6),
	goals VARCHAR(MAX),
	allow_certificate INT,
	country VARCHAR(2),
	city VARCHAR,
	bio VARCHAR(300),
	profile_image_uploaded_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/auth_userprofile'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- auth_user
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.auth_user (
	id INT,
	username VARCHAR(150),
	first_name VARCHAR(30),
	last_name VARCHAR(30),
	email VARCHAR,
	password VARCHAR(128),
	is_staff INT,
	is_active INT,
	is_superuser INT,
	last_login TIMESTAMP,
	date_joined TIMESTAMP,
	status VARCHAR(2),
	email_key VARCHAR(32),
	avatar_typ VARCHAR(1),
	country VARCHAR(2),
	show_country INT,
	date_of_birth VARCHAR,
	interesting_tags VARCHAR(MAX),
	ignored_tags VARCHAR(MAX),
	email_tag_filter_strategy INT,
	display_tag_filter_strategy INT,
	consecutive_days_visit_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/auth_user'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- user_id_map
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.user_id_map (
	hashid INT,
	user_id INT,
	username VARCHAR(30)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/user_id_map'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- student_anonymousid
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.student_anonymoususerid (
	id INT,
	user_id INT,
	anonymous_user_id VARCHAR(32),
	course_id VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/student_anonymoususerid'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- student_courseenrollment
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.student_courseenrollment (
	id INT,
	user_id INT,
	course_id VARCHAR,
	created TIMESTAMP,
	is_active INT,
	mode VARCHAR(100)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/student_courseenrollment'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- student_courseaccessrole
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.student_courseaccessrole (
	user_id INT,
	course_id VARCHAR,
	role VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/student_courseaccessrole'
TABLE PROPERTIES ('skip.header.line.count'='1');


--django_comment_client_role_users
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.django_comment_client_role_users (
	user_id INT,
	course_id VARCHAR,
	name VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/django_comment_client_role_users'
TABLE PROPERTIES ('skip.header.line.count'='1');


--user_api_usercoursetag
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.user_api_usercoursetag (
	user_id INT,
	course_id VARCHAR,
	key VARCHAR,
	value VARCHAR(MAX)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/user_api_usercoursetag'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- student_languageproficiency
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.student_languageproficiency (
	id INT,
	user_profile_id VARCHAR,
	code VARCHAR(16)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/student_languageproficiency'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- teams
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.teams (
	id INT,
	team_id VARCHAR,
	name VARCHAR,
	course_id VARCHAR(MAX),
	topic_id VARCHAR,
	date_created TIMESTAMP,
	description VARCHAR(300),
	country VARCHAR(2),
	language VARCHAR(16),
	discussion_topic_id VARCHAR(255),
	last_activity_at TIMESTAMP,
	team_size INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/teams'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- teams_membership
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.teams_membership (
	id INT,
	user_id INT,
	team_id VARCHAR,
	date_joined TIMESTAMP,
	last_activity_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/teams_membership'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- courseware_studentmodule
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.courseware_studentmodule (
	id INT,
	module_type VARCHAR(32),
	module_id VARCHAR,
	student_id INT,
	state VARCHAR(MAX),
	grade DOUBLE PRECISION,
	created TIMESTAMP,
	modified TIMESTAMP,
	max_grade DOUBLE PRECISION,
	done VARCHAR(8),
	course_id VARCHAR

)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/courseware_studentmodule'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- grades_persistentcoursegrade
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.grades_persistentcoursegrade (
	course_id VARCHAR,
	user_id INT,
	grading_policy_hash VARCHAR,
	percent_grade DOUBLE PRECISION,
	letter_grade VARCHAR,
	passed_timestamp TIMESTAMP,
	created TIMESTAMP,
	modified TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/grades_persistentcoursegrade'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- grades_persistentsubsectiongrade
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.grades_persistentsubsectiongrade (
	course_id VARCHAR,
	user_id INT,
	usage_key VARCHAR,
	earned_all DOUBLE PRECISION,
	possible_all DOUBLE PRECISION,
	earned_graded DOUBLE PRECISION,
	possible_graded DOUBLE PRECISION,
	first_attempted TIMESTAMP,
	created TIMESTAMP,
	modified TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/grades_persistentsubsectiongrade'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- certificates_generatedcertificate
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.certificates_generatedcertificate (
	id INT,
	user_id INT,
	download_url VARCHAR(128),
	grade VARCHAR(5),
	course_id VARCHAR,
	key VARCHAR(32),
	distinction INT,
	status VARCHAR(32),
	verify_uuid VARCHAR(32),
	download_uuid VARCHAR(32),
	name VARCHAR,
	created_date TIMESTAMP,
	modified_date TIMESTAMP,
	error_reason VARCHAR(512),
	mode VARCHAR(32)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/certificates_generatedcertificate'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- credit_crediteligibility
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.credit_crediteligibility (
	id INT,
	created VARCHAR,
	modified TIMESTAMP,
	username TIMESTAMP,
	deadline TIMESTAMP,
	course_key VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/credit_crediteligibility'
TABLE PROPERTIES ('skip.header.line.count'='1');



/*************
* ORA Tables
*************/
-- workflow_assessmentworkflow
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_workflow_assessmentworkflow (
	id INT,
	course_id VARCHAR,
	created TIMESTAMP,
	item_id VARCHAR,
	modified TIMESTAMP,
	status VARCHAR,
	status_changed VARCHAR,
	submission_uuid VARCHAR,
	uuid VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/workflow_assessmentworkflow'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- workflow_assessmentworkflowstep
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_workflow_assessmentworkflowstep (
	id INT,
	workflow VARCHAR,
	assessment_completed_at TIMESTAMP,
	name VARCHAR,
	order_num INT,
	submitter_completed_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/workflow_assessmentworkflowstep'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- Submissions
-- ora_submissions_scoresummary
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_submissions_scoresummary (
	id INT,
	highest INT,
	latest INT,
	student_item INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/submissions_scoresummary'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- ora_submissions_score
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_submissions_score (
	id INT,
	student_item INT,
	submission INT,
	created_at TIMESTAMP,
  	points_earned INT,
	points_possible INT,
	reset VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/submissions_score'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- submissions_submission
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_submissions_submission (
	id INT,
	student_item INT,
	attempt_number INT,
	created_at TIMESTAMP,
	raw_answer VARCHAR(MAX),
	submitted_at TIMESTAMP,
	uuid VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/submissions_submission'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- submission_studentitem
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_submission_studentitem (
	id INT,
	course_id VARCHAR,
	item_id VARCHAR,
	sem_type VARCHAR,
	student_id VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/submissions_studentitem'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- Assessment
-- assessment_assessment
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_assessment_assessment (
	id INT,
	rubric INT,
	feedback VARCHAR(MAX),
	score_type VARCHAR,
	scored_at TIMESTAMP,
	scorer_id VARCHAR,
	submission_uuid VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/assessment_assessment'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- assessment_assessmentfeedback
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_assessment_assessmentfeedback (
	id INT,
	feedback_text VARCHAR(MAX),
	submission_uuid VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/assessment_assessmentfeedback'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- assessment_assessmentfeedbackoption
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_assessment_assessmentfeedbackoption (
	id INT,
	text VARCHAR(MAX)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/assessment_assessmentfeedbackoption'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- assessment_assessmentfeedback_assessments
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_assessment_assessmentfeedback_assessments (
	id INT,
	assessmentfeedback_id INT,
	assessment_id INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/assessment_assessmentfeedback_assesments'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- assessment_assessmentfeedback_options
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_assessment_assessmentfeedback_options (
	id INT,
	assessmentfeedback_id INT,
	assessmentfeedbackoption_id INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/ora_assessment_assessmentfeedback_options'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- assessment_assessmentpart
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_assessment_assessmentpart (
	id INT,
	assessment INT,
	criterion INT,
	option INT,
	feedback VARCHAR(MAX)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/assessment_assessmentpart'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- assessment_criterion
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_assessment_criterion (
	id INT,
	rubric INT,
	label VARCHAR,
	name VARCHAR,
	order_num INT,
	prompt VARCHAR(MAX)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/assessment_criterion'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- assessment_criterionoption
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_assessment_criterionoption (
	id INT,
	criterion INT,
	explanation VARCHAR(MAX),
	label VARCHAR,
	name VARCHAR,
	order_num INT,
	points INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/assessment_criterionoption'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- assessment_peerworkflow
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_assessment_peerworkflow (
	id INT,
	completed_at TIMESTAMP,
	course_id VARCHAR,
	created_at TIMESTAMP,
	grading_completed_at TIMESTAMP,
	item_id VARCHAR,
	student_id VARCHAR,
	submission_uuid VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/assessment_peerworkflow'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- assessment_peerworkflowitem
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_assessment_peerworkflowitem (
	id INT,
	assessment INT,
	author INT,
	scorer INT,
	scored VARCHAR,
	started_at TIMESTAMP,
	submission_uuid VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/assessment_peerworkflowitem'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- assessment_rubric
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_assessment_rubric (
	id INT,
	content_hash VARCHAR,
	structure_hash VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/assessment_rubric'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- assessment_studenttrainingworkflow
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_assessment_studenttrainingworkflow (
	id INT,
	course_id VARCHAR,
	item_id VARCHAR,
	student_id VARCHAR,
	submission_uuid VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/assessment_studenttrainingworkflow'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- assessment_studenttrainingworkflowitem
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_assessment_studenttrainingworkflowitem (
	id INT,
	training_example INT,
	workflow INT,
	completed_at TIMESTAMP,
	order_num INT,
	started_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/assessment_studenttrainingworkflowitem'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- assessment_trainingexample
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_assessment_trainingexample (
	id INT,
	rubric INT,
	content_hash VARCHAR,
	raw_answer VARCHAR(MAX)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/assessment_trainingexample'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- assessment_trainingexample_options_selected
CREATE EXTERNAL TABLE {redshift_berkeleyx_ext_schema}.ora_assessment_trainingexample_options_selected (
	id INT,
	trainingexample_id INT,
	criterionoption_id INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_berkeleyx_data_path}/ora/assessment_trainingexample_options_selected'
TABLE PROPERTIES ('skip.header.line.count'='1');


-- daily transaction logs
CREATE EXTERNAL TABLE  {redshift_berkeleyx_ext_schema}.transactional_logs (
	username VARCHAR(MAX),
	host VARCHAR(MAX),
	event_source VARCHAR(MAX),
	event_type VARCHAR(MAX),
	time VARCHAR(MAX),
	ip VARCHAR(MAX),
	event VARCHAR(MAX),
	agent VARCHAR(MAX),
	page VARCHAR(MAX),
	session VARCHAR(MAX),
	context STRUCT <
		course_id: VARCHAR(MAX),
		user_id: VARCHAR(MAX),
		org_id: VARCHAR(MAX),
		username: VARCHAR(MAX),
		ip: VARCHAR(MAX),
		agent: VARCHAR(MAX),
		host: VARCHAR(MAX),
		session: VARCHAR(MAX),
		path: VARCHAR(MAX),
		module: STRUCT <
			display_name: VARCHAR(MAX),
			usage_key: VARCHAR(MAX)
		>,
		course_user_tags: VARCHAR(MAX),
		component: VARCHAR(MAX),
		received_at: VARCHAR(MAX),
		application: STRUCT <
			version: VARCHAR(MAX),
			name: VARCHAR(MAX)
		>,
		client: STRUCT <
			network: STRUCT <
				wifi: BOOLEAN,
				carrier: VARCHAR(MAX),
				cellular: BOOLEAN,
				bluetooth: BOOLEAN
			>,
			locale: VARCHAR(MAX),
			app: STRUCT <
				name: VARCHAR(MAX),
				packageName: VARCHAR(MAX),
				version: VARCHAR(MAX),
				build: VARCHAR(MAX),
				versionName: VARCHAR(MAX),
				versionCode: INT,
				namespace: VARCHAR(MAX)
			>,
			library: STRUCT <
			version: VARCHAR(MAX),
			name: VARCHAR(MAX),
			versionName: VARCHAR(MAX)
			>,
			timezone: VARCHAR(MAX),
			device: STRUCT <
				name: VARCHAR(MAX),
				advertisingId: VARCHAR(MAX),
				model: VARCHAR(MAX),
				type: VARCHAR(MAX),
				id: VARCHAR(MAX),
				adTrackingEnabled: BOOLEAN,
				manufacturer: VARCHAR(MAX)
			>,
			os: STRUCT <
				version: VARCHAR(MAX),
				name: VARCHAR(MAX),
				sdk: INT
			>,
			screen: STRUCT <
				densityBucket: VARCHAR(MAX),
				density: DOUBLE PRECISION,
				height: INT,
				weight: INT,
				densityDpi: INT,
				scaledDensity: DOUBLE PRECISION
	        >,
	        ip: VARCHAR(MAX)
		>,
		open_in_browser_url: VARCHAR(MAX),
		asides: VARCHAR(MAX),
		label: VARCHAR(MAX)
	>,
	name VARCHAR(MAX),
	referer VARCHAR(MAX),
	accept_language VARCHAR(MAX),
	label VARCHAR(MAX),
	noninteraction INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '{loch_s3_berkeleyx_transaction_log_path}/events';
