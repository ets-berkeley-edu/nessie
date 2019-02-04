/**
 * Copyright ©2019. The Regents of the University of California (Regents). All Rights Reserved.
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
-- CREATE EXTERNAL SCHEMA
--------------------------------------------------------------------

CREATE EXTERNAL SCHEMA IF NOT EXISTS {redshift_schema_lrs_external}
FROM data catalog
DATABASE '{redshift_schema_lrs_external}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;


--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

/**
 * Fixed Caliper explode template with flexible data types. We unload data in JSON format to S3 so that it can accommodate
 * changing schemas if more columns are added, and so that it can be accessed via Redshift Spectrum rather than being stored
 * on a cluster.
 */

DROP TABLE IF EXISTS {redshift_schema_lrs_external}.{canvas_caliper_explode_table};

CREATE EXTERNAL TABLE {redshift_schema_lrs_external}.{canvas_caliper_explode_table} (
    "@context" VARCHAR,
    action VARCHAR,
    "actor.extensions.com.instructure.canvas.entity_id" VARCHAR,
    "actor.extensions.com.instructure.canvas.real_user_id" VARCHAR,
    "actor.extensions.com.instructure.canvas.root_account_id" VARCHAR,
    "actor.extensions.com.instructure.canvas.root_account_lti_guid" VARCHAR,
    "actor.extensions.com.instructure.canvas.root_account_uuid" VARCHAR,
    "actor.extensions.com.instructure.canvas.user_login" VARCHAR,
    "actor.id" VARCHAR,
    "actor.type" VARCHAR,
    "edapp.id" VARCHAR,
    "edapp.type" VARCHAR,
    eventtime VARCHAR,
    "extensions.com.instructure.canvas.hostname" VARCHAR,
    "extensions.com.instructure.canvas.job_id" VARCHAR,
    "extensions.com.instructure.canvas.job_tag" VARCHAR,
    "extensions.com.instructure.canvas.request_id" VARCHAR,
    "extensions.com.instructure.canvas.user_agent" VARCHAR,
    "extensions.com.instructure.canvas.version" VARCHAR,
    "generated.attempt.assignable.id" VARCHAR,
    "generated.attempt.assignable.type" VARCHAR,
    "generated.attempt.assignee.id" VARCHAR,
    "generated.attempt.assignee.type" VARCHAR,
    "generated.attempt.extensions.com.instructure.canvas.grade" VARCHAR,
    "generated.attempt.id" VARCHAR,
    "generated.attempt.type" VARCHAR,
    "generated.extensions.com.instructure.canvas.entity_id" VARCHAR,
    "generated.extensions.com.instructure.canvas.grade" VARCHAR,
    "generated.id" VARCHAR,
    "generated.maxscore" VARCHAR,
    "generated.scoregiven" VARCHAR,
    "generated.scoredby" VARCHAR,
    "generated.type" VARCHAR,
    "group.extensions.com.instructure.canvas.context_type" VARCHAR,
    "group.extensions.com.instructure.canvas.entity_id" VARCHAR,
    "group.id" VARCHAR,
    "group.type" VARCHAR,
    id VARCHAR,
    "membership.id" VARCHAR,
    "membership.member.id" VARCHAR,
    "membership.member.type" VARCHAR,
    "membership.organization.id" VARCHAR,
    "membership.organization.suborganizationof.id" VARCHAR,
    "membership.organization.suborganizationof.type" VARCHAR,
    "membership.organization.type" VARCHAR,
    "membership.roles" VARCHAR,
    "membership.type" VARCHAR,
    "object.assignable.id" VARCHAR,
    "object.assignable.type" VARCHAR,
    "object.assignee.id" VARCHAR,
    "object.assignee.type" VARCHAR,
    "object.body" VARCHAR(MAX),
    "object.count" BIGINT,
    "object.creators" VARCHAR,
    "object.datecreated" VARCHAR,
    "object.datemodified" VARCHAR,
    "object.datetoshow" VARCHAR,
    "object.datetosubmit" VARCHAR,
    "object.description" VARCHAR,
    "object.extensions.com.instructure.canvas.access_is_current" BOOLEAN,
    "object.extensions.com.instructure.canvas.asset_subtype" VARCHAR,
    "object.extensions.com.instructure.canvas.asset_type" VARCHAR,
    "object.extensions.com.instructure.canvas.body" VARCHAR(MAX),
    "object.extensions.com.instructure.canvas.context_id" VARCHAR,
    "object.extensions.com.instructure.canvas.context_type" VARCHAR,
    "object.extensions.com.instructure.canvas.course_id" VARCHAR,
    "object.extensions.com.instructure.canvas.course_section_id" VARCHAR,
    "object.extensions.com.instructure.canvas.entity_id" VARCHAR,
    "object.extensions.com.instructure.canvas.filename" VARCHAR,
    "object.extensions.com.instructure.canvas.folder_id" VARCHAR,
    "object.extensions.com.instructure.canvas.grade" VARCHAR,
    "object.extensions.com.instructure.canvas.is_admin" BOOLEAN,
    "object.extensions.com.instructure.canvas.is_announcement" BOOLEAN,
    "object.extensions.com.instructure.canvas.limit_privileges_to_course_section" BOOLEAN,
    "object.extensions.com.instructure.canvas.lock_at" VARCHAR,
    "object.extensions.com.instructure.canvas.redirect_url" VARCHAR,
    "object.extensions.com.instructure.canvas.restricted_access" BOOLEAN,
    "object.extensions.com.instructure.canvas.state" VARCHAR,
    "object.extensions.com.instructure.canvas.state_is_current" BOOLEAN,
    "object.extensions.com.instructure.canvas.state_valid_until" VARCHAR,
    "object.extensions.com.instructure.canvas.submission_type" VARCHAR,
    "object.extensions.com.instructure.canvas.type" VARCHAR,
    "object.extensions.com.instructure.canvas.url" VARCHAR,
    "object.extensions.com.instructure.canvas.user_id" VARCHAR,
    "object.extensions.com.instructure.canvas.user_name" VARCHAR,
    "object.extensions.com.instructure.canvas.workflow_state" VARCHAR,
    "object.id" VARCHAR,
    "object.ispartof.id" VARCHAR,
    "object.ispartof.name" VARCHAR,
    "object.ispartof.type" VARCHAR,
    "object.maxscore" VARCHAR,
    "object.mediatype" VARCHAR,
    "object.member.id" VARCHAR,
    "object.member.type" VARCHAR,
    "object.name" VARCHAR,
    "object.organization.extensions.com.instructure.canvas.entity_id" VARCHAR,
    "object.organization.id" VARCHAR,
    "object.organization.ispartof.id" VARCHAR,
    "object.organization.ispartof.name" VARCHAR,
    "object.organization.ispartof.type" VARCHAR,
    "object.organization.name" VARCHAR,
    "object.organization.type" VARCHAR,
    "object.roles" VARCHAR,
    "object.startedattime" VARCHAR,
    "object.type" VARCHAR,
    "session.id" VARCHAR,
    "session.type" VARCHAR,
    "timestamp" VARCHAR,
    type VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '{loch_s3_caliper_explode_url}';
