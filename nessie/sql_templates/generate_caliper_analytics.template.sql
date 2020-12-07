/**
 * Copyright ©2021. The Regents of the University of California (Regents). All Rights Reserved.
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


INSERT INTO {redshift_schema_caliper_analytics}.canvas_caliper_user_requests (
    SELECT
        split_part("id", ':', 3) AS uuid,
        "action",
        "type",
        "eventtime",
        "timestamp"::timestamp,
        split_part("actor.id", ':', 5) AS "actor.id",
        "actor.type",
        "actor.extensions.com.instructure.canvas.user_login",
        "actor.extensions.com.instructure.canvas.entity_id",
        "group.extensions.com.instructure.canvas.context_type",
        split_part("group.id", ':', 5) AS "group.id",
        "group.type",
        "membership.id",
        split_part("membership.member.id", ':', 5) AS "membership.member.id",
        "membership.member.type",
        split_part("membership.organization.id", ':', 5) AS "membership.organization.id",
        "membership.organization.type",
        "membership.type",
        split_part("object.id", ':', 5) AS "object.id",
        "object.name",
        "object.type",
        "object.extensions.com.instructure.canvas.asset_type",
        "object.extensions.com.instructure.canvas.asset_subtype",
        "object.extensions.com.instructure.canvas.context_id",
        "object.extensions.com.instructure.canvas.context_type",
        "object.extensions.com.instructure.canvas.entity_id",
        "object.extensions.com.instructure.canvas.filename",
        "object.extensions.com.instructure.canvas.grade",
        "object.extensions.com.instructure.canvas.submission_type",
        "object.extensions.com.instructure.canvas.workflow_state",
        split_part("session.id", ':', 5) AS "session.id",
        "session.type",
        "extensions.com.instructure.canvas.request_id",
        "extensions.com.instructure.canvas.user_agent",
        "extensions.com.instructure.canvas.version"

    FROM {redshift_schema_lrs_external}.canvas_caliper_explode
    WHERE "actor.type"='Person'
	    AND ("group.type"='CourseOffering'
	    OR "membership.organization.type"='CourseOffering'
	    OR "object.name" = 'home')
);
