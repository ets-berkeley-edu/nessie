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

--------------------------------------------------------------------
-- CREATE EXTERNAL SCHEMA
--------------------------------------------------------------------

CREATE EXTERNAL SCHEMA {redshift_schema_canvas_api}
FROM data catalog
DATABASE '{redshift_schema_canvas_api}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

CREATE EXTERNAL TABLE {redshift_schema_canvas_api}.gradebook_history (
    id BIGINT,
    course_id BIGINT,
    assignment_id BIGINT,
    assignment_name VARCHAR,
    body TEXT,
    current_grade INT,
    current_graded_at TIMESTAMP,
    current_grader VARCHAR,
    grade_matches_current_submission BOOLEAN,
    graded_at TIMESTAMP,
    grader VARCHAR,
    grader_id BIGINT,
    new_grade VARCHAR,
    new_graded_at TIMESTAMP,
    new_grader VARCHAR,
    previous_grade VARCHAR,
    previous_graded_at TIMESTAMP,
    previous_grader VARCHAR,
    score DOUBLE PRECISION,
    user_name VARCHAR,
    submission_type VARCHAR,
    url VARCHAR,
    user_id BIGINT,
    workflow_state VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '{loch_s3_canvas_api_data_path}/gradebook_history';

CREATE EXTERNAL TABLE {redshift_schema_canvas_api}.grade_change_log (
    id VARCHAR,
    course_id BIGINT,
    created_at TIMESTAMP,
    event_type VARCHAR,
    excused_after BOOLEAN,
    excused_before BOOLEAN,
    grade_after VARCHAR,
    grade_before VARCHAR,
    graded_anonymously BOOLEAN,
    version_number VARCHAR,
    request_id VARCHAR,
    links ARRAY <
        STRUCT <
            assignment:BIGINT,
            course:BIGINT,
            student:VARCHAR,
            grader:VARCHAR,
            page_view:VARCHAR
        >
    >
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '{loch_s3_canvas_api_data_path}/grade_change_log';
