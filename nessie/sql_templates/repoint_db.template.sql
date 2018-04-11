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

-- Repoints External Tables to new S3 location

-- Users
ALTER TABLE {externalSchema}.user_dim
      SET LOCATION '{s3DailyLocation}/user_dim';

-- Pseudonym
ALTER TABLE {externalSchema}.pseudonym_dim
      SET LOCATION '{s3DailyLocation}/pseudonym_dim';

-- Courses
ALTER TABLE {externalSchema}.course_dim
      SET LOCATION '{s3DailyLocation}/course_dim';

-- Course Section Dimensions
ALTER TABLE {externalSchema}.course_section_dim
      SET LOCATION '{s3DailyLocation}/course_section_dim';

-- enrollment_fact
ALTER TABLE {externalSchema}.enrollment_fact
      SET LOCATION '{s3DailyLocation}/enrollment_fact';

-- enrollment_dim
ALTER TABLE {externalSchema}.enrollment_dim
      SET LOCATION '{s3DailyLocation}/enrollment_dim';

-- Assignments Fact
ALTER TABLE {externalSchema}.assignment_fact
            SET LOCATION '{s3DailyLocation}/assignment_fact';

-- Assignment Dimension table
ALTER TABLE {externalSchema}.assignment_dim
      SET LOCATION '{s3DailyLocation}/assignment_dim';

-- Assignment override fact
ALTER TABLE {externalSchema}.assignment_override_fact
  SET LOCATION '{s3DailyLocation}/assignment_override_fact';

-- Assignment override dim
ALTER TABLE {externalSchema}.assignment_override_dim
  SET LOCATION '{s3DailyLocation}/assignment_override_dim';

-- Assignment override user fact
ALTER TABLE {externalSchema}.assignment_override_user_fact
  SET LOCATION '{s3DailyLocation}/assignment_override_user_fact';

-- Assignment override user dim
ALTER TABLE {externalSchema}.assignment_override_user_dim
  SET LOCATION '{s3DailyLocation}/assignment_override_user_dim';

-- Assignment override user rollup fact
ALTER TABLE {externalSchema}.assignment_override_user_rollup_fact
  SET LOCATION '{s3DailyLocation}/assignment_override_user_rollup_fact';

-- Discussion Entry dimension
ALTER TABLE      {externalSchema}.discussion_entry_dim
      SET LOCATION '{s3DailyLocation}/discussion_entry_dim';

-- Discussion entry fact
ALTER TABLE {externalSchema}.discussion_entry_fact
      SET LOCATION '{s3DailyLocation}/discussion_entry_fact';

-- Discussion topic dim
ALTER TABLE {externalSchema}.discussion_topic_dim
      SET LOCATION '{s3DailyLocation}/discussion_topic_dim';

-- Discussion topic fact
ALTER TABLE {externalSchema}.discussion_topic_fact
      SET LOCATION '{s3DailyLocation}/discussion_topic_fact';

-- Submission_fact
ALTER TABLE {externalSchema}.submission_fact
      SET LOCATION '{s3DailyLocation}/submission_fact';

-- Submission_dim
ALTER TABLE {externalSchema}.submission_dim
      SET LOCATION '{s3DailyLocation}/submission_dim';

-- Submissions comments fact
ALTER TABLE {externalSchema}.submission_comment_fact
      SET LOCATION '{s3DailyLocation}/submission_comment_fact';

-- Submission_comment_participant_fact
ALTER TABLE {externalSchema}.submission_comment_participant_fact
      SET LOCATION '{s3DailyLocation}/submission_comment_participant_fact';

-- Submission_comment_dim
ALTER TABLE {externalSchema}.submission_comment_dim
      SET LOCATION '{s3DailyLocation}/submission_comment_dim';

-- Requests
ALTER TABLE {externalSchema}.requests
      SET LOCATION '{s3RequestsTermLocation}/requests';

-- Historical Requests in raw gzip format
ALTER TABLE {externalSchema}.historical_requests
      SET LOCATION '{s3RequestsHistoricalLocation}/requests';

-- Historical Requests compressed to parquet
ALTER TABLE {externalSchema}.historical_requests_parquet
      SET LOCATION '{s3RequestsHistoricalLocation}/requests-parquet-snappy';
