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

/*
 * Store a properly sorted list of millions of assignment submissions in one (we hope) S3 CSV per term.
 */

UNLOAD (
    'SELECT
        ac1.canvas_user_id AS reference_user_id,
        ac1.course_id AS canvas_course_id,
        ac2.canvas_user_id AS canvas_user_id,
        COUNT(
            CASE WHEN ac2.assignment_status IN (\'graded\', \'late\', \'on_time\', \'submitted\')
            THEN 1 ELSE NULL END
        ) AS submissions_turned_in
    FROM {redshift_schema_boac}.assignment_submissions_scores ac1
    JOIN {redshift_schema_boac}.assignment_submissions_scores ac2
        ON ac1.uid IN (SELECT ldap_uid FROM {redshift_schema_calnet}.persons)
        AND ac1.term_name = \'{term_name}\'
        AND ac1.assignment_id = ac2.assignment_id
        AND ac1.course_id = ac2.course_id
    GROUP BY reference_user_id, ac1.course_id, ac2.canvas_user_id
    HAVING count(*) = (
        SELECT count(*) FROM {redshift_schema_boac}.assignment_submissions_scores
        WHERE canvas_user_id = reference_user_id AND course_id = ac1.course_id
    )
    ORDER BY reference_user_id, ac1.course_id, ac2.canvas_user_id'
)
TO '{boac_assignments_path}/{term_id}/sub_'
IAM_ROLE '{redshift_iam_role}'
ENCRYPTED
DELIMITER AS ','
NULL AS ''
ALLOWOVERWRITE
PARALLEL OFF
GZIP;
