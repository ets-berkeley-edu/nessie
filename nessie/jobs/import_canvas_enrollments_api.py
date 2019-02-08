"""
Copyright ©2019. The Regents of the University of California (Regents). All Rights Reserved.

Permission to use, copy, modify, and distribute this software and its documentation
for educational, research, and not-for-profit purposes, without fee and without a
signed licensing agreement, is hereby granted, provided that the above copyright
notice, this paragraph and the following two paragraphs appear in all copies,
modifications, and distributions.

Contact The Office of Technology Licensing, UC Berkeley, 2150 Shattuck Avenue,
Suite 510, Berkeley, CA 94720-1620, (510) 643-7201, otl@berkeley.edu,
http://ipira.berkeley.edu/industry-info for commercial licensing opportunities.

IN NO EVENT SHALL REGENTS BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF REGENTS HAS BEEN ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.

REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED
"AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
ENHANCEMENTS, OR MODIFICATIONS.
"""

import json

from flask import current_app as app
from nessie.externals import canvas_api, redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.berkeley import current_term_id
from nessie.lib.queries import get_enrolled_canvas_sites_for_term
from nessie.lib.util import encoded_tsv_row, get_s3_sis_api_daily_path, resolve_sql_template_string

"""Logic for Canvas enrollments API import job."""


class ImportCanvasEnrollmentsApi(BackgroundJob):

    def run(self, term_id=None):
        if not term_id:
            term_id = current_term_id()
        canvas_course_ids = [row['canvas_course_id'] for row in get_enrolled_canvas_sites_for_term(term_id)]

        app.logger.info(f'Starting Canvas enrollments API import job for term {term_id}, {len(canvas_course_ids)} course sites...')

        rows = []
        success_count = 0
        failure_count = 0
        index = 1
        for course_id in canvas_course_ids:
            app.logger.info(f'Fetching Canvas enrollments API for course id {course_id}, term {term_id} ({index} of {len(canvas_course_ids)})')
            feed = canvas_api.get_course_enrollments(course_id)
            if feed:
                success_count += 1
                for enrollment in feed:
                    user_id = enrollment.get('user_id')
                    last_activity_at = enrollment.get('last_activity_at') or ''
                    rows.append(encoded_tsv_row([course_id, user_id, term_id, last_activity_at, json.dumps(enrollment)]))
            else:
                failure_count += 1
                app.logger.error(f'Canvas enrollments API import failed for course id {course_id}.')
            index += 1

        s3_key = f'{get_s3_sis_api_daily_path()}/canvas_api_enrollments_{term_id}.tsv'
        app.logger.info(f'Will stash {success_count} feeds in S3: {s3_key}')
        if not s3.upload_tsv_rows(rows, s3_key):
            raise BackgroundJobError('Error on S3 upload: aborting job.')

        app.logger.info('Will copy S3 feeds into Redshift...')
        query = resolve_sql_template_string(
            """
            DELETE FROM {redshift_schema_student}_staging.canvas_api_enrollments WHERE term_id = '{term_id}';
            COPY {redshift_schema_student}_staging.canvas_api_enrollments
                FROM '{loch_s3_sis_api_data_path}/canvas_api_enrollments_{term_id}.tsv'
                IAM_ROLE '{redshift_iam_role}'
                DELIMITER '\\t'
                TIMEFORMAT 'YYYY-MM-DDTHH:MI:SSZ';
            DELETE FROM {redshift_schema_student}.canvas_api_enrollments
                WHERE term_id = '{term_id}'
                AND course_id IN
                (SELECT course_id FROM {redshift_schema_student}_staging.canvas_api_enrollments WHERE term_id = '{term_id}');
            INSERT INTO {redshift_schema_student}.canvas_api_enrollments
                (SELECT * FROM {redshift_schema_student}_staging.canvas_api_enrollments WHERE term_id = '{term_id}');
            DELETE FROM {redshift_schema_student}_staging.canvas_api_enrollments
                WHERE term_id = '{term_id}';
            """,
            term_id=term_id,
        )
        if not redshift.execute(query):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')

        return (
            f'Canvas enrollments API import completed for term {term_id}: {success_count} succeeded, '
            f'{failure_count} failed.'
        )
