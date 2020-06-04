"""
Copyright ©2020. The Regents of the University of California (Regents). All Rights Reserved.

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

import time

from flask import current_app as app
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.berkeley import current_term_id
from nessie.lib.dispatcher import dispatch
from nessie.lib.metadata import create_canvas_api_import_status, update_canvas_api_import_status
from nessie.lib.queries import get_enrolled_canvas_sites_for_term
from nessie.lib.util import get_s3_canvas_api_daily_path

"""Canvas grade change log import job."""


class ImportCanvasGradeChangeLog(BackgroundJob):

    @classmethod
    def generate_job_id(cls):
        return 'ImportCanvasGradeChangeLog_' + str(int(time.time()))

    def run(self, term_id=None):
        job_id = self.generate_job_id()
        if not term_id:
            term_id = current_term_id()
        if app.config['TEST_CANVAS_COURSE_IDS']:
            canvas_course_ids = app.config['TEST_CANVAS_COURSE_IDS']
        else:
            canvas_course_ids = [row['canvas_course_id'] for row in get_enrolled_canvas_sites_for_term(term_id)]
        app.logger.info(f'Starting Canvas grade change log import job {job_id} for term {term_id}, {len(canvas_course_ids)} course sites...')

        success_count = 0
        failure_count = 0
        index = 1
        for course_id in canvas_course_ids:
            path = f'/api/v1/audit/grade_change/courses/{course_id}'
            s3_key = f'{get_s3_canvas_api_daily_path()}/grade_change_log/grade_change_log_{course_id}.json'
            create_canvas_api_import_status(
                job_id=job_id,
                term_id=term_id,
                course_id=course_id,
                table_name='grade_change_log',
            )
            app.logger.info(
                f'Fetching Canvas grade change log for course id {course_id}, term {term_id} ({index} of {len(canvas_course_ids)})',
            )
            response = dispatch(
                'import_canvas_api_data',
                data={
                    'course_id': course_id,
                    'path': path,
                    's3_key': s3_key,
                    'key': 'events',
                    'canvas_api_import_job_id': job_id,
                },
            )
            if not response:
                app.logger.error(f'Canvas grade change log import failed for course id {course_id}.')
                update_canvas_api_import_status(
                    job_id=job_id,
                    course_id=course_id,
                    status='error',
                )
                failure_count += 1
            else:
                success_count += 1
            index += 1

        return (
            f'Canvas grade change log import completed for term {term_id}: {success_count} succeeded, '
            f'{failure_count} failed.'
        )
