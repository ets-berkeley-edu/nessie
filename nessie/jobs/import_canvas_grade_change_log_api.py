"""
Copyright ©2021. The Regents of the University of California (Regents). All Rights Reserved.

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

from flask import current_app as app
from nessie.externals import canvas_api, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.berkeley import current_term_id
from nessie.lib.queries import get_enrolled_canvas_sites_for_term
from nessie.lib.util import get_s3_canvas_api_daily_path

"""Logic for Canvas grade change log API import job."""


class ImportCanvasGradeChangeLogApi(BackgroundJob):

    def run(self, term_id=None):
        if not term_id:
            term_id = current_term_id()
        canvas_course_ids = [row['canvas_course_id'] for row in get_enrolled_canvas_sites_for_term(term_id)]
        app.logger.info(f'Starting Canvas grade change log API import job for term {term_id}, {len(canvas_course_ids)} course sites...')

        rows = []
        success_count = 0
        failure_count = 0
        index = 1
        for course_id in canvas_course_ids:
            app.logger.info(
                f'Fetching Canvas grade change log history for course id {course_id}, term {term_id} ({index} of {len(canvas_course_ids)})',
            )
            feed = canvas_api.get_course_grade_change_log(course_id)
            if feed:
                success_count += 1
                for event in feed:
                    rows.append({
                        'course_id': course_id,
                        **event,
                    })
            else:
                failure_count += 1
                app.logger.error(f'Canvas grade change log history import failed for course id {course_id}.')
            index += 1

        s3_key = f'{get_s3_canvas_api_daily_path()}/grade_change_log/grade_change_log.json'
        app.logger.info(f'Will stash {success_count} feeds in S3: {s3_key}')
        if not s3.upload_json(rows, s3_key):
            raise BackgroundJobError('Error on S3 upload: aborting job.')
        return (
            f'Canvas grade change log API import completed for term {term_id}: {success_count} succeeded, '
            f'{failure_count} failed.'
        )
