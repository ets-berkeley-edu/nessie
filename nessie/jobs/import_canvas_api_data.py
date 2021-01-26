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

import json
import tempfile

from botocore.exceptions import ClientError, ConnectionError
from flask import current_app as app
from nessie.externals import canvas_api, s3
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.metadata import update_canvas_api_import_status
from nessie.lib.mockingbird import fixture

"""Canvas API import."""


class ImportCanvasApiData(BackgroundJob):

    def run(self, course_id, path, s3_key, mock=None, key=None, canvas_api_import_job_id=None):
        if canvas_api_import_job_id:
            update_canvas_api_import_status(
                job_id=canvas_api_import_job_id,
                course_id=course_id,
                status='started',
            )
        with tempfile.TemporaryFile() as feed_file:
            if self._fetch_canvas_api_data(path, feed_file, course_id, mock=mock, key=key):
                app.logger.info(f'Will stash feed in S3: {s3_key}')
                try:
                    response = s3.upload_file(feed_file, s3_key)
                    if response and canvas_api_import_job_id:
                        update_canvas_api_import_status(
                            job_id=canvas_api_import_job_id,
                            course_id=course_id,
                            status='complete',
                        )
                    return True
                except (ClientError, ConnectionError, ValueError) as e:
                    if canvas_api_import_job_id:
                        update_canvas_api_import_status(
                            job_id=canvas_api_import_job_id,
                            course_id=course_id,
                            status='error',
                            details=str(e),
                        )
                    app.logger.error(e)
                    return False
            if canvas_api_import_job_id:
                update_canvas_api_import_status(
                    job_id=canvas_api_import_job_id,
                    course_id=course_id,
                    status='no_data',
                )
            return True

    @fixture('canvas_course_grade_change_log_7654321.json')
    def _fetch_canvas_api_data(self, path, feed_file, course_id, mock=None, key=None):
        response = canvas_api.paged_request(
            path=path,
            mock=mock,
            key=key,
        )
        for page in response:
            for record in page:
                record['course_id'] = course_id
                feed_file.write(json.dumps(record).encode() + b'\n')
        return response
