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


import json
import tempfile

from flask import current_app as app
from nessie.externals import s3
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.util import get_s3_canvas_api_path


"""Logic for transforming Canvas API data into Spark-parsable JSON."""


class TransformCanvasApiData(BackgroundJob):

    def run(self, datestamp=None):
        app.logger.info('Starting Canvas API data transform job...')

        s3_source = get_s3_canvas_api_path()
        s3_dest = get_s3_canvas_api_path(transformed=True)

        self.transform(f'{s3_source}/gradebook_history/gradebook_history', f'{s3_dest}/gradebook_history')
        self.transform(f'{s3_source}/grade_change_log/grade_change_log', f'{s3_dest}/grade_change_log', key='events')

        return 'Canvas API data transform complete.'

    def transform(self, s3_source, s3_dest, key=None):
        objects = s3.get_keys_with_prefix(s3_source)
        app.logger.info(f'Will transform {len(objects)} objects from {s3_source} and put results to {s3_dest}.')
        skip_count = 0
        for o in objects:
            file_name = o.split('/')[-1]
            if s3.object_exists(f'{s3_dest}/{file_name}'):
                skip_count += 1
                continue
            canvas_api_data = s3.get_object_json(o).get(key) if key else s3.get_object_json(o)
            with tempfile.TemporaryFile() as result:
                course_id = int(file_name.split('_')[-2])
                for record in canvas_api_data:
                    record['course_id'] = course_id
                    result.write(json.dumps(record).encode() + b'\n')
                s3.upload_file(result, f'{s3_dest}/{file_name}')
        app.logger.info(f'Transformed {len(objects) - skip_count} new objects; skipped {skip_count} existing objects.')
