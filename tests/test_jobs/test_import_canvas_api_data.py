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

from nessie.externals import s3
from nessie.jobs.import_canvas_api_data import ImportCanvasApiData
from tests.util import mock_s3


class TestImportCanvasApiData:

    def test_canvas_sync_metadata(self, app, metadata_db):
        """Makes an API call and puts the result in S3."""
        with mock_s3(app):
            bucket = app.config['LOCH_S3_BUCKET']
            path = '/api/v1/audit/grade_change/courses/7654321'
            s3_key = f'{bucket}/grade_change_log/grade_change_log_7654321'
            result = ImportCanvasApiData().run_wrapped(
                course_id='7654321',
                path=path,
                s3_key=s3_key,
                job_id='ImportCanvasGradeChangeLog_123',
            )
            assert result is True
            assert s3.object_exists(f'{s3_key}_0.json') is True
