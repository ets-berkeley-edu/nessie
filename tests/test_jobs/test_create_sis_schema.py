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

from datetime import datetime, timedelta
import json

from nessie.externals import s3
from nessie.jobs.create_sis_schema import CreateSisSchema
from nessie.lib.util import get_s3_sis_daily_path
from tests.util import mock_s3


class TestCreateSisSchema:

    def test_update_manifests(self, app):
        """Updates manifests in S3."""
        with mock_s3(app):
            daily_path = get_s3_sis_daily_path()
            historical_path = app.config['LOCH_S3_SIS_DATA_PATH'] + '/historical'
            manifest_path = app.config['LOCH_S3_SIS_DATA_PATH'] + '/manifests'

            s3.upload_data('some new course data', f'{daily_path}/courses/courses-aaa.gz')
            s3.upload_data('some more new course data', f'{daily_path}/courses/courses-bbb.gz')
            s3.upload_data('some new enrollment data', f'{daily_path}/enrollments/enrollments-ccc.gz')
            s3.upload_data('some old course data', f'{historical_path}/courses/courses-ddd.gz')
            s3.upload_data('some old enrollment data', f'{historical_path}/enrollments/enrollments-eee.gz')
            s3.upload_data('some perfectly antique enrollment data', f'{historical_path}/enrollments/enrollments-fff.gz')

            assert CreateSisSchema().update_manifests()

            courses_manifest = json.loads(s3.get_object_text(manifest_path + '/courses.json'))
            assert len(courses_manifest['entries']) == 3
            assert courses_manifest['entries'][0]['url'] == f's3://{app.config["LOCH_S3_BUCKET"]}/{daily_path}/courses/courses-aaa.gz'
            assert courses_manifest['entries'][0]['meta']['content_length'] == 20

            enrollments_manifest = json.loads(s3.get_object_text(manifest_path + '/enrollments.json'))
            assert len(enrollments_manifest['entries']) == 3
            assert (enrollments_manifest['entries'][2]['url']
                    == f's3://{app.config["LOCH_S3_BUCKET"]}/{historical_path}/enrollments/enrollments-fff.gz')
            assert enrollments_manifest['entries'][2]['meta']['content_length'] == 38

    def test_fallback_update_manifests(self, app):
        """Uses yesterday's news if today's is unavailable."""
        with mock_s3(app):
            yesterday = datetime.now() - timedelta(days=1)
            daily_path = get_s3_sis_daily_path(yesterday)
            historical_path = app.config['LOCH_S3_SIS_DATA_PATH'] + '/historical'
            manifest_path = app.config['LOCH_S3_SIS_DATA_PATH'] + '/manifests'

            s3.upload_data('some new course data', f'{daily_path}/courses/courses-aaa.gz')
            s3.upload_data('some more new course data', f'{daily_path}/courses/courses-bbb.gz')
            s3.upload_data('some new enrollment data', f'{daily_path}/enrollments/enrollments-ccc.gz')
            s3.upload_data('some old course data', f'{historical_path}/courses/courses-ddd.gz')
            s3.upload_data('some old enrollment data', f'{historical_path}/enrollments/enrollments-eee.gz')
            s3.upload_data('some perfectly antique enrollment data', f'{historical_path}/enrollments/enrollments-fff.gz')

            assert CreateSisSchema().update_manifests()

            courses_manifest = json.loads(s3.get_object_text(manifest_path + '/courses.json'))
            assert len(courses_manifest['entries']) == 3
            assert courses_manifest['entries'][0]['url'] == f's3://{app.config["LOCH_S3_BUCKET"]}/{daily_path}/courses/courses-aaa.gz'
            assert courses_manifest['entries'][0]['meta']['content_length'] == 20

            enrollments_manifest = json.loads(s3.get_object_text(manifest_path + '/enrollments.json'))
            assert len(enrollments_manifest['entries']) == 3
            assert (enrollments_manifest['entries'][2]['url']
                    == f's3://{app.config["LOCH_S3_BUCKET"]}/{historical_path}/enrollments/enrollments-fff.gz')
            assert enrollments_manifest['entries'][2]['meta']['content_length'] == 38
