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

from datetime import datetime, timedelta
import json

from nessie.externals import s3
from nessie.jobs.background_job import BackgroundJobError
from nessie.lib.util import get_s3_sis_daily_path
import pytest
from tests.util import capture_app_logs, mock_s3


class TestCreateSisSchema:

    def test_update_manifests(self, app):
        """Updates manifests in S3."""
        from nessie.jobs.create_sis_schema import CreateSisSchema
        with mock_s3(app):
            daily_path = get_s3_sis_daily_path()
            historical_path = app.config['LOCH_S3_SIS_DATA_PATH'] + '/historical'
            self._upload_data_to_s3(daily_path, historical_path)
            assert CreateSisSchema().update_manifests()
            self._assert_complete_manifest(app, daily_path, historical_path)

    def test_fallback_update_manifests(self, app):
        """Uses yesterday's news if today's is unavailable."""
        from nessie.jobs.create_sis_schema import CreateSisSchema
        with mock_s3(app):
            yesterday = datetime.now() - timedelta(days=1)
            daily_path = get_s3_sis_daily_path(yesterday)
            historical_path = app.config['LOCH_S3_SIS_DATA_PATH'] + '/historical'
            self._upload_data_to_s3(daily_path, historical_path)
            assert CreateSisSchema().update_manifests()
            self._assert_complete_manifest(app, daily_path, historical_path)

    def test_aborts_on_missing_term(self, app, caplog):
        from nessie.jobs.create_sis_schema import CreateSisSchema
        with mock_s3(app):
            daily_path = get_s3_sis_daily_path()
            historical_path = app.config['LOCH_S3_SIS_DATA_PATH'] + '/historical'
            self._upload_data_to_s3(daily_path, historical_path)
            s3.delete_objects([f'{daily_path}/enrollments/enrollments-2178.gz'])
            with capture_app_logs(app):
                with pytest.raises(BackgroundJobError) as e:
                    CreateSisSchema().update_manifests()
                assert 'Expected filename enrollments-2178.gz not found in S3, aborting' in str(e.value)

    def _upload_data_to_s3(self, daily_path, historical_path):
        s3.upload_data('some futuristic course data', f'{daily_path}/courses/courses-2182.gz')
        s3.upload_data('some futuristic enrollment data', f'{daily_path}/enrollments/enrollments-2182.gz')
        s3.upload_data('some new course data', f'{daily_path}/courses/courses-2178.gz')
        s3.upload_data('some new enrollment data', f'{daily_path}/enrollments/enrollments-2178.gz')
        s3.upload_data('some old course data', f'{historical_path}/courses/courses-2175.gz')
        s3.upload_data('some old enrollment data', f'{historical_path}/enrollments/enrollments-2175.gz')
        s3.upload_data('some older course data', f'{historical_path}/courses/courses-2172.gz')
        s3.upload_data('some older enrollment data', f'{historical_path}/enrollments/enrollments-2172.gz')
        s3.upload_data('some perfectly antique course data', f'{historical_path}/courses/courses-2168.gz')
        s3.upload_data('some perfectly antique enrollment data', f'{historical_path}/enrollments/enrollments-2168.gz')
        s3.upload_data('some ancient course data', f'{historical_path}/courses/courses-2165.gz')
        s3.upload_data('some ancient enrollment data', f'{historical_path}/enrollments/enrollments-2165.gz')

    def _assert_complete_manifest(self, app, daily_path, historical_path):
        bucket = app.config['LOCH_S3_BUCKET']
        manifest_path = app.config['LOCH_S3_SIS_DATA_PATH'] + '/manifests'

        courses_manifest = json.loads(s3.get_object_text(manifest_path + '/courses.json'))
        assert len(courses_manifest['entries']) == 6
        assert courses_manifest['entries'][0]['url'] == f's3://{bucket}/{daily_path}/courses/courses-2178.gz'
        assert courses_manifest['entries'][0]['meta']['content_length'] == 20

        enrollments_manifest = json.loads(s3.get_object_text(manifest_path + '/enrollments.json'))
        assert len(enrollments_manifest['entries']) == 6
        assert (enrollments_manifest['entries'][4]['url'] == f's3://{bucket}/{historical_path}/enrollments/enrollments-2172.gz')
        assert enrollments_manifest['entries'][4]['meta']['content_length'] == 26
