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

from nessie.externals import rds
from nessie.jobs.import_canvas_gradebook_history import ImportCanvasGradebookHistory
from tests.util import assert_background_job_status, mock_s3, override_config


class TestImportCanvasGradebookHistory:

    def test_run(self, app, metadata_db):
        """Uploads Canvas gradebook history to S3, then stores feeds in Redshift."""
        with mock_s3(app):
            with override_config(app, 'TEST_CANVAS_COURSE_IDS', [1492459, 1488704, 1491827]):
                result = ImportCanvasGradebookHistory().run_wrapped()
                assert result
                assert f'Canvas gradebook history import completed for term {2178}: 3 succeeded, ' in result
                assert '0 failed.' in result

        assert_background_job_status('ImportCanvasGradebookHistory')
        schema = app.config['RDS_SCHEMA_METADATA']
        count_results = rds.fetch(f'SELECT count(*) FROM {schema}.canvas_api_import_job_status')
        assert count_results[0]['count'] == 3

        canvas_status_results = rds.fetch(f'SELECT DISTINCT status FROM {schema}.canvas_api_import_job_status')
        assert len(canvas_status_results) == 1
        assert canvas_status_results[0]['status'] == 'created'

        sync_results = rds.fetch(f'SELECT * FROM {schema}.canvas_api_import_job_status LIMIT 1')
        assert sync_results[0]['job_id'].startswith('ImportCanvasGradebookHistory_')
        assert sync_results[0]['table_name'] == 'gradebook_history'
        assert sync_results[0]['details'] is None
        assert sync_results[0]['created_at']
        assert sync_results[0]['updated_at']
