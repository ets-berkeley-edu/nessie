"""
Copyright ©2018. The Regents of the University of California (Regents). All Rights Reserved.

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

from nessie.externals import redshift
from nessie.jobs.sync_file_to_s3 import SyncFileToS3
from nessie.lib import metadata
from nessie.lib.mockingbird import _get_fixtures_path
import pytest
import responses
from tests.util import capture_app_logs, mock_s3


class TestSyncFileToS3:

    @pytest.mark.testext
    def test_file_upload_and_skip(self, app, caplog, cleanup_s3):
        """Uploads files to real S3, skipping duplicates."""
        url = 'http://shakespeare.mit.edu/Poetry/sonnet.XLV.html'
        key = app.config['LOCH_S3_PREFIX_TESTEXT'] + '/00001/sonnet-xlv.html'

        with capture_app_logs(app):
            result = SyncFileToS3().run(url=url, key=key)
            assert result is True
            assert f'Key {key} does not exist, starting upload' in caplog.text
            assert 'S3 upload complete' in caplog.text

            result = SyncFileToS3().run(url=url, key=key)
            assert result is False
            assert f'Key {key} exists, skipping upload' in caplog.text

    def test_canvas_sync_metadata(self, app, metadata_db):
        """When given a job id, updates metadata on file sync."""
        url = 'http://shakespeare.mit.edu/Poetry/sonnet.XLV.html'
        key = 'canvas/sonnet_submission_dim/sonnet-xlv.txt'

        with mock_s3(app):
            with open(_get_fixtures_path() + '/sonnet_xlv.html', 'r') as file:
                responses.add(responses.GET, url, body=file.read(), headers={'Content-Length': '767'})

            # Run two successive sync jobs on the same file. The first succeeds, the second is skipped as
            # a duplicate.
            metadata.create_canvas_sync_status('job_1', 'sonnet-xlv.txt', 'sonnet_submission_dim', url)
            result = SyncFileToS3().run(url=url, key=key, canvas_sync_job_id='job_1')
            assert result is True
            metadata.create_canvas_sync_status('job_2', 'sonnet-xlv.txt', 'sonnet_submission_dim', url)
            result = SyncFileToS3().run(url=url, key=key, canvas_sync_job_id='job_2')
            assert result is False

            schema = app.config['REDSHIFT_SCHEMA_METADATA']
            job_metadata = redshift.fetch(f'SELECT * FROM {schema}.canvas_sync_job_status')
            snapshot_metadata = redshift.fetch(f'SELECT * FROM {schema}.canvas_synced_snapshots')

            assert len(job_metadata) == 2
            assert job_metadata[0].job_id == 'job_1'
            assert job_metadata[0].destination_url == 's3://bucket_name/canvas/sonnet_submission_dim/sonnet-xlv.txt'
            assert job_metadata[0].status == 'complete'
            assert job_metadata[0].source_size == 767
            assert job_metadata[0].destination_size == 767
            assert job_metadata[0].updated_at > job_metadata[0].created_at
            assert job_metadata[1].job_id == 'job_2'
            assert job_metadata[1].destination_url == 's3://bucket_name/canvas/sonnet_submission_dim/sonnet-xlv.txt'
            assert job_metadata[1].status == 'duplicate'
            assert job_metadata[1].source_size is None
            assert job_metadata[1].destination_size is None
            assert job_metadata[1].updated_at > job_metadata[1].created_at

            assert len(snapshot_metadata) == 1
            assert snapshot_metadata[0].filename == 'sonnet-xlv.txt'
            assert snapshot_metadata[0].canvas_table == 'sonnet_submission_dim'
            assert snapshot_metadata[0].url == 's3://bucket_name/canvas/sonnet_submission_dim/sonnet-xlv.txt'
            assert snapshot_metadata[0].size == 767
            assert snapshot_metadata[0].created_at
            assert snapshot_metadata[0].deleted_at is None
