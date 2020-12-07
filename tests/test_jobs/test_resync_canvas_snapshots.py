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

import logging

from nessie.externals import canvas_data, rds
from nessie.jobs.resync_canvas_snapshots import ResyncCanvasSnapshots
from nessie.lib import metadata
from nessie.lib.util import get_s3_canvas_daily_path
from tests.util import assert_background_job_status, capture_app_logs, mock_s3


class TestResyncCanvasSnapshots:
    """Resync Canvas snapshots job."""

    def test_resync_canvas_snapshots(self, app, metadata_db, caplog):
        """Dispatches a complete resync job against fixtures."""
        caplog.set_level(logging.INFO)
        snapshots = canvas_data.get_snapshots()['files']

        def mock_metadata(job_id, snapshot, status, destination_size):
            metadata.create_canvas_sync_status(job_id, snapshot['filename'], snapshot['table'], snapshot['url'])
            key = '/'.join([get_s3_canvas_daily_path(), snapshot['table'], snapshot['filename']])
            metadata.update_canvas_sync_status(job_id, key, status, source_size=1048576, destination_size=destination_size)

        old_sync_job = 'sync_152550000'
        latest_sync_job = 'sync_152560000'

        # The older job should be ignored by the resync.
        for snapshot in snapshots[0:5]:
            mock_metadata(old_sync_job, snapshot, 'complete', 1048576)
        for snapshot in snapshots[5:10]:
            mock_metadata(old_sync_job, snapshot, 'error', None)

        # The latest job synced five files successfully and ran into three problems.
        for snapshot in snapshots[10:15]:
            mock_metadata(latest_sync_job, snapshot, 'complete', 1048576)
        stalled = snapshots[15]
        errored = snapshots[16]
        size_discrepancy = snapshots[17]
        mock_metadata(latest_sync_job, stalled, 'streaming', None)
        mock_metadata(latest_sync_job, errored, 'error', None)
        mock_metadata(latest_sync_job, size_discrepancy, 'complete', 65536)

        schema = app.config['RDS_SCHEMA_METADATA']

        with capture_app_logs(app):
            assert rds.fetch(f'SELECT count(*) FROM {schema}.canvas_sync_job_status')[0]['count'] == 18
            with mock_s3(app):
                result = ResyncCanvasSnapshots().run_wrapped()
            assert 'Canvas snapshot resync job dispatched to workers' in result
            assert_background_job_status('resync')
            assert f"Dispatched S3 resync of snapshot {stalled['filename']}" in caplog.text
            assert f"Dispatched S3 resync of snapshot {errored['filename']}" in caplog.text
            assert f"Dispatched S3 resync of snapshot {size_discrepancy['filename']}" in caplog.text
            assert '3 successful dispatches, 0 failures' in caplog.text

        assert rds.fetch(f'SELECT count(*) FROM {schema}.canvas_sync_job_status')[0]['count'] == 21
        resync_results = rds.fetch(f"SELECT * FROM {schema}.canvas_sync_job_status WHERE job_id LIKE 'resync%'")
        assert len(resync_results) == 3

        urls = []
        for r in resync_results:
            assert r['job_id'].startswith('resync_')
            assert r['filename']
            assert r['canvas_table']
            assert r['created_at']
            assert r['updated_at']
            urls.append(r['source_url'])
        assert stalled['url'] in urls
        assert errored['url'] in urls
        assert size_discrepancy['url'] in urls
