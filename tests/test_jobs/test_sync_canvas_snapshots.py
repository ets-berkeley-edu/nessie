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

import logging

from nessie.externals import rds, s3
from nessie.jobs.sync_canvas_snapshots import delete_objects_with_prefix, SyncCanvasSnapshots
import pytest
from tests.util import assert_background_job_status, capture_app_logs, mock_s3


class TestSyncCanvasSnapshots:
    """Sync Canvas snapshots job."""

    def test_sync_canvas_snapshots(self, app, metadata_db, caplog):
        """Dispatches a complete sync job against fixtures."""
        caplog.set_level(logging.INFO)
        with capture_app_logs(app):
            with mock_s3(app):
                result = SyncCanvasSnapshots().run_wrapped()
            assert 'Canvas snapshot sync job dispatched to workers' in result
            assert_background_job_status('sync')
            assert 'Dispatched S3 sync of snapshot quiz_dim-00000-0ab80c7c.gz' in caplog.text
            assert 'Dispatched S3 sync of snapshot requests-00098-b14782f5.gz' in caplog.text
            assert '311 successful dispatches, 0 failures' in caplog.text

            schema = app.config['RDS_SCHEMA_METADATA']

            count_results = rds.fetch(f'SELECT count(*) FROM {schema}.canvas_sync_job_status')
            assert count_results[0]['count'] == 311

            canvas_status_results = rds.fetch(f'SELECT DISTINCT status FROM {schema}.canvas_sync_job_status')
            assert len(canvas_status_results) == 1
            assert canvas_status_results[0]['status'] == 'created'

            sync_results = rds.fetch(f'SELECT * FROM {schema}.canvas_sync_job_status LIMIT 1')
            assert sync_results[0]['job_id'].startswith('sync_')
            assert sync_results[0]['filename'] == 'account_dim-00000-5eb7ee9e.gz'
            assert sync_results[0]['canvas_table'] == 'account_dim'
            assert 'account_dim/part-00505-5c40f1f3-b611-4f64-a007-67b775e984fe.c000.txt.gz' in sync_results[0]['source_url']
            assert sync_results[0]['destination_url'] is None
            assert sync_results[0]['details'] is None
            assert sync_results[0]['created_at']
            assert sync_results[0]['updated_at']

    @pytest.mark.testext
    def test_remove_obsolete_files(self, app, caplog, cleanup_s3):
        """Removes files from S3 following prefix and whitelist rules."""
        caplog.set_level(logging.INFO)
        with capture_app_logs(app):
            prefix1 = app.config['LOCH_S3_PREFIX_TESTEXT'] + '/001'
            prefix2 = app.config['LOCH_S3_PREFIX_TESTEXT'] + '/002'

            assert s3.upload_from_url('http://shakespeare.mit.edu/Poetry/sonnet.XX.html', prefix1 + '/xx/sonnet-xx.html')
            assert s3.upload_from_url('http://shakespeare.mit.edu/Poetry/sonnet.XXI.html', prefix1 + '/xxi/sonnet-xxi.html')
            assert s3.upload_from_url('http://shakespeare.mit.edu/Poetry/sonnet.XXII.html', prefix1 + '/xxii/sonnet-xxii.html')
            assert s3.upload_from_url('http://shakespeare.mit.edu/Poetry/sonnet.XLV.html', prefix2 + '/xlv/sonnet-xlv.html')

            whitelist = ['sonnet-xxi.html', 'sonnet-xxii.html']
            assert delete_objects_with_prefix(prefix1, whitelist) is True

            assert f'3 key(s) matching prefix "{prefix1}"' in caplog.text
            assert '2 key(s) in whitelist' in caplog.text
            assert 'will delete 1 object(s)' in caplog.text

            assert s3.object_exists(prefix1 + '/xx/sonnet-xx.html') is False
            assert s3.object_exists(prefix1 + '/xxi/sonnet-xxi.html') is True
            assert s3.object_exists(prefix1 + '/xxii/sonnet-xxii.html') is True
            assert s3.object_exists(prefix2 + '/xlv/sonnet-xlv.html') is True
