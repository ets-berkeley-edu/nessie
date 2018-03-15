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

from nessie.jobs.sync_file_to_s3 import SyncFileToS3
import pytest
from tests.util import capture_app_logs


@pytest.mark.testext
class TestSyncFileToS3:

    def test_file_upload_and_skip(self, app, caplog, ensure_s3_bucket_empty):
        """Uploads files to S3, skipping duplicates."""
        url = 'http://shakespeare.mit.edu/Poetry/sonnet.XLV.html'
        key = '00001/sonnet-xlv.html'

        with capture_app_logs(app):
            result = SyncFileToS3().run(url=url, key=key)
            assert result is True
            assert 'Key 00001/sonnet-xlv.html does not exist, starting upload' in caplog.text
            assert 'S3 upload complete' in caplog.text

            result = SyncFileToS3().run(url=url, key=key)
            assert result is False
            assert 'Key 00001/sonnet-xlv.html exists, skipping upload' in caplog.text
