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

from nessie.externals import s3
from nessie.jobs.sync_canvas_dumps import SyncCanvasDumps
import pytest
from tests.util import capture_app_logs


@pytest.mark.testext
class TestSyncCanvasDumps:

    def test_remove_obsolete_files(self, app, caplog, ensure_s3_bucket_empty):
        """Removes files from S3 following prefix and whitelist rules."""
        with capture_app_logs(app):
            assert s3.upload_from_url('http://shakespeare.mit.edu/Poetry/sonnet.XX.html', '001/xx/sonnet-xx.html')
            assert s3.upload_from_url('http://shakespeare.mit.edu/Poetry/sonnet.XXI.html', '001/xxi/sonnet-xxi.html')
            assert s3.upload_from_url('http://shakespeare.mit.edu/Poetry/sonnet.XXII.html', '001/xxii/sonnet-xxii.html')
            assert s3.upload_from_url('http://shakespeare.mit.edu/Poetry/sonnet.XLV.html', '002/xlv/sonnet-xlv.html')

            prefix = '001'
            whitelist = ['sonnet-xxi.html', 'sonnet-xxii.html']
            assert SyncCanvasDumps().delete_objects_with_prefix(prefix, whitelist) is True

            assert '3 key(s) matching prefix "001"' in caplog.text
            assert '2 key(s) in whitelist' in caplog.text
            assert 'will delete 1 object(s)' in caplog.text

            assert s3.object_exists('001/xx/sonnet-xx.html') is False
            assert s3.object_exists('001/xxi/sonnet-xxi.html') is True
            assert s3.object_exists('001/xxii/sonnet-xxii.html') is True
            assert s3.object_exists('002/xlv/sonnet-xlv.html') is True
