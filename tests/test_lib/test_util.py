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

from datetime import datetime

import mock
from nessie.lib import util


class TestUtil:
    """Generic utilities."""

    def test_vacuum_whitespace(self):
        """Cleans up leading, trailing, and repeated whitespace."""
        assert util.vacuum_whitespace('  Firstname    Lastname   ') == 'Firstname Lastname'

    @mock.patch('nessie.lib.util.datetime', autospec=True)
    def test_get_s3_sis_attachment_current_paths(self, mock_datetime, app):
        """Returns a list of S3 paths to SIS attachments that need to be migrated."""
        prefix = app.config['LOCH_S3_ADVISING_NOTE_ATTACHMENT_SOURCE_PATH']
        assert util.get_s3_sis_attachment_current_paths() == [f'{prefix}/']

        # Start date is 9/20 5am UTC (9/19 10pm PST). Today is 9/23 5am UTC (9/22 10pm PST).
        mock_datetime.utcnow.return_value = datetime(year=2019, month=9, day=23, hour=5, minute=21)
        paths = util.get_s3_sis_attachment_current_paths(datetime(year=2019, month=9, day=20, hour=5, minute=22))
        assert len(paths) == 4
        assert paths[0] == f'{prefix}/2019/09/19'
        assert paths[1] == f'{prefix}/2019/09/20'
        assert paths[2] == f'{prefix}/2019/09/21'
        assert paths[3] == f'{prefix}/2019/09/22'

        # Start date is 9/25 5am UTC (9/24 10pm PST). Today is 9/26 5am UTC (9/25 10pm PST).
        mock_datetime.utcnow.return_value = datetime(year=2019, month=9, day=26, hour=5, minute=21)
        paths = util.get_s3_sis_attachment_current_paths(datetime(year=2019, month=9, day=25, hour=5, minute=22))
        assert len(paths) == 2
        assert paths[0] == f'{prefix}/2019/09/24'
        assert paths[1] == f'{prefix}/2019/09/25'

        # Start date is 9/20 5am UTC (9/19 10pm PST). Today is 9/20 5pm UTC (9/20 10am PST)
        mock_datetime.utcnow.return_value = datetime(year=2019, month=9, day=20, hour=17, minute=21)
        paths = util.get_s3_sis_attachment_current_paths(datetime(year=2019, month=9, day=20, hour=5, minute=22))
        assert len(paths) == 2
        assert paths[0] == f'{prefix}/2019/09/19'
        assert paths[1] == f'{prefix}/2019/09/20'

        # Start date is 9/20 5pm UTC (9/20 10am PST). Today is 9/21 5am UTC (9/20 10pm PST)
        mock_datetime.utcnow.return_value = datetime(year=2019, month=9, day=21, hour=5, minute=21)
        paths = util.get_s3_sis_attachment_current_paths(datetime(year=2019, month=9, day=20, hour=17, minute=22))
        assert len(paths) == 1
        assert paths[0] == f'{prefix}/2019/09/20'

        # Start date is 9/20 5pm UTC (9/20 10am PST). Today is 9/21 6am UTC (9/20 11pm PST)
        mock_datetime.utcnow.return_value = datetime(year=2019, month=9, day=21, hour=6, minute=21)
        paths = util.get_s3_sis_attachment_current_paths(datetime(year=2019, month=9, day=20, hour=17, minute=22))
        assert len(paths) == 1
        assert paths[0] == f'{prefix}/2019/09/20'

        # Start date is 9/20 5pm UTC (9/20 10am PST). Today is 9/21 7am UTC (9/21 12am PST)
        mock_datetime.utcnow.return_value = datetime(year=2019, month=9, day=21, hour=7, minute=21)
        paths = util.get_s3_sis_attachment_current_paths(datetime(year=2019, month=9, day=20, hour=17, minute=22))
        assert len(paths) == 2
        assert paths[0] == f'{prefix}/2019/09/20'
        assert paths[1] == f'{prefix}/2019/09/21'
