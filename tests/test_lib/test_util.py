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

from nessie.lib import util


class TestUtil:
    """Generic utilities."""

    def test_vacuum_whitespace(self):
        """Cleans up leading, trailing, and repeated whitespace."""
        assert util.vacuum_whitespace('  Firstname    Lastname   ') == 'Firstname Lastname'

    def test_legacy_note_datetime_to_utc(self, app):
        for expect_none in (None, '  ', '+00', 'garbled date'):
            assert util.legacy_note_datetime_to_utc(expect_none) is None, f'Failed on input: \'{expect_none}\''

        utc = util.legacy_note_datetime_to_utc('2016-12-14 06:17:08.821896+00')
        assert utc
        assert utc.tzinfo.zone == 'UTC'
        assert f'{utc.year}-{utc.month}-{utc.day} {utc.hour}:{utc.minute}:0{utc.second}' == '2016-12-14 14:17:08'

        utc = util.legacy_note_datetime_to_utc('2016-12-16 23:59:30+00')
        assert utc
        assert utc.tzinfo.zone == 'UTC'
        assert f'{utc.year}-{utc.month}-{utc.day} 0{utc.hour}:{utc.minute}:{utc.second}' == '2016-12-17 07:59:30'
