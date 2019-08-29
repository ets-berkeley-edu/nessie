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

from botocore.exceptions import ClientError, ConnectionError
from nessie.jobs.migrate_sis_advising_note_attachments import MigrateSisAdvisingNoteAttachments
from tests.util import capture_app_logs, mock_s3


def object_exists(m3, bucket, key):
    try:
        m3.Object(bucket, key).load()
        return True
    except (ClientError, ConnectionError, ValueError):
        return False


class TestMigrateSisAdvisingNoteAttachments:

    def test_run(self, app, caplog):
        """Copies files from dated folder to destination, organized into folders by SID."""
        bucket = app.config['LOCH_S3_PROTECTED_BUCKET']
        datestamp = '20190828'
        source_prefix = app.config['LOCH_S3_ADVISING_NOTE_ATTACHMENT_SOURCE_PATH']
        dest_prefix = app.config['LOCH_S3_ADVISING_NOTE_ATTACHMENT_DEST_PATH']

        caplog.set_level(logging.INFO)
        with capture_app_logs(app):
            with mock_s3(app, bucket=bucket) as m3:
                m3.Object(bucket, f'{source_prefix}/{datestamp}/12345678_00012_1.pdf').put(Body=b'a note attachment')
                m3.Object(bucket, f'{source_prefix}/{datestamp}/23456789_00003_1.png').put(Body=b'another note attachment')
                m3.Object(bucket, f'{source_prefix}/20190901/34567890_00014_2.xls').put(Body=b'don\'t copy me')

                MigrateSisAdvisingNoteAttachments().run(datestamp=datestamp)

                assert 'Copied 2 attachments to the destination folder.' in caplog.text
                assert object_exists(m3, bucket, f'{dest_prefix}/12345678/12345678_00012_1.pdf')
                assert object_exists(m3, bucket, f'{dest_prefix}/23456789/23456789_00003_1.png')
                assert not object_exists(m3, bucket, f'{dest_prefix}/34567890/34567890_00014_2.xls')
