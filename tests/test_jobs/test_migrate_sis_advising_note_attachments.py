"""
Copyright ©2022. The Regents of the University of California (Regents). All Rights Reserved.

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
import logging

from botocore.exceptions import ClientError, ConnectionError
import mock
from nessie.jobs.migrate_sis_advising_note_attachments import MigrateSisAdvisingNoteAttachments
import pytest
from tests.util import capture_app_logs, mock_s3


@pytest.fixture()
def prior_job_status(app):
    from nessie.externals import rds
    rds_schema = app.config['RDS_SCHEMA_METADATA']
    rds.execute(f"""INSERT INTO {rds_schema}.background_job_status
                (job_id, status, instance_id, created_at, updated_at)
                VALUES ('MigrateSisAdvisingNoteAttachments_123', 'succeeded', 'abc', '2019-08-27 05:21:00', '2019-08-27 05:22:00')""")


def object_exists(m3, bucket, key):
    try:
        m3.Object(bucket, key).load()
        return True
    except (ClientError, ConnectionError, ValueError):
        return False


def get_s3_refs(app):
    bucket = app.config['LOCH_S3_PROTECTED_BUCKET']
    source_prefix = app.config['LOCH_S3_ADVISING_NOTE_ATTACHMENT_SOURCE_PATH']
    dest_prefix = app.config['LOCH_S3_ADVISING_NOTE_ATTACHMENT_DEST_PATH']
    return (bucket, source_prefix, dest_prefix)


class TestMigrateSisAdvisingNoteAttachments:
    """Copies files from source path(s) to the destination, organized into folders by SID."""

    @mock.patch('nessie.lib.util.datetime', autospec=True)
    def test_first_time_run_with_no_param(self, mock_datetime, app, caplog, metadata_db):
        """When no parameter is provided and there is no prior successful run, copies all files."""
        (bucket, source_prefix, dest_prefix) = get_s3_refs(app)
        mock_datetime.utcnow.return_value = datetime(year=2019, month=8, day=29, hour=5, minute=21)

        caplog.set_level(logging.INFO)
        with capture_app_logs(app):
            with mock_s3(app, bucket=bucket) as m3:
                m3.Object(bucket, f'{source_prefix}/2019/08/28/12345678_00012_1.pdf').put(Body=b'a note attachment')
                m3.Object(bucket, f'{source_prefix}/2019/08/28/23456789_00003_1').put(Body=b'another note attachment')
                m3.Object(bucket, f'{source_prefix}/2019/08/29/34567890_00014_2.xls').put(Body=b'ok to copy me')

                response = MigrateSisAdvisingNoteAttachments().run()

                assert 'Will copy files from /sis-data/sis-sftp/incremental/advising-notes/attachment-files/.' in caplog.text
                assert 'Copied 3 attachments to the destination folder.' in caplog.text
                assert response == (
                    'SIS advising note attachment migration complete for sis-data/sis-sftp/incremental/advising-notes/attachment-files/.'
                )
                assert object_exists(m3, bucket, f'{dest_prefix}/12345678/12345678_00012_1.pdf')
                assert object_exists(m3, bucket, f'{dest_prefix}/23456789/23456789_00003_1')
                assert object_exists(m3, bucket, f'{dest_prefix}/34567890/34567890_00014_2.xls')

    @mock.patch('nessie.lib.util.datetime', autospec=True)
    def test_run_with_no_param(self, mock_datetime, app, caplog, metadata_db, prior_job_status):
        """When no parameter is provided, copies new files since the last succesful run."""
        (bucket, source_prefix, dest_prefix) = get_s3_refs(app)
        mock_datetime.utcnow.return_value = datetime(year=2019, month=8, day=29, hour=5, minute=21)

        caplog.set_level(logging.INFO)
        with capture_app_logs(app):
            with mock_s3(app, bucket=bucket) as m3:
                m3.Object(bucket, f'{source_prefix}/2019/08/25/45678912_00027_1.pdf').put(Body=b'i\'ve already been copied')
                m3.Object(bucket, f'{source_prefix}/2019/08/26/12345678_00012_1.pdf').put(Body=b'a note attachment')
                m3.Object(bucket, f'{source_prefix}/2019/08/28/23456789_00003_1').put(Body=b'another note attachment')
                m3.Object(bucket, f'{source_prefix}/2019/08/29/34567890_00014_2.xls').put(Body=b'don\'t copy me')

                response = MigrateSisAdvisingNoteAttachments().run()

                assert 'Will copy files from /sis-data/sis-sftp/incremental/advising-notes/attachment-files/2019/08/25.' not in caplog.text
                assert 'Will copy files from /sis-data/sis-sftp/incremental/advising-notes/attachment-files/2019/08/26.' in caplog.text
                assert 'Will copy files from /sis-data/sis-sftp/incremental/advising-notes/attachment-files/2019/08/27.' in caplog.text
                assert 'Will copy files from /sis-data/sis-sftp/incremental/advising-notes/attachment-files/2019/08/28.' in caplog.text
                assert 'Will copy files from /sis-data/sis-sftp/incremental/advising-notes/attachment-files/2019/08/29.' not in caplog.text
                assert 'Copied 1 attachments to the destination folder.' in caplog.text
                assert 'Copied 0 attachments to the destination folder.' in caplog.text
                assert response == (
                    'SIS advising note attachment migration complete for sis-data/sis-sftp/incremental/advising-notes/attachment-files/2019/08/26, \
sis-data/sis-sftp/incremental/advising-notes/attachment-files/2019/08/27, \
sis-data/sis-sftp/incremental/advising-notes/attachment-files/2019/08/28.'
                )
                assert not object_exists(m3, bucket, f'{dest_prefix}/45678912/45678912_00027_1.xls')
                assert object_exists(m3, bucket, f'{dest_prefix}/12345678/12345678_00012_1.pdf')
                assert object_exists(m3, bucket, f'{dest_prefix}/23456789/23456789_00003_1')
                assert not object_exists(m3, bucket, f'{dest_prefix}/34567890/34567890_00014_2.xls')

    def test_run_with_datestamp_param(self, app, caplog):
        """When datestamp is provided, copies files from the corresponding dated folder."""
        (bucket, source_prefix, dest_prefix) = get_s3_refs(app)
        datestamp = '2019-08-28'

        caplog.set_level(logging.INFO)
        with capture_app_logs(app):
            with mock_s3(app, bucket=bucket) as m3:
                m3.Object(bucket, f'{source_prefix}/2019/08/28/12345678_00012_1.pdf').put(Body=b'a note attachment')
                m3.Object(bucket, f'{source_prefix}/2019/08/28/23456789_00003_1').put(Body=b'another note attachment')
                m3.Object(bucket, f'{source_prefix}/2019/08/29/34567890_00014_2.xls').put(Body=b'don\'t copy me')

                response = MigrateSisAdvisingNoteAttachments().run(datestamp=datestamp)

                assert 'Will copy files from /sis-data/sis-sftp/incremental/advising-notes/attachment-files/2019/08/28.' in caplog.text
                assert 'Copied 2 attachments to the destination folder.' in caplog.text
                assert response == (
                    'SIS advising note attachment migration complete for sis-data/sis-sftp/incremental/advising-notes/attachment-files/2019/08/28.'
                )
                assert object_exists(m3, bucket, f'{dest_prefix}/12345678/12345678_00012_1.pdf')
                assert object_exists(m3, bucket, f'{dest_prefix}/23456789/23456789_00003_1')
                assert not object_exists(m3, bucket, f'{dest_prefix}/34567890/34567890_00014_2.xls')

    def test_run_with_all_param(self, app, caplog):
        """When 'all' is provided, copies all files."""
        (bucket, source_prefix, dest_prefix) = get_s3_refs(app)
        datestamp = 'all'

        caplog.set_level(logging.INFO)
        with capture_app_logs(app):
            with mock_s3(app, bucket=bucket) as m3:
                m3.Object(bucket, f'{source_prefix}/2019/08/28/12345678_00012_1.pdf').put(Body=b'a note attachment')
                m3.Object(bucket, f'{source_prefix}/2019/08/28/23456789_00003_1').put(Body=b'another note attachment')
                m3.Object(bucket, f'{source_prefix}/2019/08/29/34567890_00014_2.xls').put(Body=b'ok to copy me')

                response = MigrateSisAdvisingNoteAttachments().run(datestamp=datestamp)

                assert 'Will copy files from /sis-data/sis-sftp/incremental/advising-notes/attachment-files.' in caplog.text
                assert 'Copied 3 attachments to the destination folder.' in caplog.text
                assert response == (
                    'SIS advising note attachment migration complete for sis-data/sis-sftp/incremental/advising-notes/attachment-files.'
                )
                assert object_exists(m3, bucket, f'{dest_prefix}/12345678/12345678_00012_1.pdf')
                assert object_exists(m3, bucket, f'{dest_prefix}/23456789/23456789_00003_1')
                assert object_exists(m3, bucket, f'{dest_prefix}/34567890/34567890_00014_2.xls')

    def test_run_with_invalid_param(self, app, caplog):
        """When invalid value is provided, job completes but copies zero files."""
        (bucket, source_prefix, dest_prefix) = get_s3_refs(app)
        datestamp = 'wrong!#$&'

        caplog.set_level(logging.INFO)
        with capture_app_logs(app):
            with mock_s3(app, bucket=bucket) as m3:
                m3.Object(bucket, f'{source_prefix}/2019/08/28/12345678_00012_1.pdf').put(Body=b'a note attachment')

                response = MigrateSisAdvisingNoteAttachments().run(datestamp=datestamp)

                assert 'Will copy files from /sis-data/sis-sftp/incremental/advising-notes/attachment-files/wrong!#$&.' in caplog.text
                assert 'Copied 0 attachments to the destination folder.' in caplog.text
                assert response == (
                    'SIS advising note attachment migration complete for sis-data/sis-sftp/incremental/advising-notes/attachment-files/wrong!#$&.'
                )
                assert not object_exists(m3, bucket, f'{dest_prefix}/12345678/12345678_00012_1.pdf')

    def test_malformed_filenames(self, app, caplog):
        (bucket, source_prefix, dest_prefix) = get_s3_refs(app)
        datestamp = 'all'

        caplog.set_level(logging.INFO)
        with capture_app_logs(app):
            with mock_s3(app, bucket=bucket) as m3:
                m3.Object(bucket, f'{source_prefix}/2019/08/28/12345678_00012_1_May_7_2019_email.pdf').put(Body=b'extra chars in my name lol')
                m3.Object(bucket, f'{source_prefix}/2019/08/28/23456789_00052_1.png.png').put(Body=b'somehow i got a redundant .ext')
                m3.Object(bucket, f'{source_prefix}/2019/08/29/23456789_00053_1._DEGREE_COMPLETION_LETTER').put(
                    Body=b'original file name mistaken for the .ext',
                )
                m3.Object(bucket, f'{source_prefix}/2019/08/29/34567890_00014_2..7.19_(2)-edited_(1)-2_(1)_(1).xls').put(
                    Body=b'is this a versioning scheme?',
                )

                MigrateSisAdvisingNoteAttachments().run(datestamp=datestamp)

                assert 'Copied 4 attachments to the destination folder.' in caplog.text
                assert object_exists(m3, bucket, f'{dest_prefix}/12345678/12345678_00012_1.pdf')
                assert object_exists(m3, bucket, f'{dest_prefix}/23456789/23456789_00052_1.png')
                assert object_exists(m3, bucket, f'{dest_prefix}/23456789/23456789_00053_1')
                assert object_exists(m3, bucket, f'{dest_prefix}/34567890/34567890_00014_2.xls')
