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


from contextlib import contextmanager
import logging

from nessie.jobs.background_job import BackgroundJobError
from nessie.jobs.verify_sis_advising_note_attachments import VerifySisAdvisingNoteAttachments
import pytest
from tests.util import capture_app_logs, mock_s3


def get_s3_refs(app):
    bucket = app.config['LOCH_S3_PROTECTED_BUCKET']
    source_prefix = app.config['LOCH_S3_ADVISING_NOTE_ATTACHMENT_SOURCE_PATH']
    dest_prefix = app.config['LOCH_S3_ADVISING_NOTE_ATTACHMENT_DEST_PATH']
    return (bucket, source_prefix, dest_prefix)


@contextmanager
def set_up_to_succeed(app, caplog):
    (bucket, source_prefix, dest_prefix) = get_s3_refs(app)
    caplog.set_level(logging.INFO)
    with capture_app_logs(app):
        with mock_s3(app, bucket=bucket) as m3:
            m3.Object(bucket, f'{source_prefix}/2017/01/18/12345678_00012_1.pdf').put(Body=b'a note attachment')
            m3.Object(bucket, f'{source_prefix}/2018/12/22/23456789_00003_1.png').put(Body=b'another note attachment')
            m3.Object(bucket, f'{source_prefix}/2019/08/29/34567890_00014_2.xls').put(Body=b'yet another note attachment')
            m3.Object(bucket, f'{dest_prefix}/12345678/12345678_00012_1.pdf').put(Body=b'a note attachment')
            m3.Object(bucket, f'{dest_prefix}/23456789/23456789_00003_1.png').put(Body=b'another note attachment')
            m3.Object(bucket, f'{dest_prefix}/34567890/34567890_00014_2.xls').put(Body=b'yet another note attachment')
            yield
    assert 'No attachments missing on S3 when compared against the view.' in caplog.text


@contextmanager
def set_up_to_fail(app, caplog):
    (bucket, source_prefix, dest_prefix) = get_s3_refs(app)
    caplog.set_level(logging.INFO)
    with capture_app_logs(app):
        with mock_s3(app, bucket=bucket) as m3:
            m3.Object(bucket, f'{source_prefix}/2017/01/18/12345678_00012_1.pdf').put(Body=b'a note attachment')
            m3.Object(bucket, f'{source_prefix}/2018/12/22/23456789_00003_1.png').put(Body=b'another note attachment')
            m3.Object(bucket, f'{dest_prefix}/12345678/12345678_00012_1.pdf').put(Body=b'a note attachment')
            m3.Object(bucket, f'{dest_prefix}/34567890/34567890_00014_2.xls').put(Body=b'yet another note attachment')
            m3.Object(bucket, f'{dest_prefix}/45678901/45678901_00192_4.xls').put(Body=b'bamboozled by a completely unexpected note attachment')
            with pytest.raises(BackgroundJobError) as e:
                yield
    assert 'Attachments verification found missing attachments or sync failures:' in str(e.value)
    assert '\'attachment_sync_failure_count\': 1' in str(e.value)
    assert '\'missing_s3_attachments_count\': 1' in str(e.value)
    assert '\'attachment_sync_failures\': [\'sis-data/sis-sftp/incremental/advising-notes/attachment-files/2018/12/22/23456789_00003_1.png\']' in str(
        e.value,
    )
    assert '\'missing_s3_attachments\': [\'23456789_00003_1.png\']' in str(e.value)
    assert 'Attachments missing on S3 when compared against SIS notes views: 1' in caplog.text


@pytest.fixture()
def prior_job_status(app):
    from nessie.externals import rds
    rds_schema = app.config['RDS_SCHEMA_METADATA']
    rds.execute(f"""INSERT INTO {rds_schema}.background_job_status
                (job_id, status, instance_id, created_at, updated_at)
                VALUES ('MigrateSisAdvisingNoteAttachments_123', 'succeeded', 'abc', '2018-12-21 00:00:00', '2018-12-21 00:00:00')""")


class TestVerifySisAdvisingNoteAttachments:
    """Validates the work of MigrateSisAdvisingNoteAttachments and reports any failures."""

    def test_run_with_no_param(self, app, caplog, sis_note_tables):
        """When no parameter is provided, validates all files."""
        with set_up_to_succeed(app, caplog):
            response = VerifySisAdvisingNoteAttachments().run()
        assert 'Will validate files from sis-data/sis-sftp/incremental/advising-notes/attachment-files.' in caplog.text
        assert 'No attachment sync failures found from sis-data/sis-sftp/incremental/advising-notes/attachment-files.' in caplog.text
        assert response == 'Note attachment verification completed successfully. No missing attachments or sync failures found.'

        with set_up_to_fail(app, caplog):
            response = VerifySisAdvisingNoteAttachments().run()
        assert 'Will validate files from sis-data/sis-sftp/incremental/advising-notes/attachment-files.' in caplog.text
        assert 'Total number of failed attachment syncs from sis-data/sis-sftp/incremental/advising-notes/attachment-files is 1' in caplog.text

    def test_run_with_all_param(self, app, caplog, sis_note_tables):
        """When 'all' is provided, validates all files."""
        with set_up_to_succeed(app, caplog):
            response = VerifySisAdvisingNoteAttachments().run(datestamp='all')
        assert 'Will validate files from sis-data/sis-sftp/incremental/advising-notes/attachment-files.' in caplog.text
        assert 'No attachment sync failures found from sis-data/sis-sftp/incremental/advising-notes/attachment-files.' in caplog.text
        assert response == 'Note attachment verification completed successfully. No missing attachments or sync failures found.'

        with set_up_to_fail(app, caplog):
            response = VerifySisAdvisingNoteAttachments().run(datestamp='all')
        assert 'Will validate files from sis-data/sis-sftp/incremental/advising-notes/attachment-files.' in caplog.text
        assert 'Total number of failed attachment syncs from sis-data/sis-sftp/incremental/advising-notes/attachment-files is 1' in caplog.text

    def test_run_with_datestamp_param(self, sis_note_tables, app, caplog, metadata_db):
        """When a datestamp is provided, validates files copied from the corresponding dated folder."""
        with set_up_to_succeed(app, caplog):
            response = VerifySisAdvisingNoteAttachments().run(datestamp='2018-12-22')
        assert 'Will validate files from sis-data/sis-sftp/incremental/advising-notes/attachment-files/2018/12/22.' in caplog.text
        assert 'No attachment sync failures found from sis-data/sis-sftp/incremental/advising-notes/attachment-files/2018/12/22.' in caplog.text
        assert response == 'Note attachment verification completed successfully. No missing attachments or sync failures found.'

        with set_up_to_fail(app, caplog):
            response = VerifySisAdvisingNoteAttachments().run(datestamp='2018-12-22')
        assert 'Will validate files from sis-data/sis-sftp/incremental/advising-notes/attachment-files/2018/12/22.' in caplog.text
        assert (
            'Total number of failed attachment syncs from sis-data/sis-sftp/incremental/advising-notes/attachment-files/2018/12/22 is 1'
        ) in caplog.text

    def test_run_with_partial_datestamp_param(self, sis_note_tables, app, caplog, metadata_db):
        """When a partial datestamp is provided, validates files copied from the corresponding dated folder."""
        with set_up_to_succeed(app, caplog):
            response = VerifySisAdvisingNoteAttachments().run(datestamp='2018')
        assert 'Will validate files from sis-data/sis-sftp/incremental/advising-notes/attachment-files/2018.' in caplog.text
        assert 'No attachment sync failures found from sis-data/sis-sftp/incremental/advising-notes/attachment-files/2018.' in caplog.text
        assert response == 'Note attachment verification completed successfully. No missing attachments or sync failures found.'

        with set_up_to_fail(app, caplog):
            response = VerifySisAdvisingNoteAttachments().run(datestamp='2018')
        assert 'Will validate files from sis-data/sis-sftp/incremental/advising-notes/attachment-files/2018.' in caplog.text
        assert (
            'Total number of failed attachment syncs from sis-data/sis-sftp/incremental/advising-notes/attachment-files/2018 is 1'
        ) in caplog.text
