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


from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.util import get_s3_sis_attachment_path, normalize_sis_note_attachment_file_name


"""Logic for validating SIS advising note attachments."""


class VerifySisAdvisingNoteAttachments(BackgroundJob):

    def run(self, datestamp=None):
        s3_attachment_sync_failures = []
        missing_s3_attachments = []
        app.logger.info('Starting SIS Advising Note attachments validation job...')

        dest_prefix = app.config['LOCH_S3_ADVISING_NOTE_ATTACHMENT_DEST_PATH']

        for source_prefix in self.source_paths(datestamp):
            app.logger.info(f'Will validate files from {source_prefix}.')
            s3_attachment_sync_failures.extend(self.verify_attachment_migration(source_prefix, dest_prefix))

        missing_s3_attachments = self.find_missing_notes_view_attachments(dest_prefix)

        if s3_attachment_sync_failures or missing_s3_attachments:
            verification_results = {
                'attachment_sync_failure_count': len(s3_attachment_sync_failures),
                'missing_s3_attachments_count': len(missing_s3_attachments),
                'attachment_sync_failures': s3_attachment_sync_failures,
                'missing_s3_attachments': missing_s3_attachments,
            }
            raise BackgroundJobError(f'Attachments verification found missing attachments or sync failures:  {verification_results}.')
        else:
            return 'Note attachment verification completed successfully. No missing attachments or sync failures found.'

    def source_paths(self, datestamp):
        if datestamp:
            return get_s3_sis_attachment_path(datestamp)
        return [app.config['LOCH_S3_ADVISING_NOTE_ATTACHMENT_SOURCE_PATH']]

    def verify_attachment_migration(self, source_prefix, dest_prefix):
        s3_attachment_sync_failures = []
        bucket = app.config['LOCH_S3_PROTECTED_BUCKET']

        source_attachments = sorted(s3.get_keys_with_prefix(source_prefix, False, bucket))
        dest_attachments = sorted(s3.get_keys_with_prefix(dest_prefix, False, bucket))

        for source_key in source_attachments:
            file_name = normalize_sis_note_attachment_file_name(source_key)
            sid = file_name.split('_')[0]
            dest_key = f'{dest_prefix}/{sid}/{file_name}'

            if dest_key not in dest_attachments:
                s3_attachment_sync_failures.append(source_key)

        if s3_attachment_sync_failures:
            app.logger.error(f'Total number of failed attachment syncs from {source_prefix} is {len(s3_attachment_sync_failures)} \
              \n {s3_attachment_sync_failures}.')
        else:
            app.logger.info(f'No attachment sync failures found from {source_prefix}.')

        return s3_attachment_sync_failures

    def get_all_notes_attachments(self):
        results = redshift.fetch(f"""
            SELECT DISTINCT sis_file_name FROM {app.config['REDSHIFT_SCHEMA_EDL']}.advising_note_attachments""")
        sis_notes_attachments = set([r['sis_file_name'] for r in results])
        return sis_notes_attachments

    def find_missing_notes_view_attachments(self, dest_prefix):
        # Checks for attachments in SIS view that are not on S3.
        missing_s3_attachments = []
        sis_attachments_files_names = []
        bucket = app.config['LOCH_S3_PROTECTED_BUCKET']
        sis_notes_view_attachments = sorted(self.get_all_notes_attachments())
        sis_s3_attachments = sorted(s3.get_keys_with_prefix(dest_prefix, False, bucket))

        for dest_key in sis_s3_attachments:
            file_name = dest_key.split('/')[-1]
            sis_attachments_files_names.append(file_name)

        for file_name in sis_notes_view_attachments:
            if file_name not in sis_attachments_files_names:
                missing_s3_attachments.append(file_name)

        if missing_s3_attachments:
            app.logger.error(f'Attachments missing on S3 when compared against SIS notes views: {len(missing_s3_attachments)} \
             \n {missing_s3_attachments}.')
        else:
            app.logger.info('No attachments missing on S3 when compared against the view.')

        return missing_s3_attachments
