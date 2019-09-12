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


from datetime import date, datetime, timedelta

from dateutil.rrule import DAILY, rrule
from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.metadata import most_recent_background_job_status


"""Logic for validating SIS advising note attachments."""


class VerifySisAdvisingNoteAttachments(BackgroundJob):

    def run(self, datestamp=None):
        s3_attachment_sync_failures = []
        missing_s3_attachments = []
        app.logger.info(f'Starting SIS Advising Note attachments Validation job...')

        dest_prefix = app.config['LOCH_S3_ADVISING_NOTE_ATTACHMENT_DEST_PATH']

        for path in self.source_paths(datestamp):
            source_prefix = '/'.join([app.config['LOCH_S3_ADVISING_NOTE_ATTACHMENT_SOURCE_PATH'], path])
            app.logger.info(f'Will validate files from {source_prefix}.')
            s3_attachment_sync_failures.extend(self.verify_attachment_migration(source_prefix, dest_prefix))

        missing_s3_attachments = self.find_missing_notes_view_attachments(dest_prefix)

        verification_results = {
            's3_attachment_sync_failures': s3_attachment_sync_failures,
            'missing_s3_attachments': missing_s3_attachments,
        }

        if s3_attachment_sync_failures or missing_s3_attachments:
            raise BackgroundJobError(f'Attachments verification found missing attachments or sync failures:  {verification_results}')
        else:
            return f'Note attachment verification completed successfully. No missing attachments or sync failures found.'

    def source_paths(self, datestamp):
        if datestamp == 'all':
            return ['']
        if datestamp == 'month':
            return [date.today().strftime('%Y/%m')]
        if datestamp:
            return [datestamp]

        # If no datestamp param, calculate a range of dates from the last successful run to yesterday.
        # The files land in S3 in PDT, but we're running in UTC, so we subtract 1 day from start and end date.
        last_successful_run = most_recent_background_job_status(self.__class__.__name__, 'succeeded')
        if not last_successful_run:
            return ['']
        start_date = last_successful_run.get('updated_at') - timedelta(days=1)
        yesterday = datetime.now() - timedelta(days=1)

        return [d.strftime('%Y/%m/%d') for d in rrule(DAILY, dtstart=start_date, until=yesterday)]

    def verify_attachment_migration(self, source_prefix, dest_prefix):
        s3_attachment_sync_failures = []
        bucket = app.config['LOCH_S3_PROTECTED_BUCKET']

        source_attachments = sorted(s3.get_keys_with_prefix(source_prefix, False, bucket))
        dest_attachments = sorted(s3.get_keys_with_prefix(dest_prefix, False, bucket))

        for source_key in source_attachments:
            file_name = source_key.split('/')[-1]
            sid = file_name.split('_')[0]
            dest_key = f'{dest_prefix}/{sid}/{file_name}'

            if dest_key not in dest_attachments:
                s3_attachment_sync_failures.append(source_key)

        if s3_attachment_sync_failures:
            app.logger.error(f'Total number of failed attachment syncs from {source_prefix} is {len(s3_attachment_sync_failures)} \
              \n {s3_attachment_sync_failures}')
        else:
            app.logger.info(f'No attachment sync failures found from {source_prefix}')

        return s3_attachment_sync_failures

    def get_all_notes_attachments(self):
        advisor_schema_redshift = app.config['REDSHIFT_SCHEMA_SIS_ADVISING_NOTES_INTERNAL']
        sis_notes_attachments = set(
            [r['sis_file_name'] for r in redshift.fetch(f'SELECT DISTINCT sis_file_name FROM {advisor_schema_redshift}.advising_note_attachments')],
        )
        return sis_notes_attachments

    def find_missing_notes_view_attachments(self, dest_prefix):
        missing_s3_attachments = []
        bucket = app.config['LOCH_S3_PROTECTED_BUCKET']
        sis_notes_view_attachments = sorted(self.get_all_notes_attachments())
        sis_s3_attachments = sorted(s3.get_keys_with_prefix(dest_prefix, False, bucket))

        for dest_key in sis_s3_attachments:
            file_name = dest_key.split('/')[-1]

            if file_name not in sis_notes_view_attachments:
                missing_s3_attachments.append(file_name)

        if missing_s3_attachments:
            app.logger.error(f'Attachments missing on S3 when compared against SIS notes views: {len(missing_s3_attachments)} \
             \n {missing_s3_attachments}')
        else:
            app.logger.info(f'No attachments missing on S3 when compared against the view.')

        return missing_s3_attachments
