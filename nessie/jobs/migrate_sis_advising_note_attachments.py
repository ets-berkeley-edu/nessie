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


from flask import current_app as app
from nessie.externals import s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.metadata import most_recent_background_job_status
from nessie.lib.util import get_s3_sis_attachment_current_paths, get_s3_sis_attachment_path


"""Logic for migrating SIS advising note attachments. Expects a full or partial datestamp parameter in the form YYYY-MM-DD."""


class MigrateSisAdvisingNoteAttachments(BackgroundJob):

    def run(self, datestamp=None):
        app.logger.info(f'Starting SIS Advising Note attachments migration job...')

        dest_prefix = app.config['LOCH_S3_ADVISING_NOTE_ATTACHMENT_DEST_PATH']

        source_paths = self.source_paths(datestamp)
        for source_prefix in source_paths:
            app.logger.info(f'Will copy files from /{source_prefix}.')
            self.copy_to_destination(source_prefix, dest_prefix)

        sources = ', '.join(source_paths) if len(source_paths) else 'all files'
        return f'SIS advising note attachment migration complete for {sources}.'

    def source_paths(self, datestamp):
        if datestamp:
            return get_s3_sis_attachment_path(datestamp)

        last_successful_run = most_recent_background_job_status(self.__class__.__name__, 'succeeded')
        begin_dt = last_successful_run.get('updated_at') if last_successful_run else None
        return get_s3_sis_attachment_current_paths(begin_dt)

    def copy_to_destination(self, source_prefix, dest_prefix):
        bucket = app.config['LOCH_S3_PROTECTED_BUCKET']
        objects = s3.get_keys_with_prefix(source_prefix, bucket=app.config['LOCH_S3_PROTECTED_BUCKET'])
        for o in objects:
            file_name = o.split('/')[-1]
            sid = file_name.split('_')[0]

            dest_key = f'{dest_prefix}/{sid}/{file_name}'
            if not s3.copy(bucket, o, bucket, dest_key):
                raise BackgroundJobError(f'Copy from source to destination {dest_key} failed.')

        app.logger.info(f'Copied {len(objects) if objects else 0} attachments to the destination folder.')
