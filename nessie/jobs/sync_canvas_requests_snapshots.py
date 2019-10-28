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

import time

from flask import current_app as app
from nessie.externals import canvas_data, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib import metadata
from nessie.lib.dispatcher import dispatch

"""Logic for Canvas snapshot sync job."""


class SyncCanvasRequestsSnapshots(BackgroundJob):

    @classmethod
    def generate_job_id(cls):
        return 'sync_canvas_requests_' + str(int(time.time()))

    def run(self, cleanup=True):
        job_id = self.generate_job_id()
        app.logger.info(f'Starting Canvas snapshot sync job... (id={job_id})')

        snapshot_response = canvas_data.get_snapshots()
        if not snapshot_response:
            raise BackgroundJobError('Error retrieving Canvas data snapshots, aborting job.')
        snapshots = snapshot_response.get('files', [])

        def should_sync(snapshot):
            return snapshot['table'] == 'requests' and snapshot['partial'] is False

        snapshots_to_sync = [s for s in snapshots if should_sync(s)]
        app.logger.info(f'Will sync {len(snapshots_to_sync)} of {len(snapshots)} available files from Canvas Data.')

        success = 0
        failure = 0

        for snapshot in snapshots_to_sync:
            metadata.create_canvas_sync_status(
                job_id=job_id,
                filename=snapshot['filename'],
                canvas_table=snapshot['table'],
                source_url=snapshot['url'],
            )

            key_components = [app.config['LOCH_S3_CANVAS_DATA_PATH_HISTORICAL'], snapshot['table'], snapshot['filename']]

            key = '/'.join(key_components)
            response = dispatch('sync_file_to_s3', data={'canvas_sync_job_id': job_id, 'url': snapshot['url'], 'key': key})

            if not response:
                app.logger.error('Failed to dispatch S3 sync of snapshot ' + snapshot['filename'])
                metadata.update_canvas_sync_status(job_id, key, 'error', details=f'Failed to dispatch: {response}')
                failure += 1
            else:
                app.logger.info('Dispatched S3 sync of snapshot ' + snapshot['filename'])
                success += 1

        if cleanup:
            app.logger.info('Will remove obsolete snapshots from S3.')
            current_snapshot_filenames = [s['filename'] for s in snapshots_to_sync]
            requests_prefix = app.config['LOCH_S3_CANVAS_DATA_PATH_HISTORICAL'] + '/requests'
            delete_result = s3.delete_objects_with_prefix(requests_prefix, whitelist=current_snapshot_filenames)
            if not delete_result:
                app.logger.error('Cleanup of obsolete snapshots failed.')
        return f'Canvas snapshot sync job dispatched to workers ({success} successful dispatches, {failure} failures).'
