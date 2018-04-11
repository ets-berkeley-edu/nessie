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


"""Logic for Canvas snapshot sync job."""


import re

from flask import current_app as app
from nessie.externals import canvas_data, s3
from nessie.jobs.background_job import BackgroundJob, get_s3_canvas_daily_path
from nessie.lib.dispatcher import dispatch


def delete_objects_with_prefix(prefix, whitelist=[]):
    keys_to_delete = []
    existing_keys = s3.get_keys_with_prefix(prefix)
    if existing_keys is None:
        app.logger.error('Error listing keys, aborting job.')
        return False
    for key in existing_keys:
        filename = key.split('/')[-1]
        if filename not in whitelist:
            keys_to_delete.append(key)
    app.logger.info(
        f'Found {len(existing_keys)} key(s) matching prefix "{prefix}", {len(existing_keys) - len(keys_to_delete)} '
        f'key(s) in whitelist, will delete {len(keys_to_delete)} object(s)')
    if not keys_to_delete:
        return True
    return s3.delete_objects(keys_to_delete)


class SyncCanvasSnapshots(BackgroundJob):

    def run(self, cleanup=True):
        app.logger.info(f'Starting Canvas snapshot sync job...')

        snapshot_response = canvas_data.get_snapshots()
        if not snapshot_response:
            app.logger.error('Error retrieving Canvas data snapshots, aborting job.')
            return
        snapshots = snapshot_response.get('files', [])

        def should_sync(snapshot):
            # For tables other than requests, sync all snapshots.
            # For the requests table, sync snapshots that are partial or later than the configured cutoff date.
            def after_cutoff_date(url):
                match = re.search('requests/(20\d{6})', url)
                return match is not None and (match[1] >= app.config['LOCH_CANVAS_DATA_REQUESTS_CUTOFF_DATE'])
            return snapshot['table'] != 'requests' or snapshot['partial'] is True or after_cutoff_date(snapshot['url'])

        snapshots_to_sync = [s for s in snapshots if should_sync(s)]
        app.logger.info(f'Will sync {len(snapshots_to_sync)} of {len(snapshots)} available files from Canvas Data.')

        success = 0
        failure = 0

        for snapshot in snapshots_to_sync:
            if snapshot['table'] == 'requests':
                key_components = [app.config['LOCH_S3_CANVAS_DATA_PATH_CURRENT_TERM'], snapshot['table'], snapshot['filename']]
            else:
                key_components = [get_s3_canvas_daily_path(), snapshot['table'], snapshot['filename']]
            response = dispatch('sync_file_to_s3', data={'url': snapshot['url'], 'key': '/'.join(key_components)})
            if not response:
                app.logger.error('Failed to dispatch S3 sync of snapshot ' + snapshot['filename'])
                failure += 1
            else:
                app.logger.info('Dispatched S3 sync of snapshot ' + snapshot['filename'])
                success += 1

        if cleanup:
            app.logger.info('Will remove obsolete snapshots from S3.')
            current_snapshot_filenames = [s['filename'] for s in snapshots_to_sync]
            requests_prefix = app.config['LOCH_S3_CANVAS_DATA_PATH_CURRENT_TERM'] + '/requests'
            delete_result = delete_objects_with_prefix(requests_prefix, whitelist=current_snapshot_filenames)
            if not delete_result:
                app.logger.error('Cleanup of obsolete snapshots failed.')
        app.logger.info(f'Canvas snapshot sync job dispatched to workers ({success} successful dispatches, {failure} failures).')
