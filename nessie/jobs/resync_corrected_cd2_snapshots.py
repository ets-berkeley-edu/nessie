"""
Copyright ©2024. The Regents of the University of California (Regents). All Rights Reserved.

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
from nessie.jobs.background_job import BackgroundJob
from nessie.lib import cd2_metadata


"""Logic to trigger query Canvas Data 2 snapshot job with Instructure."""


class ResyncCorrectedCD2Snapshots(BackgroundJob):

    def run(self, cleanup=True):
        # Find and Retrieve Active Canvas Data 2 Query Job from the Dynamo DB Metadata table
        last_cd2_query_job = cd2_metadata.get_recent_cd2_query_job_by_date_and_environment()

        corrected_snapshot_objects = []
        corrected_snapshot_env = ''
        snapshot_resync_status = ''
        corrected_snapshot_job_id = ''

        if last_cd2_query_job:
            if last_cd2_query_job['workflow_status']['snapshot_retrieved_status'] == 'success':
                app.logger.info('Resync not required as snapshot object retrieval is successful')
                return (f'Resync not required as CD2 snapshot object retrieval for the {last_cd2_query_job["environment"]} was successful')

            elif last_cd2_query_job['workflow_status']['snapshot_retrieved_status'] == 'failed':
                app.logger.info(f'Snapshot objects retirval attempt failed on {last_cd2_query_job["environment"]}.')
                app.logger.info('Starting resync process and checking for success in other environments')

                # Get all available snapshot jobs for the day across environemnts from the common metadata table
                todays_cd2_query_jobs = cd2_metadata.get_cd2_query_jobs_by_date_and_environment()

                # Set corrected snapshot details tracking successful runs in other environments.
                for job in todays_cd2_query_jobs:
                    if job['workflow_status']['snapshot_retrieved_status'] == 'success':
                        corrected_snapshot_objects = job['snapshot_objects']
                        corrected_snapshot_env = job['environment']
                        corrected_snapshot_job_id = job['cd2_query_job_id']
                        snapshot_resync_status = 'success'
                        break

                # Update CD2 metadata for the failed run in current environment with corrected snapshot details
                app.logger.info('Updating CD2 metadata table with corrected snapshot objects, environment, and resync_status')
                # Build corrected snapshot metadata updates
                metadata_updates = {
                    'corrected_snapshot_objects': corrected_snapshot_objects,
                    'corrected_snapshot_env': corrected_snapshot_env,
                    'corrected_snapshot_job_id': corrected_snapshot_job_id,
                    'workflow_status': {
                        'snapshot_resync_status': snapshot_resync_status,
                    },
                }

                update_status = cd2_metadata.update_cd2_metadata(
                    primary_key_name='cd2_query_job_id',
                    primary_key_value=last_cd2_query_job['cd2_query_job_id'],
                    sort_key_name='created_at',
                    sort_key_value=last_cd2_query_job['created_at'],
                    metadata_updates=metadata_updates,
                )

                if update_status:
                    app.logger.info('Metatdata updated with CD2 snapshot update status successfully')
                else:
                    app.logger.error('Metadata update failed')

                app.logger.info(f'Resync Corrected Snapshots successful for job {last_cd2_query_job["cd2_query_job_id"]}')
                return (f'Resync Corrected Snapshots successful for job {last_cd2_query_job["cd2_query_job_id"]}')

        else:
            return ('No CD2 query snapshot job triggered for today. Skipping refresh')
