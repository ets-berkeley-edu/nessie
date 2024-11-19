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


import time

from flask import current_app as app
from nessie.externals import canvas_data_2
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib import cd2_metadata


"""Logic to trigger query Canvas Data 2 snapshot job with Instructure."""


class RetrieveCanvasData2Snapshots(BackgroundJob):

    def get_cd2_table_objects(self, cd2_table_jobs):

        secret = canvas_data_2.get_cd2_secret()
        access_token = canvas_data_2.get_cd2_access_token()
        headers = {'x-instauth': access_token}

        for table_job in cd2_table_jobs:
            app.logger.info(f'Retrieving job status for table {table_job["table"]}')

            counter = 0
            max_retries = 10
            while counter < max_retries:
                request_status = canvas_data_2.get_job_status(secret, headers, table_job['job_id'])
                job_status = request_status.json().get('status')

                # Check if status is 'complete' or 'failed'
                if job_status == 'complete':
                    file_objects = request_status.json().get('objects')
                    table_job['job_status'] = 'complete'
                    table_job['file_objects'] = file_objects
                    table_job['expires_at'] = request_status.json().get('expires_at')
                    table_job['job_started_at'] = request_status.json().get('at')
                    table_job['schema_version'] = request_status.json().get('schema_version')
                    app.logger.debug(f'File Objects retrieved successfully for {table_job["table"]}. \n{file_objects}')
                    break
                elif job_status == 'running':
                    table_job['job_status'] = 'running'
                    table_job['file_objects'] = []
                    # TODO: Remove the timer once metadata tables are used to track job status
                    app.logger.info('Wait for query snapshots jobs to complete. Sleep for 1 min')
                    time.sleep(1 * 60)
                    counter += 1
                elif job_status == 'failed':
                    app.logger.error(f'Job {table_job["job_id"]} failed to retrieve file objects for table {table_job["table"]}')
                    table_job['job_status'] = 'failed'
                    table_job['file_objects'] = []
                    break
        # After 10 retries, if the job is still 'running', it will break the loop
            if counter >= max_retries:
                app.logger.error(
                    f'Maximum retries reached for table {table_job["table"]} having job id {table_job["job_id"]}. '
                    f'Job still running after {max_retries} minutes.',
                )
                table_job['job_status'] = 'failed'

        app.logger.info(f'Successfully retrieved file objects for all cd2 tables {cd2_table_jobs} and job status')
        return cd2_table_jobs

    def run(self):
        # Find and Retrieve Active Canvas Data 2 Query Job from the Dynamo DB Metadata table
        cd2_table_query_jobs = []
        last_cd2_query_job = cd2_metadata.get_recent_cd2_query_job_by_date_and_environment()

        if last_cd2_query_job:
            app.logger.info(f'Latest CD2 Query Job triggered retrieved from metadata table {last_cd2_query_job}')
            cd2_table_query_jobs = last_cd2_query_job['table_query_jobs_id']

            app.logger.info(f'Tracking query snapshot jobs and retrived job IDs for {len(cd2_table_query_jobs)} Canvas data 2 tables')

            # Checks job status for the query snapshot job triggered for each CD2 table.
            # Collect all available file object details once jobs are complete
            app.logger.info('Get File objects from query snapshot for each table')
            cd2_table_objects = self.get_cd2_table_objects(cd2_table_query_jobs)
            app.logger.info(f'CD2 table objects retrieved successfully {cd2_table_objects}')

            # Build metadata updates
            # last_cd2_query_job['snapshot_objects'] = cd2_table_objects
            snapshot_retrieved_status = 'success'
            query_job_failures = []

            for table_job in cd2_table_objects:
                if table_job['job_status'] == 'failed':
                    obj = {
                        'table': table_job['table'],
                        'job_id': table_job['job_id'],
                    }
                    query_job_failures.append(obj)
                    snapshot_retrieved_status = 'failed'

            metadata_updates = {
                'snapshot_objects': cd2_table_objects,
                'workflow_status': {
                    'snapshot_retrieved_status': snapshot_retrieved_status,
                },
            }

            # Update Canvas Data 2 Metadata table on Dynamo DB with metadata
            app.logger.info('Updating CD2 metadata table with snapshot retrival and job status details')

            update_status = cd2_metadata.update_cd2_metadata(
                primary_key_name='cd2_query_job_id',
                primary_key_value=last_cd2_query_job['cd2_query_job_id'],
                sort_key_name='created_at',
                sort_key_value=last_cd2_query_job['created_at'],
                metadata_updates=metadata_updates,
            )

            if update_status:
                app.logger.info('Metatdata updated with CD2 snapshot update status successfully')

            if len(query_job_failures) > 0:
                app.logger.error(f'Encountered query job failures from source for following tables {query_job_failures}. Aborting ingest.')
                raise BackgroundJobError(f'Encountered query job failures from source for following tables {query_job_failures}. Aborting ingest.')
            else:
                app.logger.info('CD2 snapshot objects retrieved and metatdata update completed successfully ')
                return ('CD2 snapshot objects retrieved and metatdata update completed successfully ')

        else:
            return ('No CD2 query snapshot job triggered for today. Skipping refresh')
