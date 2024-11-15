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
from nessie.externals import canvas_data_2, lambda_service
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib import cd2_metadata


"""Logic to trigger query Canvas Data 2 snapshot job with Instructure."""


class RetrieveAndDispatchCD2FileUrls(BackgroundJob):

    def retrieve_cd2_file_urls(self, cd2_table_objects):
        secret = canvas_data_2.get_cd2_secret()
        access_token = canvas_data_2.get_cd2_access_token()
        headers = {'x-instauth': access_token}

        datestamp = time.strftime('%Y-%m-%d', time.gmtime())
        s3_path = f'{app.config["LOCH_S3_CANVAS_DATA_2_PATH_DAILY"]}/{datestamp}'
        files_to_sync = []

        for table_object in cd2_table_objects:
            app.logger.info(f'Getting presigned URLs for table {table_object["table"]}')
            # Get S3 presigned URLS for the resulting canvas data 2 query snapshot job objects.
            file_urls = canvas_data_2.get_cd2_file_urls(secret, headers, table_object['file_objects'])

            # Prepare event payloads to invoke lambdas to process and dowload the table data asynchronously
            files = [{'table': table_object['table'],
                      's3_bucket': app.config['LOCH_S3_BUCKET'],
                      's3_path': s3_path,
                      'cd2_secret_name': app.config['CD2_SECRET_NAME'],
                      'job_id': file_name.split('/')[0],
                      'file_name': file_name.split('/')[1],
                      'url': data['url']} for file_name, data in file_urls.json()['urls'].items()]

            app.logger.debug(f'File objects for table {table_object["table"]} : {files}')
            for file in files:
                files_to_sync.append(file)
        return files_to_sync

    def dispatch_for_download(self, cd2_file_urls):
        lambda_function_name = app.config['CD2_INGEST_LAMBDA_NAME']
        for file in cd2_file_urls:
            event_payload = file
            # Dispatch file payloads for processing to lambdas asynchronously and collect dispatch status
            result = lambda_service.invoke_lambda_function(lambda_function_name, event_payload, 'Event')  # Runs in 'Event' Asynchronous mode
            file['dispatch_status'] = result

        app.logger.debug('Dispatch of all files to lambdas complete.')
        return cd2_file_urls

    def run(self, cleanup=True):
        # Find and Retrieve Active Canvas Data 2 Query Job from the Dynamo DB Metadata table
        cd2_table_snapshot_objects = []
        last_cd2_query_job = cd2_metadata.get_recent_cd2_query_job_by_date_and_environment()

        if last_cd2_query_job:
            app.logger.info(f'Latest CD2 Query Job triggered retrieved from metadata table {last_cd2_query_job}')

            snapshot_retrieved_status = last_cd2_query_job['workflow_status']['snapshot_retrieved_status']
            snapshot_resync_status = last_cd2_query_job['workflow_status']['snapshot_resync_status']

            if snapshot_resync_status == 'success':
                cd2_table_snapshot_objects = last_cd2_query_job['corrected_snapshot_objects']
            elif snapshot_retrieved_status == 'success':
                cd2_table_snapshot_objects = last_cd2_query_job['snapshot_objects']
            else:
                raise BackgroundJobError('Retrieve and Resync of snapshot has failed for the day. Aborting ingest')

            # Get downloadable URLs for all tables and dispatch jobs to Lambdas for S3 syncs
            cd2_files_to_sync = self.retrieve_cd2_file_urls(cd2_table_snapshot_objects)
            app.logger.debug(f'CD2 file urls retrieved successfully {cd2_files_to_sync}')

            # Dispatch files details with urls for processing to microservicce
            dispatched_files = self.dispatch_for_download(cd2_files_to_sync)

            success = 0
            failure = 0
            retrieve_download_urls_status = ''
            dispatch_for_download_status = ''

            for file in dispatched_files:
                if file['dispatch_status']:
                    success += 1
                else:
                    failure += 1

            if failure > 0:
                retrieve_download_urls_status = 'failed'
                dispatch_for_download_status = 'failed'
            else:
                retrieve_download_urls_status = 'success'
                dispatch_for_download_status = 'success'

            # Update Canvas Data 2 Metadata table on Dynamo DB with metadata
            app.logger.info('Updating CD2 metadata table with snapshot retrival and job status details')
            # Build metadata updates
            metadata_updates = {
                'workflow_status': {
                    'retrieve_download_urls_status': retrieve_download_urls_status,
                    'dispatch_for_download_status': dispatch_for_download_status,
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

            app.logger.debug(f'Total files dispatched: {len(dispatched_files)}')
            return (f'Query snapshot dispatch Process completed with Success: {success} and Failed:{failure}. All done !')

        else:
            return ('No CD2 query snapshot job triggered for today. Skipping refresh')
