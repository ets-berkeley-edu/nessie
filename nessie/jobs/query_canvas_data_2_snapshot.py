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
from nessie.jobs.background_job import BackgroundJob


"""Logic to trigger query Canvas Data 2 snapshot job with Instructure."""


class QueryCanvasData2Snapshot(BackgroundJob):

    @classmethod
    def generate_job_id(cls):
        return 'query_cd2_snapshot_' + str(int(time.time()))

    def get_cd2_table_objects(self, cd2_table_jobs):

        secret = canvas_data_2.get_cd2_secret()
        access_token = canvas_data_2.get_cd2_access_token()
        headers = {'x-instauth': access_token}

        for table_job in cd2_table_jobs:
            app.logger.info(f'Retrieving job status for table {table_job["table"]}')

            while True:
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
                elif job_status == 'failed':
                    app.logger.error(f'Job {table_job["job_id"]} failed to retrieve file objects for table {table_job["table"]}')
                    table_job['job_status'] = 'failed'
                    table_job['file_objects'] = []
                    break
        app.logger.info(f'Successfully retrieved file objects for all cd2 tables {cd2_table_jobs}')
        return cd2_table_jobs

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
        nessie_job_id = self.generate_job_id()
        app.logger.info(f'Starting Query Canvas Data 2 snapshot job... (id={nessie_job_id})')
        namespace = 'canvas'
        cd2_tables = canvas_data_2.get_cd2_tables_list(namespace)

        app.logger.info(f'{len(cd2_tables)} tables available for download from namespace {namespace}. \n{cd2_tables}')
        app.logger.info('Begin query snapshot process for each table and retrieve job ids for tracking')
        cd2_table_query_jobs = []
        cd2_table_query_jobs = canvas_data_2.start_query_snapshot(cd2_tables)

        app.logger.info(f'Started query snapshot jobs and retrived job IDs for {len(cd2_table_query_jobs)} Canvas data 2 tables')

        # TODO: Remove sleep timer and use metadata tables to store states for component jobs to refer and get status asynchronously.
        # Adds a temporary 20 min sleep time for the query snapshot job for source data to be available.
        app.logger.info('Wait for query snapshots jobs to complete. Sleep for 20 min')
        time.sleep(30 * 60)

        # Checks job status for the query snapshot job triggered for each CD2 table. Collect all available file object details once jobs are complete
        app.logger.info('Get File objects from query snapshot for each table')
        cd2_table_objects = self.get_cd2_table_objects(cd2_table_query_jobs)

        # Get downloadable URLs for all tables and dispatch jobs to Lambdas for S3 syncs
        cd2_files_to_sync = self.retrieve_cd2_file_urls(cd2_table_objects)
        app.logger.debug(f'CD2 file urls retrieved successfully {cd2_files_to_sync}')

        # Dispatch files details with urls for processing to microservicce
        dispatched_files = self.dispatch_for_download(cd2_files_to_sync)

        success = 0
        failure = 0
        for file in dispatched_files:
            if file['dispatch_status']:
                success += 1
            else:
                failure += 1

        app.logger.debug(f'Total files dispatched: {len(dispatched_files)}')
        return (f'Query snapshot dispatch Process completed with Success: {success} and Failed:{failure}. All done !')
