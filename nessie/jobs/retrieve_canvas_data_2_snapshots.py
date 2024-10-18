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

from datetime import datetime, timezone
import time

from flask import current_app as app
from nessie.externals import canvas_data_2, dynamodb, lambda_service
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError


"""Logic to trigger query Canvas Data 2 snapshot job with Instructure."""


class RetrieveCanvasData2Snapshots(BackgroundJob):

    def get_cd2_query_jobs_by_date(self, date_str):
        # Create the DynamoDB resource
        dynamodb_resource = dynamodb.get_client()

        # Reference the DynamoDB table
        table = dynamodb_resource.Table(app.config['CD2_DYNAMODB_METADATA_TABLE'])

        # Scan the table and filter items where created_at begins with today's date
        response = table.scan(
            FilterExpression='begins_with(created_at, :date)',
            ExpressionAttributeValues={
                ':date': date_str,
            },
        )

        # Extract Canvas data 2 query jobs from the metadata table for a given date.
        cd2_query_jobs = response.get('Items', [])

        return cd2_query_jobs

    def get_recent_cd2_query_job_metadata(self):
        today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        todays_cd2_query_jobs = self.get_cd2_query_jobs_by_date(today_str)

        last_cd2_query_job_metadata = max(todays_cd2_query_jobs, key=lambda x: datetime.fromisoformat(x['created_at']))

        return last_cd2_query_job_metadata

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

    def update_cd2_metadata(self, metadata):
        try:
            # Create the DynamoDB resource
            dynamodb_resource = dynamodb.get_client()

            # Access the DynamoDB table
            table = dynamodb_resource.Table(app.config['CD2_DYNAMODB_METADATA_TABLE'])

            # Get the current UTC timestamp
            updated_at = datetime.now(timezone.utc).isoformat()

            # New values for the update
            snapshot_objects = metadata['snapshot_objects']
            workflow_status = metadata['workflow_status']
            cd2_query_job_id = metadata['cd2_query_job_id']
            created_at = metadata['created_at']

            # Update the 'snapshot_objects', 'updated_at', and 'workflow_status' fields for the specified record
            response = table.update_item(
                Key={
                    # Partition key
                    'cd2_query_job_id': cd2_query_job_id,
                    # Sort key
                    'created_at': created_at,
                },
                UpdateExpression='SET snapshot_objects = :snapshot_objects, updated_at = :updated_at, workflow_status = :workflow_status',
                ExpressionAttributeValues={
                    ':snapshot_objects': snapshot_objects,
                    ':updated_at': updated_at,
                    ':workflow_status': workflow_status,
                },
                # Return the updated attributes
                ReturnValues='UPDATED_NEW',
            )

            app.logger.info(f'Successfully updated snapshot_objects, updated_at, and workflow_status for cd2_query_job_id {cd2_query_job_id}')
            return response

        except Exception as e:
            app.logger.error(f'Error updating CD2 metadata in DynamoDB: {str(e)}')
            return None

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
        cd2_table_query_jobs = []
        last_cd2_query_job = self.get_recent_cd2_query_job_metadata()

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
            last_cd2_query_job['snapshot_objects'] = cd2_table_objects
            last_cd2_query_job['workflow_status'] = 'snapshots_retrieved_successfully'

            query_job_failures = []
            for table_job in cd2_table_objects:
                if table_job['job_status'] == 'failed':
                    obj = {
                        'table': table_job['table'],
                        'job_id': table_job['job_id'],
                    }
                    query_job_failures.append(obj)
                    last_cd2_query_job['workflow_status'] = 'snapshots_retrieved_with_failures'

            # Update Canvas Data 2 Metadata table on Dynamo DB with metadata
            app.logger.info('Updating CD2 metadata table with snapshot retrival and job status details')
            update_status = self.update_cd2_metadata(last_cd2_query_job)
            if update_status:
                app.logger.info('Metatdata updated with CD2 snapshot update status successfully')

            if len(query_job_failures) > 0:
                app.logger.error(f'Encountered query job failures from source for following tables {query_job_failures}. Aborting ingest.')
                raise BackgroundJobError(f'Encountered query job failures from source for following tables {query_job_failures}. Aborting ingest.')

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

        else:
            return ('No CD2 query snapshot job triggered for today. Skipping refresh')
