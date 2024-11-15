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

from flask import current_app as app
from nessie.externals import dynamodb


def get_cd2_query_jobs_by_date_and_environment(date_str=None, environment=None):
    try:
        # Initialize the DynamoDB resource
        dynamodb_resource = dynamodb.get_client()

        # Reference the DynamoDB table
        table = dynamodb_resource.Table(app.config['CD2_DYNAMODB_METADATA_TABLE'])

        if date_str is None:
            # Get today's date as a string as default
            today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            date_str = today_str

        # Define the basic filter expression and attribute values
        filter_expression = 'begins_with(created_at, :date)'
        expression_attribute_values = {':date': date_str}

        # Add the environment filter if provided
        if environment:
            filter_expression += ' AND environment = :env'
            expression_attribute_values[':env'] = environment

        # Scan and filter items based on date and (optionally) environment
        response = table.scan(
            FilterExpression=filter_expression,
            ExpressionAttributeValues=expression_attribute_values,
        )

        # Retrieve query jobs for the specified date (and environment, if provided)
        cd2_query_jobs = response.get('Items', [])

        return cd2_query_jobs

    except Exception as e:
        app.logger.error(f'Error retrieving CD2 query jobs for date {date_str} and environment {environment}: {str(e)}')
        return []


def get_recent_cd2_query_job_by_date_and_environment(date_str=None, environment=None):

    if date_str is None:
        # Get today's date as a string as default
        today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        date_str = today_str

    if environment is None:
        # Get Nessie environment from LOCH_BUCKET_NAME as a string as default
        environment = app.config['LOCH_S3_BUCKET']

    # Retrieve today's jobs for the specified environment
    todays_cd2_query_jobs = get_cd2_query_jobs_by_date_and_environment(date_str, environment)

    if not todays_cd2_query_jobs:
        app.logger.warning(f'No CD2 query jobs found for today in environment {environment}.')
        return None

    try:
        # Get the latest job by created_at
        last_cd2_query_job_metadata = max(
            todays_cd2_query_jobs,
            key=lambda x: datetime.fromisoformat(x['created_at']),
        )

        return last_cd2_query_job_metadata

    except ValueError as ve:
        app.logger.error(f'Error parsing created_at in CD2 query jobs: {str(ve)}')
        return None


def update_cd2_metadata(primary_key_name, primary_key_value, sort_key_name=None, sort_key_value=None, metadata_updates=None):
    try:
        # Initialize the DynamoDB resource
        dynamodb_resource = dynamodb.get_client()
        table_name = app.config.get('CD2_DYNAMODB_METADATA_TABLE')
        table = dynamodb_resource.Table(table_name)

        # Add `updated_at` field with the current timestamp
        metadata_updates = metadata_updates or {}
        metadata_updates['updated_at'] = datetime.now(timezone.utc).isoformat()

        # Build the UpdateExpression and ExpressionAttributeValues
        update_expression = 'SET '
        expression_attribute_values = {}

        for key, value in metadata_updates.items():
            if isinstance(value, dict):
                # Handle nested workflow_status fields
                for sub_key, sub_value in value.items():
                    update_expression += f'workflow_status.{sub_key} = :{key}_{sub_key}, '
                    expression_attribute_values[f':{key}_{sub_key}'] = sub_value
            else:
                # Top-level fields
                update_expression += f'{key} = :{key}, '
                expression_attribute_values[f':{key}'] = value

        # Remove the trailing comma and space from the UpdateExpression
        update_expression = update_expression.rstrip(', ')

        # Construct the Key dictionary
        key = {primary_key_name: primary_key_value}
        if sort_key_name and sort_key_value:
            key[sort_key_name] = sort_key_value

        app.logger.debug(f'UpdateExpression: {update_expression}')
        app.logger.debug(f'ExpressionAttributeValues: {expression_attribute_values}')

        # Execute the update operation
        response = table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues='UPDATED_NEW',
        )

        app.logger.info(f'Successfully updated metadata for {primary_key_name}={primary_key_value}')
        return response

    except Exception as e:
        app.logger.error(f'Error updating metadata in DynamoDB: {str(e)}')
        return None
