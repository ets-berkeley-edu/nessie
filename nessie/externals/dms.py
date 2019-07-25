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

import boto3
from botocore.exceptions import ClientError, ConnectionError
from flask import current_app as app

"""Client code to run AWS DMS operations."""


def create_s3_target(identifier, path):
    client = get_client()
    try:
        response = client.create_endpoint(
            EndpointIdentifier=identifier,
            EndpointType='target',
            EngineName='s3',
            S3Settings={
                'ServiceAccessRoleArn': app.config['AWS_DMS_VPC_ROLE'],
                'ExternalTableDefinition': 'string',
                'CsvRowDelimiter': '\\n',
                'CsvDelimiter': '\\t',
                'BucketFolder': path,
                'BucketName': app.config['LOCH_S3_BUCKET'],
                'CompressionType': 'gzip',
            },
        )
        if response:
            app.logger.info(f'DMS S3 target created (identifier={identifier}, path={path})')
            return response
        else:
            app.logger.info(f'Failed to create DMS S3 target (identifier={identifier}, path={path}, response={response})')
            return None
    except (ClientError, ConnectionError) as e:
        app.logger.error(f'Error creating DMS S3 target (path={path}, error={e})')
        return None


def get_sts_credentials():
    sts_client = boto3.client('sts')
    role_arn = app.config['AWS_APP_ROLE_ARN']
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName='AssumeAppRoleSession',
        DurationSeconds=900,
    )
    return assumed_role_object['Credentials']


def get_session():
    credentials = get_sts_credentials()
    return boto3.Session(
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
    )


def get_client():
    session = get_session()
    return session.client(
        'dms',
        region_name=app.config['LOCH_S3_REGION'],
    )


def get_replication_task(identifier):
    tasks = get_replication_tasks(identifier)
    if tasks and len(tasks):
        return tasks[0]


def get_replication_tasks(identifier=None):
    client = get_client()
    try:
        kwargs = {}
        if identifier:
            kwargs['Filters'] = [
                {
                    'Name': 'replication-task-id',
                    'Values': [identifier],
                },
            ]
        response = client.describe_replication_tasks(**kwargs)
        if response and response.get('ReplicationTasks'):
            return response['ReplicationTasks']
        else:
            app.logger.error(f'Failed to get DMS replication tasks')
            return None
    except (ClientError, ConnectionError) as e:
        app.logger.error(f'Error retrieving DMS replication tasks (error={e})')
        return None


def list_endpoints():
    client = get_client()
    try:
        response = client.describe_endpoints()
        if response and response.get('Endpoints'):
            return response['Endpoints']
        else:
            app.logger.error(f'Failed to get DMS endpoints')
            return None
    except (ClientError, ConnectionError) as e:
        app.logger.error(f'Error retrieving DMS endpoints (error={e})')
        return None


def start_replication_task(identifier):
    client = get_client()
    task = get_replication_task(identifier)
    if not task or 'ReplicationTaskArn' not in task:
        app.logger.error(f'Could not find replication task matching identifier {identifier}, aborting')
        return
    task_arn = task['ReplicationTaskArn']
    try:
        response = client.start_replication_task(
            ReplicationTaskArn=task_arn,
            StartReplicationTaskType='reload-target',
        )
        if response and response.get('ReplicationTask', {}).get('Status') == 'starting':
            app.logger.info(f'Replication task started (id={identifier}, arn={task_arn})')
            return response
        else:
            app.logger.error(f'Failed to start replication tasks (id={identifier}, arn={task_arn}, response={response})')
            return None
    except (ClientError, ConnectionError) as e:
        app.logger.error(f'Error starting replication task (id={identifier}, arn={task_arn}, error={e})')
        return None
