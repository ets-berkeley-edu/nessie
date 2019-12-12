"""
Copyright ©2020. The Regents of the University of California (Regents). All Rights Reserved.

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
from flask import current_app as app

"""Client code to run AWS Glue operations."""


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
        service_name='glue',
        region_name=app.config['LOCH_S3_REGION'],
        endpoint_url='https://glue.{}.amazonaws.com'.format(app.config['LOCH_S3_REGION']),
    )


def create_glue_job(name, glue_role, job_command_params, glue_job_args, allocated_capacity=2, timeout=20):
    client = get_client()
    response = client.create_job(
        Name=name,
        Role=glue_role,
        Command=job_command_params,
        DefaultArguments=glue_job_args,
        AllocatedCapacity=allocated_capacity,
        Timeout=timeout,
    )
    return response


def start_glue_job(glue_job_name, glue_job_args, glue_job_capacity, timeout):
    client = get_client()
    response = client.start_job_run(
        JobName=glue_job_name,
        Arguments=glue_job_args,
        AllocatedCapacity=glue_job_capacity,
        Timeout=timeout,
    )
    return response


def get_job(job_name):
    client = get_client()
    response = client.get_job(
        JobName=job_name,
    )
    return response


def get_jobs():
    client = get_client()
    response = client.get_jobs(
        MaxResults=20,
    )
    return response


def get_job_runs(job_name):
    client = get_client()
    response = client.get_job_runs(
        JobName=job_name,
        MaxResults=10,
    )
    return response


def check_job_run_status(job_name, job_run_id):
    client = get_client()
    response = client.get_job_run(
        JobName=job_name,
        RunId=job_run_id,
        PredecessorsIncluded=False,
    )
    return response


def batch_stop_job_runs(job_name, job_run_ids):
    client = get_client()
    response = client.batch_stop_job_run(
        JobName=job_name,
        JobRunIds=job_run_ids,
    )
    return response


def delete_glue_job(job_name):
    client = get_client()
    response = client.delete_job(JobName=job_name)
    return response


def update_glue_job(job_name, job_args):
    client = get_client()
    response = client.update_job(
        JobName=job_name,
        JobUpdate=job_args,
    )
    return response
