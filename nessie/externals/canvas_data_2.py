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

from base64 import b64encode

from flask import current_app as app
from nessie.externals import secrets_manager
import requests


def get_cd2_secret():
    secret = secrets_manager.get_secret(app.config['CD2_SECRET_NAME'])
    return secret


def get_cd2_access_token():
    secret = get_cd2_secret()
    creds = f'{secret["DAP_CLIENT_ID"]}:{secret["DAP_CLIENT_SECRET"]}'
    header = {'Authorization': 'Basic ' + b64encode(creds.encode()).decode()}
    auth_url = f'{secret["DAP_API_URL"]}/ids/auth/login'
    body = {'grant_type': 'client_credentials'}
    # Request access token from the API
    response = requests.post(auth_url, headers=header, data=body)
    response_data = response.json()
    access_token = response_data.get('access_token')

    return access_token


def get_cd2_tables_list(namespace):
    secret = get_cd2_secret()
    access_token = get_cd2_access_token()
    headers = {'x-instauth': access_token}
    # Get a list of available tables for the namespace. Namespace values can be 'canvas', 'catalog', 'canvas_logs'
    cd2_tables_list = requests.get(f'{secret["DAP_API_URL"]}/dap/query/{namespace}/table', headers=headers)
    app.logger.info(f'Tables Available for {namespace}: {len(cd2_tables_list.json()["tables"])}')

    return cd2_tables_list.json()['tables']


def query_table_data(access_token, secret, table):
    headers = {'x-instauth': access_token}
    body = {'format': 'tsv', 'mode': 'expanded'}
    # Make request to initiate job for querying table data and get job request ID
    response = requests.post(f"{secret['DAP_API_URL']}/dap/query/canvas/table/{table}/data", headers=headers, json=body)
    response_data = response.json()
    job_request_id = response_data.get('id')

    if not job_request_id:
        app.logger.error(f'Error: Invalid job request ID received for table {table}.Response: {response.text}.')
        return None

    return job_request_id


def start_query_snapshot(tables):
    secret = get_cd2_secret()
    access_token = get_cd2_access_token()
    table_query_jobs = []
    for table in tables:
        app.logger.info(f'Querying for table {table} \n')

        job_request_id = query_table_data(access_token, secret, table)

        table_query_jobs.append({
            'table': table,
            'job_id': job_request_id,
            'job_status': 'running',
        })

    app.logger.info('Successfully began query snapshot jobs for all tables and retrieved job_id for tracking.')
    return table_query_jobs


def get_job_status(secret, headers, job_request_id):
    job_status_url = f'{secret["DAP_API_URL"]}/dap/job/{job_request_id}'
    app.logger.debug(f'Job status url: {job_status_url}')

    job_status_response = requests.get(job_status_url, headers=headers)
    if job_status_response.json().get('status') == 'complete':
        app.logger.debug(job_status_response.text)

    return job_status_response


def get_cd2_file_urls(secret, headers, file_objects):

    app.logger.info(f'Retriving presigned file urls for the table objects {file_objects}')
    file_urls = requests.post(f'{secret["DAP_API_URL"]}/dap/object/url', headers=headers, json=file_objects)

    return file_urls
