"""
Copyright ©2025. The Regents of the University of California (Regents). All Rights Reserved.

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

import csv
import io
import json
from datetime import datetime

from flask import current_app as app
from flask import request

from nessie.api.auth_helper import api_key_required
from nessie.api.errors import BadRequestError, InternalServerError
from nessie.externals import s3
from nessie.lib.http import tolerant_jsonify
from nessie.lib.util import (
    get_s3_asc_advising_notes_incremental_path,
    get_s3_coe_students_path,
    get_s3_slate_sftp_daily_path,
    localize_datetime,
    localized_datestamp,
)


@app.route('/api/upload/asc_advising_notes', methods=['POST'])
@api_key_required
def upload_asc_advising_notes():
    file = _get_uploaded_file()
    _verify_json(file)
    timestamp = localize_datetime(datetime.now())
    filename = f'asc_advising_notes_{timestamp.strftime("%Y%m%dT%H%M%S")}.json'
    s3_key = f'{get_s3_asc_advising_notes_incremental_path()}/{filename}'
    return _upload_to_all_buckets(file, s3_key, app.config['API_UPLOAD_BUCKETS'])


@app.route('/api/upload/coe_advisees', methods=['POST'])
@api_key_required
def upload_coe_advisees():
    file = _get_uploaded_file()
    _verify_delimited(file, '\t', 'TSV')
    path = get_s3_coe_students_path()
    filename = f'coe_student_adviser_{localized_datestamp()}.tsv'
    s3_key = f'{path}/{filename}'
    buckets = app.config['API_UPLOAD_BUCKETS']
    response = _upload_to_all_buckets(file, s3_key, buckets)
    # The COE external table reads the whole students/ directory, so files left behind would duplicate rows.
    _delete_keys_with_prefix(f'{path}/coe_student_adviser_', buckets, whitelist=(filename,))
    return response


@app.route('/api/upload/oua_admissions', methods=['POST'])
@api_key_required
def upload_oua_admissions():
    file = _get_uploaded_file()
    _verify_delimited(file, ',', 'CSV')
    timestamp = localize_datetime(datetime.now())
    filename = f'oua_admissions_{timestamp.strftime("%Y%m%dT%H%M%S")}.csv'
    s3_key = f'{get_s3_slate_sftp_daily_path(timestamp)}/{filename}'
    return _upload_to_all_buckets(file, s3_key, app.config['API_UPLOAD_BUCKETS_PROTECTED'])


def _get_uploaded_file():
    files = list(request.files.values())
    if len(files) != 1:
        raise BadRequestError('Request must include exactly one file.')
    return files[0]


def _verify_delimited(file, delimiter, file_type):
    try:
        rows = list(csv.reader(io.StringIO(file.read().decode('utf-8')), delimiter=delimiter))
    except (csv.Error, UnicodeDecodeError) as e:
        raise BadRequestError(f'Uploaded file is not valid {file_type}.') from e
    if not rows:
        raise BadRequestError('Uploaded file contains no rows.')


def _verify_json(file):
    try:
        json.loads(file.read())
    except json.JSONDecodeError as e:
        raise BadRequestError('Uploaded file is not valid JSON.') from e


def _upload_to_all_buckets(file, s3_key, buckets):
    if not buckets:
        raise InternalServerError('No API upload destination buckets configured.')
    for bucket in buckets:
        if not s3.upload_file(file, s3_key, bucket=bucket):
            raise InternalServerError(f'Failed to upload {s3_key} to bucket {bucket}')
    return tolerant_jsonify({'key': s3_key, 'buckets': buckets})


def _delete_keys_with_prefix(prefix, buckets, whitelist=()):
    success = True
    for bucket in buckets:
        existing_keys = s3.get_keys_with_prefix(prefix, bucket=bucket)
        if existing_keys is None:
            app.logger.error(f'Error listing keys with prefix "{prefix}" in bucket {bucket}')
            success = False
            continue
        keys_to_delete = [key for key in existing_keys if key.split('/')[-1] not in whitelist]
        app.logger.info(
            f'Found {len(existing_keys)} key(s) matching prefix "{prefix}" in bucket {bucket}, '
            f'{len(existing_keys) - len(keys_to_delete)} key(s) in whitelist, will delete {len(keys_to_delete)} object(s)')
        if keys_to_delete and not s3.delete_objects(keys_to_delete, bucket=bucket):
            app.logger.error(f'Error deleting keys with prefix "{prefix}" in bucket {bucket}')
            success = False
    return success
