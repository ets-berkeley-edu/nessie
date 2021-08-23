"""
Copyright ©2021. The Regents of the University of California (Regents). All Rights Reserved.

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

from datetime import datetime, timedelta
import json
import re

from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError, verify_external_schema
from nessie.lib.berkeley import reverse_term_ids
from nessie.lib.util import get_s3_sis_daily_path, resolve_sql_template

"""Logic for SIS schema creation job."""

external_schema = app.config['REDSHIFT_SCHEMA_SIS']
rds_schema = app.config['RDS_SCHEMA_SIS_INTERNAL']


class CreateSisSchema(BackgroundJob):

    def run(self):
        app.logger.info('Starting SIS schema creation job...')
        if not self.update_manifests():
            app.logger.info('Error updating manifests, will not execute schema creation SQL')
            return False
        app.logger.info('Executing SQL...')
        redshift.drop_external_schema(external_schema)
        resolved_ddl = resolve_sql_template('create_sis_schema.template.sql')
        if redshift.execute_ddl_script(resolved_ddl):
            verify_external_schema(external_schema, resolved_ddl, is_zero_count_acceptable=app.config['FEATURE_FLAG_EDL_SIS_VIEWS'])
        else:
            raise BackgroundJobError('SIS schema creation job failed.')
        return 'SIS schema creation job completed.'

    def update_manifests(self):
        app.logger.info('Updating manifests...')

        # Because the SIS S3 copy is managed by a different application running on a different schedule,
        # it may have been made before midnight by Nessie-time.
        s3_sis_daily = get_s3_sis_daily_path()
        if not s3.get_keys_with_prefix(s3_sis_daily):
            s3_sis_daily = get_s3_sis_daily_path(datetime.now() - timedelta(days=1))
            if not s3.get_keys_with_prefix(s3_sis_daily):
                raise BackgroundJobError('No timely SIS S3 data found')
            else:
                app.logger.info('Falling back to SIS S3 daily data for yesterday')

        courses_daily = s3.get_keys_with_prefix(s3_sis_daily + '/courses', full_objects=True)
        courses_historical = s3.get_keys_with_prefix(app.config['LOCH_S3_SIS_DATA_PATH'] + '/historical/courses', full_objects=True)
        enrollments_daily = s3.get_keys_with_prefix(s3_sis_daily + '/enrollments', full_objects=True)
        enrollments_historical = s3.get_keys_with_prefix(app.config['LOCH_S3_SIS_DATA_PATH'] + '/historical/enrollments', full_objects=True)

        def deduplicate(prefix, s3list):
            filename_map = {}
            for s3obj in s3list:
                m = re.match(r'.+/(.+\.gz)', s3obj['Key'])
                if m:
                    filename_map[m[1]] = s3obj
            for term_id in reverse_term_ids(include_future_terms=True):
                filename = f'{prefix}-{term_id}.gz'
                if filename not in filename_map:
                    raise BackgroundJobError(f'Expected filename {filename} not found in S3, aborting')
            return list(filename_map.values())

        all_courses = deduplicate('courses', courses_daily + courses_historical)
        all_enrollments = deduplicate('enrollments', enrollments_daily + enrollments_historical)

        def to_manifest_entry(_object):
            return {
                'url': f"s3://{app.config['LOCH_S3_BUCKET']}/{_object['Key']}",
                'meta': {'content_length': _object['Size']},
            }

        def to_manifest(objects):
            return {
                'entries': [to_manifest_entry(o) for o in objects],
            }

        courses_manifest = json.dumps(to_manifest(all_courses))
        enrollments_manifest = json.dumps(to_manifest(all_enrollments))
        courses_result = s3.upload_data(courses_manifest, app.config['LOCH_S3_SIS_DATA_PATH'] + '/manifests/courses.json')
        enrollments_result = s3.upload_data(enrollments_manifest, app.config['LOCH_S3_SIS_DATA_PATH'] + '/manifests/enrollments.json')
        return courses_result and enrollments_result
