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
import json

from flask import current_app as app
from nessie.externals import redshift, s3, sis_student_api
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.queries import get_all_student_ids
from nessie.lib.util import encoded_tsv_row, get_s3_sis_api_daily_path, resolve_sql_template_string

"""Logic for SIS student API import job."""


class ImportSisStudentApi(BackgroundJob):

    redshift_schema = app.config['REDSHIFT_SCHEMA_STUDENT']

    def run(self, csids=None):
        if not csids:
            csids = [row['sid'] for row in get_all_student_ids()]
        app.logger.info(f'Starting SIS student API import job for {len(csids)} students...')

        rows = []
        success_count = 0
        failure_count = 0
        index = 1
        for csid in csids:
            app.logger.info(f'Fetching SIS student API for SID {csid} ({index} of {len(csids)})')
            feed = sis_student_api.get_student(csid)
            if feed:
                success_count += 1
                rows.append(encoded_tsv_row([csid, json.dumps(feed)]))
            else:
                failure_count += 1
                app.logger.error(f'SIS student API import failed for CSID {csid}.')
            index += 1

        s3_key = f'{get_s3_sis_api_daily_path()}/profiles.tsv'
        app.logger.info(f'Will stash {success_count} feeds in S3: {s3_key}')
        if not s3.upload_tsv_rows(rows, s3_key):
            raise BackgroundJobError('Error on S3 upload: aborting job.')

        app.logger.info('Will copy S3 feeds into Redshift...')
        if not redshift.execute(f'TRUNCATE {self.redshift_schema}_staging.sis_api_profiles'):
            raise BackgroundJobError('Error truncating old staging rows: aborting job.')
        if not redshift.copy_tsv_from_s3(f'{self.redshift_schema}_staging.sis_api_profiles', s3_key):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')
        staging_to_destination_query = resolve_sql_template_string(
            """
            DELETE FROM {redshift_schema_student}.sis_api_profiles WHERE sid IN
                (SELECT sid FROM {redshift_schema_student}_staging.sis_api_profiles);
            INSERT INTO {redshift_schema_student}.sis_api_profiles
                (SELECT * FROM {redshift_schema_student}_staging.sis_api_profiles);
            TRUNCATE {redshift_schema_student}_staging.sis_api_profiles;
            """,
        )
        if not redshift.execute(staging_to_destination_query):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')

        return f'SIS student API import job completed: {success_count} succeeded, {failure_count} failed.'
