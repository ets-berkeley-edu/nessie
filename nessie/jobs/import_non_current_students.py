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

from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.externals.boac import get_manually_added_advisees
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.util import get_s3_boa_api_daily_path, resolve_sql_template_string


class ImportNonCurrentStudents(BackgroundJob):

    def run(self):
        app.logger.info('Starting BOA manually added advisees import job...')
        feed = get_manually_added_advisees()

        if feed.get('error'):
            raise BackgroundJobError('Error on S3 upload: aborting job.')

        rows = [advisee['sid'].encode() for advisee in feed.get('feed')]
        s3_key = f'{get_s3_boa_api_daily_path()}/manually-added-advisees/manually-added-advisees.tsv'
        if not s3.upload_tsv_rows(rows, s3_key):
            raise BackgroundJobError('Error on S3 upload: aborting job.')

        app.logger.info('Copying data from S3 file to Redshift...')
        query = resolve_sql_template_string(
            """
            TRUNCATE {redshift_schema_advisee}.non_current_students;
            COPY {redshift_schema_advisee}.non_current_students
                FROM 's3://{s3_bucket}/{s3_key}'
                IAM_ROLE '{redshift_iam_role}'
                DELIMITER '\\t';
            """,
            s3_bucket=app.config['LOCH_S3_BUCKET'],
            s3_key=s3_key,
        )
        if not redshift.execute(query):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')

        status = f'Imported {len(rows)} non-current students.'
        app.logger.info(f'BOA manually added advisees import job completed: {status}')
        return status
