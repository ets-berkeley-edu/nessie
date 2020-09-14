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

from itertools import groupby
import json
import operator
from tempfile import TemporaryFile

from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError, verify_external_schema
from nessie.lib.util import get_s3_edl_daily_path, resolve_sql_template, resolve_sql_template_string, write_to_tsv_file

"""Logic for EDL SIS schema creation job."""


class CreateEdlSisSchema(BackgroundJob):

    external_schema = app.config['REDSHIFT_SCHEMA_EDL_SIS']
    internal_schema = app.config['REDSHIFT_SCHEMA_EDL_SIS_INTERNAL']

    def run(self):
        app.logger.info('Starting EDL SIS schema creation job...')
        self.create_schema()
        self.generate_feeds()
        return 'EDL SIS schema creation job completed.'

    def create_schema(self):
        app.logger.info('Executing SQL...')
        redshift.drop_external_schema(self.external_schema)
        resolved_ddl = resolve_sql_template('create_edl_sis_schema.template.sql')
        if redshift.execute_ddl_script(resolved_ddl):
            verify_external_schema(self.external_schema, resolved_ddl)
        else:
            raise BackgroundJobError('EDL SIS schema creation job failed.')
        app.logger.info('Redshift schema created.')

    def generate_feeds(self):
        self.stage_degree_progress()

    def stage_degree_progress(self):
        app.logger.error('Staging degree progress feeds...')
        table = 'student_degree_progress'
        rows = redshift.fetch(f'SELECT * FROM {self.internal_schema}.{table}_index ORDER by sid')
        with TemporaryFile() as feeds:
            for sid, rows_for_student in groupby(rows, operator.itemgetter('sid')):
                rows_for_student = list(rows_for_student)
                report_date = rows_for_student[0].get('report_date')
                feed = {
                    'reportDate': report_date.strftime('%Y-%m-%d'),
                    'requirements': {
                        row.get('requirement'): {
                            'name': row.get('requirement_desc'), 'status': row.get('status'),
                        } for row in rows_for_student
                    },
                }
                write_to_tsv_file(feeds, [sid, json.dumps(feed)])
            self._upload_file_to_staging(table, feeds)

    def _upload_file_to_staging(self, table, _file):
        tsv_filename = f'staging_{table}.tsv'
        s3_key = f'{get_s3_edl_daily_path()}/{tsv_filename}'

        app.logger.info(f'Will stash {table} feeds in S3: {s3_key}')
        if not s3.upload_file(_file, s3_key):
            raise BackgroundJobError('Error on S3 upload: aborting job.')

        app.logger.info('Will copy S3 feeds into Redshift...')
        query = resolve_sql_template_string(
            """
            COPY {schema}.{table}
                FROM '{loch_s3_edl_data_path_today}/{tsv_filename}'
                IAM_ROLE '{redshift_iam_role}'
                DELIMITER '\\t';
            """,
            schema=f'{self.internal_schema}',
            table=table,
            tsv_filename=tsv_filename,
        )
        if not redshift.execute(query):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')
