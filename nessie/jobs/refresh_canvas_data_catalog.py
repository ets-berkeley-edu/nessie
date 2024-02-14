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


from datetime import datetime, timedelta

from flask import current_app as app
from nessie.externals import canvas_data, redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib import berkeley
from nessie.lib.util import get_s3_canvas_daily_path
import pandas as pd


"""Logic for generate canvas data catalog job."""


class RefreshCanvasDataCatalog(BackgroundJob):

    def run(self):
        # Retrieve latest schema definitions from Canvas data API
        response = canvas_data.get_canvas_data_schema()
        external_schema = app.config['REDSHIFT_SCHEMA_CANVAS']
        redshift_iam_role = app.config['REDSHIFT_IAM_ROLE']
        canvas_schema = []

        # Parse and isolate table and column details
        for key, value in response['schema'].items():
            for column in value['columns']:
                # Not every column has description and length.
                description = None
                if 'description' in column:
                    description = column['description']

                length = None
                if 'length' in column:
                    length = column['length']

                canvas_schema.append([
                    value['tableName'],
                    column['name'],
                    column['type'],
                    description,
                    length,
                ])
        # Create a dataframe
        schema_df = pd.DataFrame(canvas_schema)
        schema_df.columns = [
            'table_name',
            'column_name',
            'column_type',
            'column_description',
            'column_length',
        ]

        # The schema definitions received from Canvas are Redshift compliant. We update
        # cetain column types to match Glue and Spectrum data types.
        schema_df['glue_type'] = schema_df['column_type'].replace({
            'enum': 'varchar',
            'guid': 'varchar',
            'text': 'varchar(max)',
            'date': 'timestamp',
            'datetime': 'timestamp',
        })

        schema_df['transformed_column_name'] = schema_df['column_name'].replace({
                                                                                'default': '"default"',
                                                                                'percent': '"percent"',
                                                                                })
        # Create Hive compliant storage descriptors
        canvas_external_catalog_ddl = self.generate_external_catalog(external_schema, schema_df)

        # Clean up and recreate refreshed tables on Glue using Spectrum
        redshift.drop_external_schema(external_schema)
        redshift.create_external_schema(external_schema, redshift_iam_role)

        if redshift.execute_ddl_script(canvas_external_catalog_ddl):
            app.logger.info('Canvas schema creation job completed.')
        else:
            app.logger.error('Canvas schema creation job failed.')
            raise BackgroundJobError('Canvas schema creation job failed.')

        self.verify_external_data_catalog()
        return 'Canvas external schema created and verified.'

    def generate_external_catalog(self, external_schema, schema_df):
        canvas_path = self.generate_canvas_path()
        canvas_tables = schema_df.table_name.unique()
        s3_canvas_data_url = 's3://' + app.config['LOCH_S3_BUCKET'] + '/' + canvas_path
        s3_requests_url = 's3://{}/{}'.format(app.config['LOCH_S3_BUCKET'], berkeley.s3_canvas_data_path_current_term())
        external_table_ddl = ''

        for table in canvas_tables:
            table_columns = schema_df.loc[schema_df['table_name'] == table].reset_index()
            storage_descriptor_df = table_columns[['transformed_column_name', 'glue_type']]

            create_ddl = 'CREATE EXTERNAL TABLE {}.{}\n(\n'.format(external_schema, table)
            storage_descriptors = ''
            for index in storage_descriptor_df.index:
                storage_descriptors = '{}    {} {}'.format(
                    storage_descriptors,
                    storage_descriptor_df['transformed_column_name'][index],
                    storage_descriptor_df['glue_type'][index],
                )
                if (index != (len(storage_descriptor_df.index) - 1)):
                    storage_descriptors = storage_descriptors + ',\n'

            table_properties = '\n) \nROW FORMAT DELIMITED FIELDS \nTERMINATED BY \'\t\' \nSTORED AS TEXTFILE'
            if (table != 'requests'):
                table_location = '\nLOCATION \'{}/{}\''.format(s3_canvas_data_url, table)
            else:
                table_location = '\nLOCATION \'{}/{}\''.format(s3_requests_url, table)

            external_table_ddl = '{}\n{}{}{}{};\n\n'.format(
                external_table_ddl,
                create_ddl,
                storage_descriptors,
                table_properties,
                table_location,
            )

        # For debugging process, export to external_table_ddl to file to get a well formed SQL template for canvas-data
        return external_table_ddl

    # Gets an inventory of all the tables by tracking the S3 canvas-data daily location and run count verification to ensure migration was successful
    def verify_external_data_catalog(self):
        s3_client = s3.get_client()
        bucket = app.config['LOCH_S3_BUCKET']
        external_schema = app.config['REDSHIFT_SCHEMA_CANVAS']
        prefix = self.generate_canvas_path()
        app.logger.info(f'Daily path = {prefix}')
        directory_names = []
        s3_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for object_summary in s3_objects['Contents']:
            # parse table names from the S3 object URLs
            directory_names.append(object_summary['Key'].split('/')[3])

        # Get unique table names from S3 object list
        tables = sorted(list(set(directory_names)))
        # Ensure that all tables required by downstream jobs have data present in S3.
        required_tables = [
            'assignment_dim',
            'assignment_override_dim',
            'assignment_override_user_rollup_fact',
            'course_dim',
            'course_score_fact',
            'course_section_dim',
            'enrollment_dim',
            'enrollment_fact',
            'enrollment_term_dim',
            'pseudonym_dim',
            'submission_dim',
            'submission_fact',
            'user_dim',
        ]
        for required_table in required_tables:
            if required_table not in tables:
                raise BackgroundJobError(f'No data in S3 for external table {required_table}: aborting job.')

        app.logger.info(f'Tables to be verified : {tables}')
        for table in tables:
            result = redshift.fetch(f'SELECT COUNT(*) FROM {external_schema}.{table}')
            if result and result[0] and result[0]['count']:
                count = result[0]['count']
                app.logger.info(f'Verified external table {table} ({count} rows).')
            else:
                raise BackgroundJobError(f'Failed to verify external table {table}: aborting job.')
        app.logger.info(f'Canvas verification job completed successfully for {len(tables)} tables')
        return True

    def generate_canvas_path(self):
        canvas_path = get_s3_canvas_daily_path()
        if not s3.get_keys_with_prefix(canvas_path):
            canvas_path = get_s3_canvas_daily_path(datetime.now() - timedelta(days=1))
            if not s3.get_keys_with_prefix(canvas_path):
                raise BackgroundJobError('No timely Canvas data found, aborting')
            else:
                app.logger.info('Falling back to yesterday\'s Canvas data')
        return canvas_path
