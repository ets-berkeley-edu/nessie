"""
Copyright ©2018. The Regents of the University of California (Regents). All Rights Reserved.

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

from datetime import datetime

from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.util import localize_datetime, resolve_sql_template

"""Logic for migrating LRS incrementals after transformation."""


class MigrateLrsIncrementals(BackgroundJob):

    def run(self):
        app.logger.info('Starting migration task on LRS incrementals...')

        self.transient_bucket = app.config['LRS_CANVAS_INCREMENTAL_TRANSIENT_BUCKET']
        self.get_pre_transform_statement_count()
        if not self.pre_transform_statement_count:
            app.logger.error(f'Failed to retrieve pre-transform statement count.')
            return False

        output_path = app.config['LRS_CANVAS_CALIPER_EXPLODE_OUTPUT_PATH']
        output_url = 's3://' + self.transient_bucket + '/' + output_path
        if not self.verify_post_transform_statement_count(output_url):
            app.logger.error(f'Failed to verify transformed statements at {output_url}.')
            return False

        etl_output_keys = s3.get_keys_with_prefix(output_path, bucket=self.transient_bucket)
        if not etl_output_keys:
            app.logger.error('Could not retrieve S3 keys from transient bucket.')
            return False

        timestamped_destination_path = output_path + '/' + localize_datetime(datetime.now()).strftime('%Y/%m/%d/%H%M%S')
        for destination_bucket in app.config['LRS_CANVAS_INCREMENTAL_DESTINATION_BUCKETS']:
            if not self.migrate_transient_to_destination(
                etl_output_keys,
                destination_bucket,
                timestamped_destination_path,
            ):
                return False

        return (
            f'Migrated {self.lrs_statement_count} statements to S3'
            f"(buckets={app.config['LRS_CANVAS_INCREMENTAL_DESTINATION_BUCKETS']}, path={timestamped_destination_path})"
        )

    def migrate_transient_to_destination(self, keys, destination_bucket, destination_path):
        destination_url = 's3://' + destination_bucket + '/' + destination_path
        for source_key in keys:
            destination_key = source_key.replace(self.transient_path, destination_path)
            if not s3.copy(self.transient_bucket, source_key, destination_bucket, destination_key):
                app.logger.error(f'Copy from transient bucket to destination bucket {destination_bucket} failed.')
                return False
        if not self.verify_post_transform_statement_count(destination_url):
            return False
        return True

    def get_pre_transform_statement_count(self):
        schema = app.config['REDSHIFT_SCHEMA_LRS']
        url = 's3://' + self.transient_bucket + '/' + app.config['LRS_CANVAS_CALIPER_INPUT_DATA_PATH']
        resolved_ddl_transient_unloaded = resolve_sql_template(
            'create_lrs_statements_unloaded_table.template.sql',
            redshift_schema_lrs_external=schema,
            loch_s3_lrs_statements_unloaded_path=url,
        )
        if redshift.execute_ddl_script(resolved_ddl_transient_unloaded):
            app.logger.info(f"statements_unloaded table created in schema '{schema}'.")
        else:
            app.logger.error(f"Failed to create statements_unloaded table in schema '{schema}'.")
            return False
        redshift_response = redshift.fetch(f'select count(*) from {schema}.statements_unloaded')
        if redshift_response:
            self.pre_transform_statement_count = redshift_response[0].get('count')
        else:
            app.logger.error('Failed to get pre-transform statement count.')
            return False

    def verify_post_transform_statement_count(self, url):
        schema = app.config['REDSHIFT_SCHEMA_LRS']
        resolved_ddl_transient = resolve_sql_template(
            'create_lrs_canvas_explode_table.template.sql',
            redshift_schema_lrs_external=schema,
            canvas_caliper_explode_table='caliper_statements_explode_transient',
            loch_s3_caliper_explode_path=url,
        )
        if redshift.execute_ddl_script(resolved_ddl_transient):
            app.logger.info(f"caliper_statements_explode_transient table created in schema '{schema}'.")
        else:
            app.logger.error(f"Failed to create caliper_statements_explode_transient table in schema '{schema}'.")
            return False

        exploded_statement_response = redshift.fetch(f'select count(*) from {schema}.caliper_statements_explode_transient')
        if exploded_statement_response:
            exploded_statement_count = exploded_statement_response[0].get('count')
        else:
            app.logger.error(f"Failed to verify caliper_statements_explode_transient table in schema '{schema}'.")
            return False

        if exploded_statement_count == self.pre_transform_statement_count:
            app.logger.info(f'Verified {exploded_statement_count} transformed statements migrated to {url}.')
            return True
        else:
            app.logger.error(
                f'Discrepancy between pre-transform statement count ({self.pre_transform_statement_count} statements)',
                f'and transformed statements at {url} ({exploded_statement_count} statements).',
            )
            return False
