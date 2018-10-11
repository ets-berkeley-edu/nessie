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


"""Logic for LRS incremental import job."""


from datetime import datetime
from time import sleep

from flask import current_app as app
from nessie.externals import dms, lrs, redshift, s3
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.util import localize_datetime, resolve_sql_template


class ImportLrsIncrementals(BackgroundJob):

    def run(self, truncate_lrs=False):
        app.logger.info('Starting DMS replication task...')
        task_id = app.config['LRS_INCREMENTAL_REPLICATION_TASK_ID']

        self.transient_bucket = app.config['LRS_INCREMENTAL_TRANSIENT_BUCKET']
        self.transient_path = app.config['LRS_INCREMENTAL_TRANSIENT_PATH']

        if not self.delete_old_incrementals():
            return False

        response = dms.start_replication_task(task_id)
        if not response:
            app.logger.error('Failed to start DMS replication task (response={response}).')
            return False

        while True:
            response = dms.get_replication_task(task_id)
            if response.get('Status') == 'stopped':
                if response.get('StopReason') == 'Stop Reason FULL_LOAD_ONLY_FINISHED':
                    app.logger.info('DMS replication task completed')
                    break
                else:
                    app.logger.error(f'Replication task stopped for unexpected reason: {response}')
                    return False
            sleep(10)

        lrs_response = lrs.fetch('select count(*) from statements')
        if lrs_response:
            self.lrs_statement_count = lrs_response[0][0]
        else:
            app.logger.error(f'Failed to retrieve LRS statements for comparison.')
            return False

        transient_keys = s3.get_keys_with_prefix(self.transient_path, bucket=self.transient_bucket)
        if not transient_keys:
            app.logger.error('Could not retrieve S3 keys from transient bucket.')
            return False

        transient_url = f's3://{self.transient_bucket}/{self.transient_path}'
        transient_schema = app.config['REDSHIFT_SCHEMA_LRS'] + '_transient'
        if not self.verify_migration(transient_url, transient_schema):
            return False
        redshift.drop_external_schema(transient_schema)

        timestamp_path = localize_datetime(datetime.now()).strftime('%Y/%m/%d/%H%M%S')
        destination_path = app.config['LRS_INCREMENTAL_DESTINATION_PATH'] + '/' + timestamp_path
        for destination_bucket in app.config['LRS_INCREMENTAL_DESTINATION_BUCKETS']:
            if not self.migrate_transient_to_destination(
                transient_keys,
                destination_bucket,
                destination_path,
                unload_to_etl=True,
            ):
                return False

        if truncate_lrs:
            if lrs.execute('TRUNCATE statements'):
                app.logger.info('Truncated incremental LRS table.')
            else:
                app.logger.error('Failed to truncate incremental LRS table.')
                return False

        return (
            f'Migrated {self.lrs_statement_count} statements to S3'
            f"(buckets={app.config['LRS_INCREMENTAL_DESTINATION_BUCKETS']}, path={destination_path})"
        )

    def delete_old_incrementals(self):
        old_incrementals = s3.get_keys_with_prefix(self.transient_path, bucket=self.transient_bucket)
        if old_incrementals is None:
            app.logger.error('Error listing old incrementals, aborting job.')
            return False
        if len(old_incrementals) > 0:
            delete_response = s3.delete_objects(old_incrementals, bucket=self.transient_bucket)
            if not delete_response:
                app.logger.error(f'Error deleting old incremental files from {self.transient_bucket}, aborting job.')
                return False
            else:
                app.logger.info(f'Deleted {len(old_incrementals)} old incremental files from {self.transient_bucket}.')
        return True

    def migrate_transient_to_destination(self, keys, destination_bucket, destination_path, unload_to_etl=False):
            destination_url = 's3://' + destination_bucket + '/' + destination_path
            destination_schema = app.config['REDSHIFT_SCHEMA_LRS']

            for transient_key in keys:
                destination_key = transient_key.replace(self.transient_path, destination_path)
                if not s3.copy(self.transient_bucket, transient_key, destination_bucket, destination_key):
                    app.logger.error(f'Copy from transient bucket to destination bucket {destination_bucket} failed.')
                    return False
            if not self.verify_migration(destination_url, destination_schema):
                return False
            if unload_to_etl:
                if not self.unload_to_etl(destination_schema, destination_bucket):
                    app.logger.error(f'Redshift statements unload from {destination_schema} to {destination_bucket} failed.')
                    return False
            redshift.drop_external_schema(destination_schema)
            return True

    def unload_to_etl(self, schema, bucket):
        timestamp_path = localize_datetime(datetime.now()).strftime('%Y/%m/%d/statements_%Y%m%d_%H%M%S_')
        credentials = ';'.join([
            f"aws_access_key_id={app.config['AWS_ACCESS_KEY_ID']}",
            f"aws_secret_access_key={app.config['AWS_SECRET_ACCESS_KEY']}",
        ])
        return redshift.execute(
            f"""
                UNLOAD ('SELECT statement FROM {schema}.statements')
                TO 's3://{bucket}/{app.config['LRS_INCREMENTAL_ETL_PATH_REDSHIFT']}/{timestamp_path}'
                CREDENTIALS '{credentials}'
                DELIMITER AS '  '
                NULL AS ''
                ALLOWOVERWRITE
                PARALLEL OFF
                MAXFILESIZE 1 gb
            """
        )

    def verify_migration(self, incremental_url, incremental_schema):
        redshift.drop_external_schema(incremental_schema)
        resolved_ddl_transient = resolve_sql_template(
            'create_lrs_schema.template.sql',
            redshift_schema_lrs_external=incremental_schema,
            loch_s3_lrs_statements_path=incremental_url,
        )
        if redshift.execute_ddl_script(resolved_ddl_transient):
            app.logger.info(f"LRS incremental schema '{incremental_schema}' created.")
        else:
            app.logger.error(f"LRS incremental schema '{incremental_schema}' creation failed.")
            return False

        redshift_response = redshift.fetch(f'select count(*) from {incremental_schema}.statements')
        if redshift_response:
            redshift_statement_count = redshift_response[0].get('count')
        else:
            app.logger.error(f"Failed to verify LRS incremental schema '{incremental_schema}'.")
            return False

        if redshift_statement_count == self.lrs_statement_count:
            app.logger.info(f'Verified {redshift_statement_count} rows migrated from LRS to {incremental_url}.')
            return True
        else:
            app.logger.error(
                f'Discrepancy between LRS ({self.lrs_statement_count} statements)'
                f'and {incremental_url} ({redshift_statement_count} statements).'
            )
            return False
