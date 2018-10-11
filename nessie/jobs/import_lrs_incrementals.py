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


from time import sleep

from flask import current_app as app
from nessie.externals import dms, lrs, redshift, s3
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.util import localized_datestamp, resolve_sql_template


class ImportLrsIncrementals(BackgroundJob):

    def run(self):
        app.logger.info('Starting DMS replication task...')
        task_id = app.config['LRS_INCREMENTAL_REPLICATION_TASK_ID']
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
            lrs_statement_count = lrs_response[0][0]
        else:
            app.logger.error(f'Failed to retrieve LRS statements for comparison.')
            return False

        transient_bucket = app.config['LRS_INCREMENTAL_TRANSIENT_BUCKET']
        transient_path = app.config['LRS_INCREMENTAL_TRANSIENT_PATH']
        transient_url = f's3://{transient_bucket}/{transient_path}'
        transient_keys = s3.get_keys_with_prefix(transient_path, bucket=transient_bucket)
        if not transient_keys:
            app.logger.error('Could not retrieve S3 keys from transient bucket.')
            return False

        transient_schema = app.config['REDSHIFT_SCHEMA_LRS'] + '_transient'
        if not self.verify_migration(transient_url, transient_schema, lrs_statement_count):
            return False
        redshift.drop_external_schema(transient_schema)

        destination_path = app.config['LRS_INCREMENTAL_DESTINATION_PATH'] + '/' + localized_datestamp()
        for destination_bucket in app.config['LRS_INCREMENTAL_DESTINATION_BUCKETS']:
            destination_url = 's3://' + destination_bucket + '/' + destination_path
            destination_schema = app.config['REDSHIFT_SCHEMA_LRS']

            for transient_key in transient_keys:
                destination_key = transient_key.replace(transient_path, destination_path)
                if not s3.copy(transient_bucket, transient_key, destination_bucket, destination_key):
                    app.logger.error(f'Copy from transient bucket to destination bucket {destination_bucket} failed.')
                    return False
            if not self.verify_migration(destination_url, destination_schema, lrs_statement_count):
                return False
            redshift.drop_external_schema(destination_schema)

        if lrs.execute('TRUNCATE STATEMENTS'):
            app.logger.info('Truncated incremental LRS table.')
        else:
            app.logger.error('Failed to truncate incremental LRS table.')
            return False

        return (
            f'Migrated {lrs_statement_count} statements to S3'
            f"(buckets={app.config['LRS_INCREMENTAL_DESTINATION_BUCKETS']}, path={destination_path})"
        )

    def verify_migration(self, incremental_url, incremental_schema, lrs_statement_count):
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

        if redshift_statement_count == lrs_statement_count:
            app.logger.info(f'Verified {redshift_statement_count} rows migrated from LRS to {incremental_url}.')
            return True
        else:
            app.logger.error(
                f'Discrepancy between LRS ({lrs_statement_count} statements)'
                f'and {incremental_url} ({redshift_statement_count} statements).'
            )
            return False
