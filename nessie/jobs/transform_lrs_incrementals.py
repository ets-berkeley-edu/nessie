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

from time import sleep

from flask import current_app as app
from nessie.externals import glue, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError

"""Logic for transforming LRS Caliper statements."""


class TransformLrsIncrementals(BackgroundJob):

    def run(self):
        app.logger.info('Starting ETL process on LRS incrementals...')
        app.logger.info('Clear old exports in Glue destination location before running Caliper ETL process')

        self.transient_bucket = app.config['LRS_CANVAS_INCREMENTAL_TRANSIENT_BUCKET']
        self.transient_path = app.config['LRS_CANVAS_CALIPER_EXPLODE_OUTPUT_PATH']
        self.job_name = app.config['LRS_CANVAS_GLUE_JOB_NAME']
        self.glue_role = app.config['LRS_GLUE_SERVICE_ROLE']
        self.job_run_id = None
        self.job_run_response = None

        # Check Glue export location and clear any files from prior runs
        self.delete_old_incrementals()

        # Run glue tranformation jobs to explode canvas caliper data into flat files
        self.start_caliper_transform_job()

        # Add logic to write glue job details to redshift metadata table.
        return f'Successfully transformed and flattened the LRS canvas caliper incrmental feeds. JobRun={self.job_run_response}'

    def delete_old_incrementals(self):
        app.logger.debug(f' Bucket: {self.transient_bucket}')
        old_incrementals = s3.get_keys_with_prefix(self.transient_path, bucket=self.transient_bucket)
        if old_incrementals is None:
            raise BackgroundJobError('Error listing old incrementals, aborting job.')
        if len(old_incrementals) > 0:
            delete_response = s3.delete_objects(old_incrementals, bucket=self.transient_bucket)
            if delete_response is None:
                raise BackgroundJobError(f'Error deleting old incremental files from {self.transient_bucket}, aborting job.')
            else:
                app.logger.info(f'Deleted {len(old_incrementals)} old incremental files from {self.transient_bucket}.')

    def start_caliper_transform_job(self):
        job_arguments = {
            '--LRS_INCREMENTAL_TRANSIENT_BUCKET': app.config['LRS_CANVAS_INCREMENTAL_TRANSIENT_BUCKET'],
            '--LRS_CANVAS_CALIPER_SCHEMA_PATH': app.config['LRS_CANVAS_CALIPER_SCHEMA_PATH'],
            '--LRS_CANVAS_CALIPER_INPUT_DATA_PATH': app.config['LRS_CANVAS_CALIPER_INPUT_DATA_PATH'],
            '--LRS_GLUE_TEMP_DIR': app.config['LRS_GLUE_TEMP_DIR'],
            '--LRS_CANVAS_CALIPER_EXPLODE_OUTPUT_PATH': app.config['LRS_CANVAS_CALIPER_EXPLODE_OUTPUT_PATH'],
            '--job-bookmark-option': 'job-bookmark-disable',
        }

        response = glue.start_glue_job(
            self.job_name, job_arguments,
            app.config['LRS_CANVAS_GLUE_JOB_CAPACITY'],
            app.config['LRS_CANVAS_GLUE_JOB_TIMEOUT'],
        )
        if not response:
            raise BackgroundJobError('Failed to create Glue job')
        elif 'JobRunId' in response:
            self.job_run_id = response['JobRunId']
            app.logger.debug(f'Response : {response}')
            app.logger.info('Started job run successfully for the Job Name {} with Job Run id {}'.format(self.job_name, self.job_run_id))

            # Once the Caliper glue job is started successfully poll the job run every 30 seconds to get the status of the run
            while True:
                response = glue.check_job_run_status(self.job_name, self.job_run_id)
                if not response:
                    raise BackgroundJobError('Failed to check Glue job status')
                elif response['JobRun']['JobRunState'] == 'SUCCEEDED':
                    app.logger.info(f'Caliper glue transformation job completed successfully: {response}')
                    break
                elif response['JobRun']['JobRunState'] == 'FAILED' or response['JobRun']['JobRunState'] == 'TIMEOUT':
                    raise BackgroundJobError(f'Caliper glue transformation job failed or timed out: {response}')

                sleep(30)
            self.job_run_response = response
