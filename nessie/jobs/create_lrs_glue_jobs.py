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


"""Logic for Creating LRS Caliper glue job to transform canvas caliper statements."""


from flask import current_app as app
from nessie.externals import glue
from nessie.jobs.background_job import BackgroundJob


class CreateLrsGlueJobs(BackgroundJob):

    def run(self):
        app.logger.info('Create Glue job to process on LRS incrementals')
        if self.create_lrs_caliper_relationalize_job():
            return True
        else:
            return False

    def create_lrs_caliper_relationalize_job(self):
        job_name = app.config['LRS_CANVAS_GLUE_JOB_NAME']
        glue_role = app.config['LRS_GLUE_SERVICE_ROLE']
        job_command = {
            'Name': 'glueetl',
            'ScriptLocation': 's3://{}/{}'.format(
                app.config['LRS_CANVAS_INCREMENTAL_TRANSIENT_BUCKET'],
                app.config['LRS_CANVAS_GLUE_JOB_SCRIPT_PATH'],
            ),
        }
        default_arguments = {
            '--LRS_INCREMENTAL_TRANSIENT_BUCKET': app.config['LRS_CANVAS_INCREMENTAL_TRANSIENT_BUCKET'],
            '--LRS_CANVAS_CALIPER_SCHEMA_PATH': app.config['LRS_CANVAS_CALIPER_SCHEMA_PATH'],
            '--LRS_CANVAS_CALIPER_INPUT_DATA_PATH': app.config['LRS_CANVAS_CALIPER_INPUT_DATA_PATH'],
            '--LRS_GLUE_TEMP_DIR': app.config['LRS_GLUE_TEMP_DIR'],
            '--LRS_CANVAS_CALIPER_EXPLODE_OUTPUT_PATH': app.config['LRS_CANVAS_CALIPER_EXPLODE_OUTPUT_PATH'],
            '--job-bookmark-option': 'job-bookmark-disable',
        }

        response = glue.create_glue_job(job_name, glue_role, job_command, default_arguments)
        if not response:
            app.logger.error('Failed to create Glue job')
            return False
        elif response['Name']:
            app.logger.info(f'Response : {response}')
            app.logger.info(f'Glue Job created successfully with Job Name : {response}')
            return True
        else:
            return False
