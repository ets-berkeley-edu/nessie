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

import os
import re
from threading import Thread
import time
import traceback

from flask import current_app as app
from nessie.externals import redshift
from nessie.jobs.queue import get_job_queue
from nessie.lib.berkeley import send_system_error_email
from nessie.lib.metadata import create_background_job_status, most_recent_background_job_status, update_background_job_status
from nessie.models.util import advisory_lock

"""Parent class for background jobs."""


def verify_external_schema(schema, resolved_ddl, is_zero_count_acceptable=False):
    pattern = f'CREATE EXTERNAL TABLE ({schema}\.\w+)'
    external_tables = re.findall(pattern, resolved_ddl)
    for table in external_tables:
        # The historical request tables are huge, rarely updated, and due to move to their own bucket:
        # skip verification on them for now.
        if 'historical_requests' in table:
            continue
        result = redshift.fetch(f'SELECT COUNT(*) FROM {table}')
        count = result and result[0] and result[0]['count']
        verified = count is not None and (is_zero_count_acceptable or count)
        description = f'external table {table} with {count} rows (zero_count_acceptable: {is_zero_count_acceptable}).'
        if verified:
            app.logger.info(f'Verified {description}')
        else:
            raise BackgroundJobError(f'Failed to verify {description}')


class BackgroundJob(object):

    status_logging_enabled = True
    condemn_stalled_jobs_to_failure = True

    def __init__(self, **kwargs):
        self.job_args = kwargs
        self.job_id = self.generate_job_id()

    @classmethod
    def generate_job_id(cls):
        return '_'.join([cls.__name__, str(int(time.time()))])

    def run(self, **kwargs):
        pass

    def run_async(self, lock_id=None, **async_opts):
        if os.environ.get('NESSIE_ENV') in ['test', 'testext']:
            app.logger.info('Test run in progress; will not muddy the waters by actually kicking off a background thread.')
            return True
        queue = get_job_queue()
        if queue:
            app.logger.info(f'Current queue size {queue.qsize()}; adding new job.')
            queue.put(self)
            return True
        # If no queue is enabled, start a new thread.
        else:
            app.logger.info('About to start background thread.')
            app_arg = app._get_current_object()
            self.job_args.update(async_opts)
            kwargs = self.job_args
            if lock_id:
                kwargs['lock_id'] = lock_id
            thread = Thread(target=self.run_in_app_context, args=[app_arg], kwargs=kwargs, daemon=True)
            thread.start()
            return True

    def run_in_app_context(self, app_arg, **kwargs):
        with app_arg.app_context():
            self.run_wrapped(**kwargs)

    def run_wrapped(self, **kwargs):
        lock_id = kwargs.pop('lock_id', None)
        with advisory_lock(lock_id):
            if self.status_logging_enabled:
                if self.condemn_stalled_jobs_to_failure:
                    check_for_stalled_job(type(self).__name__)
                create_background_job_status(self.job_id)
            try:
                error = None
                result = self.run(**kwargs)
            except BackgroundJobError as e:
                app.logger.error(e)
                result = None
                error = f'{str(e)}\n\n<pre>{traceback.format_exc()}</pre>'
            except Exception as e:
                app.logger.exception(e)
                result = None
                error = f'{str(e)}\n\n<pre>{traceback.format_exc()}</pre>'
            if self.status_logging_enabled:
                if result:
                    status = 'succeeded'
                    if isinstance(result, str):
                        app.logger.info(result)
                        details = result
                    else:
                        details = None
                else:
                    status = 'failed'
                    details = error
                    send_system_error_email(
                        message=f"""
                            job_id: {self.job_id}

                            job_args: {self.job_args}

                            {details or 'No details.'}
                        """,
                        subject=f'Job failure: {type(self).__name__}',
                    )
                update_background_job_status(self.job_id, status, details=details)
            return result


class ChainedBackgroundJob(BackgroundJob):

    def run(self, steps):
        for step in steps:
            if not step.run_wrapped():
                app.logger.error('Component job returned an error; aborting remainder of chain.')
                return False
        return True


class BackgroundJobError(Exception):
    pass


def check_for_stalled_job(job_name):
    last_job = most_recent_background_job_status(job_name)
    if last_job and last_job['status'] == 'started':
        send_system_error_email(
            message=f"""
                job_id: {last_job['job_id']}

                {'Timed out.'}
            """,
            subject=f'Job timeout: {job_name}',
        )
        update_background_job_status(last_job['job_id'], 'failed', details='Timed out.')
