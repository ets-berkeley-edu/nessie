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

import os

from flask import Flask
from nessie import db
from nessie.configs import load_configs
from nessie.jobs.queue import initialize_job_queue
from nessie.jobs.scheduling import initialize_job_schedules, run_startup_jobs
from nessie.logger import initialize_logger
from nessie.routes import register_routes


def create_app():
    """Initialize app with configs."""
    app = Flask(__name__.split('.')[0])

    load_configs(app)
    initialize_logger(app)
    configure_scheduler_mode(app)
    db.init_app(app)

    with app.app_context():
        register_routes(app)

        # See https://stackoverflow.com/questions/9449101/how-to-stop-flask-from-initialising-twice-in-debug-mode
        if not app.debug or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
            run_startup_jobs(app)
            initialize_job_schedules(app)
            initialize_job_queue(app)

    return app


def configure_scheduler_mode(app):
    """Determine whether this app instance is running as 'master' or 'worker'."""
    default_scheduler_mode = app.config['JOB_SCHEDULING_ENABLED']
    eb_environment = os.environ.get('EB_ENVIRONMENT')
    if eb_environment is not None:
        if 'worker' in eb_environment:
            override_mode = False
        elif 'master' in eb_environment:
            override_mode = True
            if app.config['WORKER_QUEUE_ENABLED']:
                app.logger.info('Changing WORKER_QUEUE_ENABLED to False')
                app.config['WORKER_QUEUE_ENABLED'] = False
        else:
            override_mode = None
        if override_mode is not None and override_mode is not default_scheduler_mode:
            app.logger.info(f'Changing JOB_SCHEDULING_ENABLED config to {override_mode}')
            app.config['JOB_SCHEDULING_ENABLED'] = override_mode
