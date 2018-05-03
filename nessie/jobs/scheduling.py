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


"""Background job scheduling."""


from apscheduler.schedulers.background import BackgroundScheduler


def initialize_job_schedules(app):
    from nessie.jobs.create_canvas_schema import CreateCanvasSchema
    from nessie.jobs.create_sis_schema import CreateSisSchema
    from nessie.jobs.generate_boac_analytics import GenerateBoacAnalytics
    from nessie.jobs.generate_intermediate_tables import GenerateIntermediateTables
    from nessie.jobs.sync_canvas_snapshots import SyncCanvasSnapshots

    if app.config['JOB_SCHEDULING_ENABLED']:
        sched = BackgroundScheduler()
        schedule_job(app, sched, 'JOB_SYNC_CANVAS_SNAPSHOTS', SyncCanvasSnapshots)
        schedule_chained_job(
            app, sched, 'JOB_GENERATE_ALL_TABLES', [
                CreateCanvasSchema,
                CreateSisSchema,
                GenerateIntermediateTables,
                GenerateBoacAnalytics,
            ],
        )
        sched.start()


def schedule_job(app, sched, config_value, job_class):
    job_schedule = app.config.get(config_value)
    if job_schedule:
        sched.add_job(start_background_job, 'cron', args=(app, job_class), **job_schedule)
        app.logger.info(f'Scheduled {job_class.__name__} job: {job_schedule}')


def start_background_job(app, job_class):
    app.logger.info(f'Starting scheduled {job_class.__name__} job')
    with app.app_context():
        job_class().run_async()


def schedule_chained_job(app, sched, config_value, job_components):
    job_schedule = app.config.get(config_value)
    if job_schedule:
        sched.add_job(start_chained_job, 'cron', args=(app, job_components), **job_schedule)
        app.logger.info(f'Scheduled chained background job: {job_schedule}, ' + ', '.join([c.__name__ for c in job_components]))


def start_chained_job(app, job_components):
    from nessie.jobs.background_job import ChainedBackgroundJob
    app.logger.info(f'Starting chained background job: ' + ', '.join([c.__name__ for c in job_components]))
    with app.app_context():
        initialized_components = [c() for c in job_components]
        ChainedBackgroundJob(steps=initialized_components).run_async()
