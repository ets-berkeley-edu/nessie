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


from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from nessie.lib.berkeley import current_term_id


app = None
sched = None


# Postgres advisory locks require numeric ids.
PG_ADVISORY_LOCK_IDS = {
    'JOB_SYNC_CANVAS_SNAPSHOTS': 1000,
    'JOB_RESYNC_CANVAS_SNAPSHOTS': 1500,
    'JOB_IMPORT_STUDENT_POPULATION': 2000,
    'JOB_IMPORT_DEGREE_PROGRESS': 2500,
    'JOB_IMPORT_SIS_ENROLLMENTS': 2600,
    'JOB_IMPORT_SIS_STUDENTS': 2700,
    'JOB_GENERATE_ALL_TABLES': 3000,
    'JOB_GENERATE_CURRENT_TERM_FEEDS': 3500,
    'JOB_REFRESH_BOAC_CACHE': 4000,
}


def get_scheduler():
    return sched


def initialize_job_schedules(_app, force=False):
    from nessie.jobs.create_calnet_schema import CreateCalNetSchema
    from nessie.jobs.create_canvas_schema import CreateCanvasSchema
    from nessie.jobs.create_coe_schema import CreateCoeSchema
    from nessie.jobs.create_sis_schema import CreateSisSchema
    from nessie.jobs.generate_asc_profiles import GenerateAscProfiles
    from nessie.jobs.generate_boac_analytics import GenerateBoacAnalytics
    from nessie.jobs.generate_intermediate_tables import GenerateIntermediateTables
    from nessie.jobs.generate_merged_student_feeds import GenerateMergedStudentFeeds
    from nessie.jobs.import_asc_athletes import ImportAscAthletes
    from nessie.jobs.import_calnet_data import ImportCalNetData
    from nessie.jobs.import_degree_progress import ImportDegreeProgress
    from nessie.jobs.import_sis_enrollments_api import ImportSisEnrollmentsApi
    from nessie.jobs.import_sis_student_api import ImportSisStudentApi
    from nessie.jobs.refresh_boac_cache import RefreshBoacCache
    from nessie.jobs.resync_canvas_snapshots import ResyncCanvasSnapshots
    from nessie.jobs.sync_canvas_snapshots import SyncCanvasSnapshots

    global app
    app = _app

    global sched
    if app.config['JOB_SCHEDULING_ENABLED']:
        db_jobstore = SQLAlchemyJobStore(url=app.config['SQLALCHEMY_DATABASE_URI'], tablename='apscheduler_jobs')
        sched = BackgroundScheduler(jobstores={'default': db_jobstore})
        sched.start()
        schedule_job(sched, 'JOB_SYNC_CANVAS_SNAPSHOTS', SyncCanvasSnapshots, force)
        schedule_job(sched, 'JOB_RESYNC_CANVAS_SNAPSHOTS', ResyncCanvasSnapshots, force)
        schedule_chained_job(
            sched,
            'JOB_IMPORT_STUDENT_POPULATION',
            [
                CreateCoeSchema,
                ImportAscAthletes,
                GenerateAscProfiles,
                ImportCalNetData,
                CreateCalNetSchema,
            ],
            force,
        )
        schedule_job(sched, 'JOB_IMPORT_DEGREE_PROGRESS', ImportDegreeProgress, force)
        schedule_job(sched, 'JOB_IMPORT_SIS_ENROLLMENTS', ImportSisEnrollmentsApi, force)
        schedule_job(sched, 'JOB_IMPORT_SIS_STUDENTS', ImportSisStudentApi, force)
        schedule_chained_job(
            sched,
            'JOB_GENERATE_ALL_TABLES',
            [
                CreateCanvasSchema,
                CreateSisSchema,
                GenerateIntermediateTables,
                GenerateBoacAnalytics,
            ],
            force,
        )
        schedule_job(sched, 'JOB_GENERATE_CURRENT_TERM_FEEDS', GenerateMergedStudentFeeds, force, term_id=current_term_id())
        schedule_job(sched, 'JOB_REFRESH_BOAC_CACHE', RefreshBoacCache, force)


def add_job(sched, job_func, job_arg, job_id, force=False, **job_opts):
    job_schedule = app.config.get(job_id)
    if job_schedule:
        existing_job = sched.get_job(job_id)
        if existing_job and (force is False):
            app.logger.info(f'Found existing cron trigger for job {job_id}, will not reschedule: {existing_job.next_run_time}')
            return False
        else:
            sched.add_job(job_func, 'cron', args=(job_arg, job_id, job_opts), id=job_id, replace_existing=True, **job_schedule)
            return job_schedule


def schedule_job(sched, job_id, job_class, force=False, **job_opts):
    job_schedule = add_job(sched, start_background_job, job_class, job_id, force, **job_opts)
    if job_schedule:
        app.logger.info(f'Scheduled {job_class.__name__} job: {job_schedule}')


def schedule_chained_job(sched, job_id, job_components, force=False):
    job_schedule = add_job(sched, start_chained_job, job_components, job_id, force)
    if job_schedule:
        app.logger.info(f'Scheduled chained background job: {job_schedule}, ' + ', '.join([c.__name__ for c in job_components]))


def start_background_job(job_class, job_id, job_opts={}):
    job_opts['lock_id'] = PG_ADVISORY_LOCK_IDS[job_id]
    app.logger.info(f'Starting scheduled {job_class.__name__} job')
    with app.app_context():
        job_class().run_async(**job_opts)


def start_chained_job(job_components, job_id, job_opts={}):
    from nessie.jobs.background_job import ChainedBackgroundJob
    job_opts['lock_id'] = PG_ADVISORY_LOCK_IDS[job_id]
    app.logger.info(f'Starting chained background job: ' + ', '.join([c.__name__ for c in job_components]))
    with app.app_context():
        initialized_components = [c() for c in job_components]
        ChainedBackgroundJob(steps=initialized_components).run_async(**job_opts)


def run_startup_jobs(_app):
    # Jobs to be run in the foreground on app startup.
    from nessie.jobs.create_asc_schema import CreateAscSchema
    from nessie.jobs.create_metadata_schema import CreateMetadataSchema
    from nessie.jobs.create_student_schema import CreateStudentSchema

    if _app.config['JOB_SCHEDULING_ENABLED']:
        _app.logger.info('Checking for required schemas...')
        CreateAscSchema().run()
        CreateMetadataSchema().run()
        CreateStudentSchema().run()
