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
from threading import Thread
from time import sleep

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler

"""Background job scheduling."""


app = None
sched = None


# Postgres advisory locks require numeric ids.
PG_ADVISORY_LOCK_IDS = {
    'JOB_SYNC_CANVAS_SNAPSHOTS': 1000,
    'JOB_RESYNC_CANVAS_SNAPSHOTS': 1500,
    'JOB_IMPORT_ADVISORS': 1800,
    'JOB_IMPORT_ADMISSIONS': 1850,
    'JOB_IMPORT_SIS_DATA': 1900,
    'JOB_IMPORT_STUDENT_POPULATION': 2000,
    'JOB_IMPORT_DEGREE_PROGRESS': 2500,
    'JOB_IMPORT_HIST_ENR': 2600,
    'JOB_IMPORT_SIS_STUDENTS': 2700,
    'JOB_IMPORT_REGISTRATIONS': 2800,
    'JOB_IMPORT_CANVAS_ENROLLMENTS': 2900,
    'JOB_GENERATE_CANVAS_CALIPER_ANALYTICS': 2950,
    'JOB_GENERATE_ALL_TABLES': 3000,
    'JOB_GENERATE_CURRENT_TERM_FEEDS': 3500,
    'JOB_LOAD_LRS_INCREMENTALS': 3800,
    'JOB_LOAD_ADVISING_NOTES': 4000,
    'JOB_IMPORT_PIAZZA_API': 6000,
    'JOB_TRANSFORM_PIAZZA_DATA': 6050,
    'JOB_IMPORT_EDL': 7000,
    'JOB_IMPORT_YCBM': 7500,
}


def get_scheduler():
    return sched


def initialize_job_schedules(_app, force=False):
    global app
    app = _app

    global sched
    if app.config['JOB_SCHEDULING_ENABLED']:
        db_jobstore = SQLAlchemyJobStore(url=app.config['SQLALCHEMY_DATABASE_URI'], tablename='apscheduler_jobs')
        sched = BackgroundScheduler(jobstores={'default': db_jobstore})
        sched.start()
        schedule_all_jobs(force)
    if app.config['WORKER_QUEUE_ENABLED']:
        start_worker_listener()


def schedule_all_jobs(force=False):
    from nessie.jobs.chained_import_student_population import ChainedImportStudentPopulation
    from nessie.jobs.create_advisor_schema import CreateAdvisorSchema
    from nessie.jobs.create_asc_advising_notes_schema import CreateAscAdvisingNotesSchema
    from nessie.jobs.create_e_i_advising_notes_schema import CreateEIAdvisingNotesSchema
    from nessie.jobs.create_edl_schema import CreateEdlSchema
    from nessie.jobs.create_oua_schema import CreateOUASchema
    from nessie.jobs.create_sis_advising_notes_schema import CreateSisAdvisingNotesSchema
    from nessie.jobs.create_sis_schema import CreateSisSchema
    from nessie.jobs.create_ycbm_schema import CreateYcbmSchema
    from nessie.jobs.generate_boac_analytics import GenerateBoacAnalytics
    from nessie.jobs.generate_intermediate_tables import GenerateIntermediateTables
    from nessie.jobs.generate_merged_hist_enr_feeds import GenerateMergedHistEnrFeeds
    from nessie.jobs.generate_merged_student_feeds import GenerateMergedStudentFeeds
    from nessie.jobs.import_canvas_enrollments_api import ImportCanvasEnrollmentsApi
    from nessie.jobs.import_degree_progress import ImportDegreeProgress
    from nessie.jobs.import_piazza_api_data import ImportPiazzaApiData
    from nessie.jobs.import_sis_student_api import ImportSisStudentApi
    from nessie.jobs.import_sis_student_api_hist_enr import ImportSisStudentApiHistEnr
    from nessie.jobs.import_registrations import ImportRegistrations
    from nessie.jobs.import_registrations_hist_enr import ImportRegistrationsHistEnr
    from nessie.jobs.import_ycbm_api import ImportYcbmApi
    from nessie.jobs.index_advising_notes import IndexAdvisingNotes
    from nessie.jobs.index_enrollments import IndexEnrollments
    from nessie.jobs.migrate_sis_advising_note_attachments import MigrateSisAdvisingNoteAttachments
    from nessie.jobs.refresh_boac_cache import RefreshBoacCache
    from nessie.jobs.refresh_canvas_data_catalog import RefreshCanvasDataCatalog
    from nessie.jobs.resync_canvas_snapshots import ResyncCanvasSnapshots
    from nessie.jobs.sync_canvas_snapshots import SyncCanvasSnapshots
    from nessie.jobs.transform_piazza_api_data import TransformPiazzaApiData
    from nessie.jobs.verify_sis_advising_note_attachments import VerifySisAdvisingNoteAttachments

    schedule_job(sched, 'JOB_SYNC_CANVAS_SNAPSHOTS', SyncCanvasSnapshots, force)
    schedule_job(sched, 'JOB_RESYNC_CANVAS_SNAPSHOTS', ResyncCanvasSnapshots, force)
    schedule_job(sched, 'JOB_IMPORT_ADVISORS', CreateAdvisorSchema, force)
    schedule_job(sched, 'JOB_IMPORT_ADMISSIONS', CreateOUASchema, force)
    schedule_job(sched, 'JOB_IMPORT_STUDENT_POPULATION', ChainedImportStudentPopulation, force)
    schedule_job(sched, 'JOB_IMPORT_DEGREE_PROGRESS', ImportDegreeProgress, force)
    schedule_chained_job(
        sched,
        'JOB_IMPORT_HIST_ENR',
        [
            ImportSisStudentApiHistEnr,
            ImportRegistrationsHistEnr,
            GenerateMergedHistEnrFeeds,
        ],
        force,
    )
    schedule_job(sched, 'JOB_IMPORT_SIS_STUDENTS', ImportSisStudentApi, force)
    schedule_job(sched, 'JOB_IMPORT_REGISTRATIONS', ImportRegistrations, force, load_mode='batch')
    schedule_job(sched, 'JOB_IMPORT_SIS_DATA', CreateSisSchema, force)
    schedule_job(sched, 'JOB_IMPORT_CANVAS_ENROLLMENTS', ImportCanvasEnrollmentsApi, force)
    schedule_chained_job(
        sched,
        'JOB_GENERATE_ALL_TABLES',
        [
            RefreshCanvasDataCatalog,
            CreateSisSchema,
            GenerateIntermediateTables,
            IndexEnrollments,
            GenerateBoacAnalytics,
        ],
        force,
    )
    schedule_chained_job(
        sched,
        'JOB_GENERATE_CURRENT_TERM_FEEDS',
        [
            GenerateMergedStudentFeeds,
            IndexEnrollments,
            RefreshBoacCache,
        ],
        force,
    )
    schedule_chained_job(
        sched,
        'JOB_LOAD_ADVISING_NOTES',
        [
            CreateAscAdvisingNotesSchema,
            CreateEIAdvisingNotesSchema,
            CreateSisAdvisingNotesSchema,
            IndexAdvisingNotes,
            MigrateSisAdvisingNoteAttachments,
            VerifySisAdvisingNoteAttachments,
        ],
        force,
    )
    schedule_job(sched, 'JOB_IMPORT_PIAZZA_API', ImportPiazzaApiData, force)
    schedule_job(sched, 'JOB_TRANSFORM_PIAZZA_DATA', TransformPiazzaApiData, force)
    schedule_job(sched, 'JOB_IMPORT_EDL', CreateEdlSchema, force)
    schedule_chained_job(
        sched,
        'JOB_IMPORT_YCBM',
        [
            ImportYcbmApi,
            CreateYcbmSchema,
        ],
        force,
    )


def add_job(sched, job_func, job_arg, job_id, force=False, **job_opts):
    job_schedule = app.config.get(job_id)
    if job_schedule is not None:
        existing_job = sched.get_job(job_id)
        if existing_job and (force is False):
            app.logger.info(f'Found existing cron trigger for job {job_id}, will not reschedule: {existing_job.next_run_time}')
            return False
        else:
            # An empty hash in configs will add the job to the scheduler as paused.
            if job_schedule == {}:
                job_schedule = {'next_run_time': None}
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
    app.logger.info('Starting chained background job: ' + ', '.join([c.__name__ for c in job_components]))
    with app.app_context():
        initialized_components = [c() for c in job_components]
        ChainedBackgroundJob(steps=initialized_components).run_async(**job_opts)


def run_startup_jobs(_app):
    # Jobs to be run in the foreground on app startup.
    from nessie.jobs.create_advisee_schema import CreateAdviseeSchema
    from nessie.jobs.create_asc_schema import CreateAscSchema
    from nessie.jobs.create_metadata_schema import CreateMetadataSchema
    from nessie.jobs.create_rds_indexes import CreateRdsIndexes
    from nessie.jobs.create_student_schema import CreateStudentSchema
    from nessie.jobs.create_terms_schema import CreateTermsSchema

    if _app.config['JOB_SCHEDULING_ENABLED'] and os.environ.get('NESSIE_ENV') != 'test':
        _app.logger.info('Checking for required schemas...')
        CreateAdviseeSchema().run()
        CreateAscSchema().run()
        CreateMetadataSchema().run()
        CreateRdsIndexes().run()
        CreateTermsSchema().run()
        CreateStudentSchema().run()


def start_worker_listener():
    listener_thread = Thread(target=listen_for_merged_enrollment_term_jobs, args=[app], daemon=True)
    listener_thread.start()


def listen_for_merged_enrollment_term_jobs(_app):
    from nessie.jobs.background_job import BackgroundJobError
    from nessie.jobs.generate_merged_enrollment_term import GenerateMergedEnrollmentTerm
    from nessie.lib.metadata import poll_merged_enrollment_term_job_queue, update_merged_enrollment_term_job_status
    with _app.app_context():
        _app.logger.info('Listening for merged enrollment term jobs.')
        while True:
            sleep(5)
            args = poll_merged_enrollment_term_job_queue()
            if args:
                _app.logger.info(f"Starting queued merged enrollment term job (master_job_id={args['master_job_id']}, term_id={args['term_id']}.")
                try:
                    job = GenerateMergedEnrollmentTerm()
                    error = None
                    result = job.run(term_id=args['term_id'])
                except BackgroundJobError as e:
                    _app.logger.error(e)
                    result = None
                    error = str(e)
                except Exception as e:
                    _app.logger.exception(e)
                    result = None
                    error = str(e)
                if result:
                    status = 'success'
                    if isinstance(result, str):
                        _app.logger.info(result)
                        details = result
                    else:
                        details = None
                else:
                    status = 'error'
                    details = error
                update_merged_enrollment_term_job_status(args['id'], status, details)
