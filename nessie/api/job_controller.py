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

import datetime
import json
import re

from flask import current_app as app, request
from nessie.api.auth_helper import auth_required
from nessie.api.errors import BadRequestError
from nessie.jobs.background_job import ChainedBackgroundJob
from nessie.jobs.chained_import_student_population import ChainedImportStudentPopulation
from nessie.jobs.create_advisor_schema import CreateAdvisorSchema
from nessie.jobs.create_asc_advising_notes_schema import CreateAscAdvisingNotesSchema
from nessie.jobs.create_berkeleyx_schema import CreateBerkeleyxSchema
from nessie.jobs.create_calnet_schema import CreateCalNetSchema
from nessie.jobs.create_canvas_schema import CreateCanvasSchema
from nessie.jobs.create_coe_schema import CreateCoeSchema
from nessie.jobs.create_data_science_advising_schema import CreateDataScienceAdvisingSchema
from nessie.jobs.create_e_i_advising_notes_schema import CreateEIAdvisingNotesSchema
from nessie.jobs.create_edl_schema import CreateEdlSchema
from nessie.jobs.create_edw_schema import CreateEdwSchema
from nessie.jobs.create_gradescope_schema import CreateGradescopeSchema
from nessie.jobs.create_lrs_glue_jobs import CreateLrsGlueJobs
from nessie.jobs.create_oua_schema import CreateOUASchema
from nessie.jobs.create_sis_advising_notes_schema import CreateSisAdvisingNotesSchema
from nessie.jobs.create_sis_schema import CreateSisSchema
from nessie.jobs.create_sis_terms_schema import CreateSisTermsSchema
from nessie.jobs.create_undergrads_schema import CreateUndergradsSchema
from nessie.jobs.delete_lrs_glue_jobs import DeleteLrsGlueJobs
from nessie.jobs.generate_asc_profiles import GenerateAscProfiles
from nessie.jobs.generate_boac_analytics import GenerateBoacAnalytics
from nessie.jobs.generate_canvas_caliper_analytics import GenerateCanvasCaliperAnalytics
from nessie.jobs.generate_intermediate_tables import GenerateIntermediateTables
from nessie.jobs.generate_merged_enrollment_term import GenerateMergedEnrollmentTerm
from nessie.jobs.generate_merged_hist_enr_feeds import GenerateMergedHistEnrFeeds
from nessie.jobs.generate_merged_student_feeds import GenerateMergedStudentFeeds
from nessie.jobs.import_asc_athletes import ImportAscAthletes
from nessie.jobs.import_calnet_data import ImportCalNetData
from nessie.jobs.import_canvas_enrollments_api import ImportCanvasEnrollmentsApi
from nessie.jobs.import_degree_progress import ImportDegreeProgress
from nessie.jobs.import_lrs_incrementals import ImportLrsIncrementals
from nessie.jobs.import_non_current_students import ImportNonCurrentStudents
from nessie.jobs.import_piazza_api_data import ImportPiazzaApiData
from nessie.jobs.import_registrations import ImportRegistrations
from nessie.jobs.import_registrations_hist_enr import ImportRegistrationsHistEnr
from nessie.jobs.import_sis_student_api import ImportSisStudentApi
from nessie.jobs.import_sis_student_api_hist_enr import ImportSisStudentApiHistEnr
from nessie.jobs.import_student_photos import ImportStudentPhotos
from nessie.jobs.import_ycbm_api import ImportYcbmApi
from nessie.jobs.index_advising_notes import IndexAdvisingNotes
from nessie.jobs.index_enrollments import IndexEnrollments
from nessie.jobs.migrate_lrs_incrementals import MigrateLrsIncrementals
from nessie.jobs.migrate_sis_advising_note_attachments import MigrateSisAdvisingNoteAttachments
from nessie.jobs.refresh_canvas_data_catalog import RefreshCanvasDataCatalog
from nessie.jobs.restore_rds_user_privileges import RestoreRdsUserPrivileges
from nessie.jobs.restore_redshift_user_privileges import RestoreRedshiftUserPrivileges
from nessie.jobs.resync_canvas_snapshots import ResyncCanvasSnapshots
from nessie.jobs.sync_canvas_requests_snapshots import SyncCanvasRequestsSnapshots
from nessie.jobs.sync_canvas_snapshots import SyncCanvasSnapshots
from nessie.jobs.sync_file_to_s3 import SyncFileToS3
from nessie.jobs.transform_lrs_incrementals import TransformLrsIncrementals
from nessie.jobs.transform_piazza_api_data import TransformPiazzaApiData
from nessie.jobs.verify_sis_advising_note_attachments import VerifySisAdvisingNoteAttachments
from nessie.lib.http import tolerant_jsonify
from nessie.lib.metadata import update_canvas_sync_status


@app.route('/api/job/create_advisor_schema', methods=['POST'])
@auth_required
def create_advisor_schema():
    job_started = CreateAdvisorSchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/create_asc_advising_notes_schema', methods=['POST'])
@auth_required
def create_asc_advising_notes_schema():
    job_started = CreateAscAdvisingNotesSchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/create_berkeleyx_schema', methods=['POST'])
@auth_required
def create_berkeleyx_schema():
    job_started = CreateBerkeleyxSchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/create_canvas_schema', methods=['POST'])
@auth_required
def create_canvas_schema():
    job_started = CreateCanvasSchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/create_coe_schema', methods=['POST'])
@auth_required
def create_coe_schema():
    job_started = CreateCoeSchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/create_calnet_schema', methods=['POST'])
@auth_required
def create_calnet_schema():
    job_started = CreateCalNetSchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/create_data_science_advising_schema', methods=['POST'])
@auth_required
def create_data_science_advising_schema():
    job_started = CreateDataScienceAdvisingSchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/create_e_and_i_advising_notes_schema', methods=['POST'])
@auth_required
def create_e_and_i_advising_notes_schema():
    job_started = CreateEIAdvisingNotesSchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/create_edl_schema', methods=['POST'])
@auth_required
def create_edl_schema():
    job_started = CreateEdlSchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/create_edw_schema', methods=['POST'])
@auth_required
def create_edw_schema():
    job_started = CreateEdwSchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/create_gradescope_schema', methods=['POST'])
@auth_required
def create_gradescope_schema():
    job_started = CreateGradescopeSchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/create_oua_schema', methods=['POST'])
@auth_required
def create_oua_schema():
    job_started = CreateOUASchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/create_undergrads_schema', methods=['POST'])
@auth_required
def create_undergrads_schema():
    job_started = CreateUndergradsSchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/create_sis_schema', methods=['POST'])
@auth_required
def create_sis_schema():
    job_started = CreateSisSchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/create_sis_terms_schema', methods=['POST'])
@auth_required
def create_sis_terms_schema():
    job_started = CreateSisTermsSchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/create_sis_advising_notes_schema', methods=['POST'])
@auth_required
def create_sis_advising_notes_schema():
    job_started = CreateSisAdvisingNotesSchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/generate_all_tables', methods=['POST'])
@auth_required
def generate_all_tables():
    chained_job = ChainedBackgroundJob(
        steps=[
            RefreshCanvasDataCatalog(),
            GenerateIntermediateTables(),
            IndexEnrollments(),
            GenerateBoacAnalytics(),
        ],
    )
    job_started = chained_job.run_async()
    return respond_with_status(job_started)


@app.route('/api/job/generate_asc_profiles', methods=['POST'])
@auth_required
def generate_asc_profiles():
    job_started = GenerateAscProfiles().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/generate_boac_analytics', methods=['POST'])
@auth_required
def generate_boac_analytics():
    job_started = GenerateBoacAnalytics().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/generate_canvas_caliper_analytics', methods=['POST'])
@auth_required
def generate_canvas_caliper_analytics():
    job_started = GenerateCanvasCaliperAnalytics().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/generate_intermediate_tables', methods=['POST'])
@auth_required
def generate_intermediate_tables():
    job_started = GenerateIntermediateTables().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/generate_merged_enrollment_term/<term_id>', methods=['POST'])
@auth_required
def generate_merged_enrollment_term(term_id):
    job_started = GenerateMergedEnrollmentTerm(term_id=term_id).run_async()
    return respond_with_status(job_started)


@app.route('/api/job/generate_merged_hist_enr_feeds', methods=['POST'])
@auth_required
def generate_merged_hist_enr_feeds():
    job_started = GenerateMergedHistEnrFeeds().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/generate_merged_student_feeds/<term_id>', methods=['POST'])
@auth_required
def generate_merged_student_feeds(term_id):
    job_started = GenerateMergedStudentFeeds(term_id=term_id).run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_canvas_enrollments_api', methods=['POST'])
@auth_required
def import_canvas_enrollments_api():
    args = get_json_args(request)
    if args:
        term_id = args.get('term')
    else:
        term_id = None
    job_started = ImportCanvasEnrollmentsApi(term_id=term_id).run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_degree_progress', methods=['POST'])
@auth_required
def import_degree_progress():
    job_started = ImportDegreeProgress().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_lrs_incrementals', methods=['POST'])
@auth_required
def import_lrs_incrementals():
    args = get_json_args(request)
    if args:
        truncate_lrs = args.get('truncate_lrs')
    else:
        truncate_lrs = False
    job_started = ImportLrsIncrementals(truncate_lrs=truncate_lrs).run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_non_current_students', methods=['POST'])
@auth_required
def import_non_current_students():
    job_started = ImportNonCurrentStudents().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_piazza_api_data', methods=['POST'])
@app.route('/api/job/import_piazza_api_data/<archive>', methods=['POST'])
@auth_required
def import_piazza_api_data(archive='latest'):
    today = datetime.date.today()
    todays_day = today.day
    first_of_the_month = today.replace(day=1)
    last_month = first_of_the_month - datetime.timedelta(days=1)
    # if today is the first day of the month, fetch the last month's monthly as latest instead of daily
    if (archive == 'monthly') or (archive == 'latest' and todays_day == 1):
        archive = last_month.strftime('monthly_%Y-%m-01')
    if (archive != 'latest') and not (re.match('(daily|monthly|full)', archive) and re.match(r'(\w+)_(\d{4}\-\d{2}\-\d{2})', archive)):
        raise BadRequestError(f"Incorrect archive parameter '{archive}', should be 'latest' or like 'daily_2020-09-12'.")
    job_started = ImportPiazzaApiData(archive=archive).run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_ycbm_api', methods=['POST'])
@auth_required
def import_ycbm_api():
    job_started = ImportYcbmApi().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/index_enrollments/<term_id>', methods=['POST'])
@auth_required
def index_enrollments(term_id):
    job_started = IndexEnrollments(term_id=term_id).run_async()
    return respond_with_status(job_started)


@app.route('/api/job/index_advising_notes', methods=['POST'])
@auth_required
def index_advising_notes():
    job_started = IndexAdvisingNotes().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/create_lrs_glue_jobs', methods=['POST'])
@auth_required
def create_lrs_glue_jobs():
    job_started = CreateLrsGlueJobs().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/load_lrs_incrementals', methods=['POST'])
@auth_required
def full_caliper_import():
    args = get_json_args(request)
    if args:
        truncate_lrs = args.get('truncate_lrs')
    else:
        truncate_lrs = False
    chained_job = ChainedBackgroundJob(
        steps=[
            ImportLrsIncrementals(truncate_lrs=truncate_lrs),
            TransformLrsIncrementals(),
            MigrateLrsIncrementals(),
        ],
    )
    job_started = chained_job.run_async()
    return respond_with_status(job_started)


@app.route('/api/job/migrate_lrs_incrementals', methods=['POST'])
@auth_required
def migrate_lrs_incrementals():
    job_started = MigrateLrsIncrementals().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/transform_lrs_incrementals', methods=['POST'])
@auth_required
def transform_lrs_incrementals():
    job_started = TransformLrsIncrementals().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/transform_piazza_api_data', methods=['POST'])
@app.route('/api/job/transform_piazza_api_data/<archive>', methods=['POST'])
@auth_required
def transform_piazza_api_data(archive='latest'):
    if (archive != 'latest') and not (re.match('(daily|monthly|full)', archive) and re.match(r'(\w+)\_(\d{4}\-\d{2}\-\d{2})', archive)):
        raise BadRequestError(f"Incorrect archive parameter '{archive}', should be 'latest' or like 'daily_2020-09-12'.")
    job_started = TransformPiazzaApiData(archive=archive).run_async()
    return respond_with_status(job_started)


@app.route('/api/job/delete_lrs_glue_jobs', methods=['POST'])
@auth_required
def delete_lrs_glue_jobs():
    job_started = DeleteLrsGlueJobs().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_sis_student_api', methods=['POST'])
@auth_required
def import_sis_student_api():
    job_started = ImportSisStudentApi().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_sis_student_api_hist_enr', methods=['POST'])
@auth_required
def import_sis_student_api_hist_enr():
    job_started = ImportSisStudentApiHistEnr().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_student_photos', methods=['POST'])
@auth_required
def import_student_photos():
    job_started = ImportStudentPhotos().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_student_population', methods=['POST'])
@auth_required
def import_student_population():
    job_started = ChainedImportStudentPopulation().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_registrations/<load_mode>', methods=['POST'])
@auth_required
def import_registrations(load_mode):
    if load_mode not in ['all', 'batch', 'new']:
        raise BadRequestError('Unrecognized mode for registrations import.')
    job_started = ImportRegistrations(load_mode=load_mode).run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_registrations_hist_enr/<load_mode>', methods=['POST'])
@auth_required
def import_registrations_hist_enr(load_mode):
    if load_mode not in ['batch', 'new']:
        raise BadRequestError('Unrecognized mode for non-advisee registrations import.')
    job_started = ImportRegistrationsHistEnr(load_mode=load_mode).run_async()
    return respond_with_status(job_started)


@app.route('/api/job/migrate_sis_advising_note_attachments/<datestamp>', methods=['POST'])
@auth_required
def migrate_sis_advising_note_attachments(datestamp):
    job_started = MigrateSisAdvisingNoteAttachments(datestamp=datestamp).run_async()
    return respond_with_status(job_started)


@app.route('/api/job/verify_sis_advising_note_attachments/<datestamp>', methods=['POST'])
@auth_required
def verify_sis_advising_note_attachments(datestamp):
    job_started = VerifySisAdvisingNoteAttachments(datestamp=datestamp).run_async()
    return respond_with_status(job_started)


@app.route('/api/job/refresh_canvas_data_catalog', methods=['POST'])
@auth_required
def refresh_canvas_data_catalog():
    job_started = RefreshCanvasDataCatalog().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/restore_rds_user_privileges', methods=['POST'])
@auth_required
def restore_rds_user_privileges():
    job_started = RestoreRdsUserPrivileges().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/restore_redshift_user_privileges', methods=['POST'])
@auth_required
def restore_redshift_user_privileges():
    job_started = RestoreRedshiftUserPrivileges().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/resync_canvas_snapshots', methods=['POST'])
@auth_required
def resync_canvas_snapshots():
    job_started = ResyncCanvasSnapshots().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/sync_canvas_requests_snapshots', methods=['POST'])
@auth_required
def sync_canvas_requests_snapshots():
    job_started = SyncCanvasRequestsSnapshots().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/sync_canvas_snapshots', methods=['POST'])
@auth_required
def sync_canvas_snapshots():
    job_started = SyncCanvasSnapshots().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/sync_file_to_s3', methods=['POST'])
@auth_required
def sync_file_to_s3():
    data = json.loads(request.data)
    url = data and data.get('url')
    if not url:
        raise BadRequestError('Required "url" parameter missing.')
    key = data and data.get('key')
    if not key:
        raise BadRequestError('Required "key" parameter missing.')
    canvas_sync_job_id = data and data.get('canvas_sync_job_id')
    if canvas_sync_job_id:
        update_canvas_sync_status(canvas_sync_job_id, key, 'received')
    job_started = SyncFileToS3(url=url, key=key, canvas_sync_job_id=canvas_sync_job_id).run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_asc_athletes', methods=['POST'])
@auth_required
def import_asc_athletes():
    job_started = ImportAscAthletes().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_calnet_data', methods=['POST'])
@auth_required
def import_calnet_data():
    job_started = ImportCalNetData().run_async()
    return respond_with_status(job_started)


def respond_with_status(job_started):
    if job_started:
        return tolerant_jsonify({'status': 'started'})
    else:
        return tolerant_jsonify({'status': 'errored'})


def get_json_args(request):
    try:
        args = request.get_json(force=True)
    except Exception:
        args = None
    return args
