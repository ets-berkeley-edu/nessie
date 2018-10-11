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

import json

from flask import current_app as app, request
from nessie.api.auth_helper import auth_required
from nessie.api.errors import BadRequestError
from nessie.jobs.background_job import ChainedBackgroundJob
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
from nessie.jobs.import_canvas_enrollments_api import ImportCanvasEnrollmentsApi
from nessie.jobs.import_degree_progress import ImportDegreeProgress
from nessie.jobs.import_lrs_incrementals import ImportLrsIncrementals
from nessie.jobs.import_sis_enrollments_api import ImportSisEnrollmentsApi
from nessie.jobs.import_sis_student_api import ImportSisStudentApi
from nessie.jobs.import_sis_terms_api import ImportSisTermsApi
from nessie.jobs.import_term_gpas import ImportTermGpas
from nessie.jobs.resync_canvas_snapshots import ResyncCanvasSnapshots
from nessie.jobs.sync_canvas_snapshots import SyncCanvasSnapshots
from nessie.jobs.sync_file_to_s3 import SyncFileToS3
from nessie.lib.http import tolerant_jsonify
from nessie.lib.metadata import update_canvas_sync_status


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


@app.route('/api/job/create_sis_schema', methods=['POST'])
@auth_required
def create_sis_schema():
    job_started = CreateSisSchema().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/generate_all_tables', methods=['POST'])
@auth_required
def generate_all_tables():
    chained_job = ChainedBackgroundJob(
        steps=[
            CreateCanvasSchema(),
            CreateSisSchema(),
            GenerateIntermediateTables(),
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


@app.route('/api/job/generate_intermediate_tables', methods=['POST'])
@auth_required
def generate_intermediate_tables():
    job_started = GenerateIntermediateTables().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/generate_merged_student_feeds/<term_id>', methods=['POST'])
@auth_required
def generate_merged_student_feeds(term_id):
    if term_id == 'all':
        term_id = None
    args = get_json_args(request)
    if args:
        backfill = args.get('backfill')
    else:
        backfill = False
    job_started = GenerateMergedStudentFeeds(term_id=term_id, backfill_new_students=backfill).run_async()
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
    job_started = ImportLrsIncrementals().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_sis_enrollments_api', methods=['POST'])
@auth_required
def import_sis_enrollments_api():
    args = get_json_args(request)
    if args:
        term_id = args.get('term')
    else:
        term_id = None
    job_started = ImportSisEnrollmentsApi(term_id=term_id).run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_sis_student_api', methods=['POST'])
@auth_required
def import_sis_student_api():
    job_started = ImportSisStudentApi().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_sis_terms_api/<term_id>', methods=['POST'])
@auth_required
def import_sis_terms_api(term_id):
    if term_id == 'all':
        term_ids = None
    else:
        term_ids = [term_id]
    job_started = ImportSisTermsApi(term_ids=term_ids).run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_student_population', methods=['POST'])
@auth_required
def import_student_population():
    chained_job = ChainedBackgroundJob(
        steps=[
            CreateCoeSchema(),
            ImportAscAthletes(),
            GenerateAscProfiles(),
            ImportCalNetData(),
            CreateCalNetSchema(),
        ],
    )
    job_started = chained_job.run_async()
    return respond_with_status(job_started)


@app.route('/api/job/import_term_gpas', methods=['POST'])
@auth_required
def import_term_gpas():
    job_started = ImportTermGpas().run_async()
    return respond_with_status(job_started)


@app.route('/api/job/resync_canvas_snapshots', methods=['POST'])
@auth_required
def resync_canvas_snapshots():
    job_started = ResyncCanvasSnapshots().run_async()
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
