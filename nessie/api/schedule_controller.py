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

from flask import current_app as app, request
from nessie.api.auth_helper import auth_required
from nessie.api.errors import BadRequestError
from nessie.jobs.scheduling import get_scheduler, PG_ADVISORY_LOCK_IDS, schedule_all_jobs
from nessie.lib.http import tolerant_jsonify
from nessie.models.util import get_granted_lock_ids


def job_to_dict(job):
    lock_ids = get_granted_lock_ids()
    job_components = job.args[0]
    if not hasattr(job_components, '__iter__'):
        job_components = [job_components]
    job_dict = {
        'id': job.id.lower(),
        'components': [c.__name__ for c in job_components],
        'trigger': str(job.trigger),
        'nextRun': str(job.next_run_time) if job.next_run_time else None,
        'locked': (PG_ADVISORY_LOCK_IDS.get(job.id) in lock_ids),
    }
    if len(job.args) > 2:
        args_dict = dict(job.args[2])
        args_dict.pop('lock_id', None)
        if args_dict:
            job_dict['args'] = args_dict
    return job_dict


@app.route('/api/schedule', methods=['GET'])
def get_job_schedule():
    sched = get_scheduler()
    return tolerant_jsonify([job_to_dict(job) for job in sched.get_jobs()])


@app.route('/api/schedule/<job_id>', methods=['POST', 'DELETE'])
@auth_required
def update_job_schedule(job_id):
    sched = get_scheduler()
    job_id = job_id.upper()
    job = sched.get_job(job_id)
    if not job:
        raise BadRequestError(f'No job found for job id: {job_id}')
    if request.method == 'DELETE':
        app.logger.warn(f'About to delete schedule definition for job id: {job_id}')
        sched.remove_job(job_id)
        return tolerant_jsonify([job_to_dict(job) for job in sched.get_jobs()])
    else:
        # If JSON properties are present, they will be evaluated by APScheduler's cron trigger API.
        # https://apscheduler.readthedocs.io/en/latest/modules/triggers/cron.html#module-apscheduler.triggers.cron
        try:
            args = request.get_json(force=True)
        except Exception as e:
            raise BadRequestError(str(e))
        if args:
            try:
                job.reschedule(trigger='cron', **args)
            except Exception as e:
                raise BadRequestError(f'Error rescheduling job: {e}')
        # Passing a empty JSON object will pause this job.
        else:
            job.pause()
        job = sched.get_job(job_id)
        return tolerant_jsonify(job_to_dict(job))


@app.route('/api/schedule/<job_id>/args', methods=['POST'])
@auth_required
def update_scheduled_job_args(job_id):
    try:
        args = request.get_json(force=True)
    except Exception as e:
        raise BadRequestError(str(e))
    if not args:
        raise BadRequestError('Could not parse args from request')
    sched = get_scheduler()
    job_id = job_id.upper()
    job = sched.get_job(job_id)
    if not job:
        raise BadRequestError(f'No job found for job id: {job_id}')
    try:
        existing_args = job.args
        if len(existing_args) > 2:
            new_args = dict(existing_args[2])
            new_args.update(args)
        else:
            new_args = args
        job.modify(args=[existing_args[0], existing_args[1], new_args])
    except Exception as e:
        raise BadRequestError(f'Error updating job args: {e}')
    job = sched.get_job(job_id)
    return tolerant_jsonify(job_to_dict(job))


@app.route('/api/schedule/reload', methods=['POST'])
@auth_required
def reload_job_schedules():
    """Discard any manual changes to job schedules and bring back the configured version."""
    if not app.config['JOB_SCHEDULING_ENABLED']:
        raise BadRequestError('Job scheduling is not enabled')
    schedule_all_jobs(force=True)
    app.logger.info('Overwrote current jobs schedule with configured values')
    return get_job_schedule()
