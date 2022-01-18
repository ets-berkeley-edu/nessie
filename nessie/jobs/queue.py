"""
Copyright ©2022. The Regents of the University of California (Regents). All Rights Reserved.

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

from queue import Queue
from threading import Thread

"""Background job queue."""


job_queue = None


def get_job_queue():
    return job_queue


def initialize_job_queue(app):
    if app.config['WORKER_QUEUE_ENABLED']:
        global job_queue
        job_queue = Queue()
        app.logger.info(f"Created job queue; starting {app.config['WORKER_THREADS']} worker threads.")
        for i in range(0, app.config['WORKER_THREADS']):
            worker_thread = Thread(target=listen_for_jobs, args=[app, job_queue], daemon=True)
            worker_thread.start()


def listen_for_jobs(app, queue):
    with app.app_context():
        while True:
            job = queue.get()
            args = job.job_args
            app.logger.info('Starting queued job.')
            job.run(**args)
            queue.task_done()
