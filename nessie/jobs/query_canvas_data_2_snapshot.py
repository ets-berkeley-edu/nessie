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

import time

from flask import current_app as app
from nessie.externals import canvas_data_2
from nessie.jobs.background_job import BackgroundJob


"""Logic to trigger query Canvas Data 2 snapshot job with Instructure."""


class QueryCanvasData2Snapshot(BackgroundJob):

    @classmethod
    def generate_job_id(cls):
        return 'query_cd2_snapshot_' + str(int(time.time()))

    def run(self, cleanup=True):
        nessie_job_id = self.generate_job_id()
        app.logger.info(f'Starting Query Canvas Data 2 snapshot job... (id={nessie_job_id})')
        namespace = 'canvas'
        cd2_tables = canvas_data_2.get_cd2_tables_list(namespace)

        app.logger.info(f'{len(cd2_tables)} tables available for download from namespace {namespace}. \n{cd2_tables}')
        app.logger.info('Begin query snapshot process for each table and retrieve job ids for tracking')
        cd2_table_query_jobs = []
        cd2_table_query_jobs = canvas_data_2.start_query_snapshot(cd2_tables)

        return f'Started query snapshot jobs and retrived job IDs for len{cd2_table_query_jobs} Canvas data 2 tables'
