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

from itertools import groupby
import tempfile

from flask import current_app as app
from nessie.externals import rds, redshift
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib import queries
from nessie.lib.analytics import generate_analytics_feeds_for_course
from nessie.lib.berkeley import reverse_term_ids
from nessie.lib.util import hashed_datestamp, resolve_sql_template
from nessie.models.student_schema_manager import refresh_from_staging, truncate_staging_table, write_file_to_staging

"""Logic for BOAC analytics job."""


class GenerateBoacAnalytics(BackgroundJob):
    s3_boa_path = f"s3://{app.config['LOCH_S3_BUCKET']}/" + app.config['LOCH_S3_BOAC_ANALYTICS_DATA_PATH']

    def run(self, term_id=None):
        app.logger.info('Starting BOAC analytics job...')

        all_canvas_terms = reverse_term_ids()
        boac_snapshot_daily_path = f'{self.s3_boa_path}/term/{all_canvas_terms[0]}/daily/{hashed_datestamp()}'
        resolved_ddl = resolve_sql_template(
            'create_boac_schema.template.sql',
            boac_snapshot_daily_path=boac_snapshot_daily_path,
            current_term_id=all_canvas_terms[0],
            last_term_id=all_canvas_terms[1],
            previous_term_id=all_canvas_terms[2],
        )
        if not redshift.execute_ddl_script(resolved_ddl):
            raise BackgroundJobError('BOAC analytics creation job failed.')

        resolved_ddl_rds = resolve_sql_template('update_rds_indexes_boac.template.sql')
        if not rds.execute(resolved_ddl_rds):
            raise BackgroundJobError('Failed to update RDS indexes for BOAC analytics schema.')

        # By default, refresh Canvas analytics for the current term only.
        if not term_id:
            term_id = [all_canvas_terms[0]]
        self.generate_analytics_feeds(term_id)

        return 'BOAC analytics creation job completed.'

    def generate_analytics_feeds(self, term_id):
        with tempfile.TemporaryFile() as output_file:
            try:
                canvas_sites_stream = queries.stream_canvas_sites(term_id)
                canvas_enrollments_stream = queries.stream_canvas_enrollments(term_id)
                assignment_submissions_stream = queries.stream_canvas_assignment_submissions(term_id)

                enrollments_by_course_id = groupby(canvas_enrollments_stream, lambda r: r['canvas_course_id'])
                submissions_by_course_id = groupby(assignment_submissions_stream, lambda r: r['canvas_course_id'])
                enr_tracker = {'course_id': 0, 'stream': []}
                sub_tracker = {'course_id': 0, 'stream': []}

                membership_count = 0

                for canvas_site_row in canvas_sites_stream:
                    course_site_id = canvas_site_row['canvas_course_id']
                    app.logger.info(f'Generating analytics: course site {course_site_id}')

                    while enr_tracker['course_id'] < course_site_id:
                        enr_tracker['course_id'], enr_tracker['stream'] = next(enrollments_by_course_id, (course_site_id, []))
                    if enr_tracker['course_id'] == course_site_id:
                        site_enrollments_stream = enr_tracker['stream']
                    else:
                        site_enrollments_stream = []

                    while sub_tracker['course_id'] < course_site_id:
                        sub_tracker['course_id'], sub_tracker['stream'] = next(submissions_by_course_id, (course_site_id, []))
                    if sub_tracker['course_id'] == course_site_id:
                        site_submissions_stream = sub_tracker['stream']
                    else:
                        site_submissions_stream = []

                    membership_count += generate_analytics_feeds_for_course(
                        output_file,
                        term_id,
                        canvas_site_row,
                        site_enrollments_stream,
                        site_submissions_stream,
                    )

            finally:
                canvas_sites_stream.close()
                canvas_enrollments_stream.close()
                assignment_submissions_stream.close()

            table_name = 'student_canvas_site_memberships'

            with redshift.transaction() as transaction:
                truncate_staging_table(table_name)
                write_file_to_staging(table_name, output_file, membership_count, term_id)
                refresh_from_staging(table_name, term_id, None, transaction)
                if not transaction.commit():
                    raise BackgroundJobError(f'Final transaction commit failed on site membership refresh (term_id={term_id}).')
