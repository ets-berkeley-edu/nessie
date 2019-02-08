"""
Copyright ©2019. The Regents of the University of California (Regents). All Rights Reserved.

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

from datetime import datetime, timedelta
import os

from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.util import resolve_sql_template

"""Generate analytics from exploded canvas caliper statements."""


def get_s3_daily_canvas_caliper_explode_path(date_to_stamp=None):
    datestring = (date_to_stamp or datetime.now()).strftime('%Y/%m/%d')
    return os.path.join(
        app.config['LRS_CANVAS_CALIPER_EXPLODE_OUTPUT_PATH'],
        datestring,
    )


class GenerateCanvasCaliperAnalytics(BackgroundJob):
    def run(self):
        app.logger.info('Start generating canvas caliper analytics')
        redshift_schema_caliper_analytics = app.config['REDSHIFT_SCHEMA_CALIPER']
        redshift_schema_lrs_external = app.config['REDSHIFT_SCHEMA_LRS']
        canvas_caliper_explode_table = 'canvas_caliper_explode'

        # Because the Caliper incrementals are provided by a Glue job running on a different schedule, the latest batch
        # may have been delivered before last midnight UTC.
        s3_caliper_daily_path = get_s3_daily_canvas_caliper_explode_path()
        if not s3.get_keys_with_prefix(s3_caliper_daily_path):
            s3_caliper_daily_path = get_s3_daily_canvas_caliper_explode_path(datetime.now() - timedelta(days=1))
            if not s3.get_keys_with_prefix(s3_caliper_daily_path):
                raise BackgroundJobError(f'No timely S3 Caliper extracts found')
            else:
                app.logger.info(f'Falling back S3 Caliper extracts for yesterday')
        s3_caliper_daily_url = s3.build_s3_url(s3_caliper_daily_path, credentials=False)

        resolved_ddl_caliper_explode = resolve_sql_template(
            'create_lrs_canvas_explode_table.template.sql',
            canvas_caliper_explode_table=canvas_caliper_explode_table,
            loch_s3_caliper_explode_url=s3_caliper_daily_url,
        )
        redshift.drop_external_schema(redshift_schema_lrs_external)
        if redshift.execute_ddl_script(resolved_ddl_caliper_explode):
            app.logger.info('Caliper explode schema and table successfully created.')
        else:
            raise BackgroundJobError('Caliper explode schema and table creation failed.')

        # Sanity-check event times from the latest Caliper batch against previously transformed event times.
        def datetime_from_query(query):
            response = redshift.fetch(query)
            timestamp = response and response[0] and response[0].get('timestamp')
            if not timestamp:
                raise BackgroundJobError(f'Timestamp query failed to return data for comparison; aborting job: {query}')
            if isinstance(timestamp, str):
                timestamp = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
            return timestamp

        earliest_untransformed = datetime_from_query(
            f'SELECT MIN(timestamp) AS timestamp FROM {redshift_schema_lrs_external}.{canvas_caliper_explode_table}',
        )
        latest_transformed = datetime_from_query(
            f'SELECT MAX(timestamp) AS timestamp FROM {redshift_schema_caliper_analytics}.canvas_caliper_user_requests',
        )
        if not earliest_untransformed or not latest_transformed:
            return False
        timestamp_diff = (earliest_untransformed - latest_transformed).total_seconds()
        if timestamp_diff < -60 or timestamp_diff > 300:
            raise BackgroundJobError(
                f'Unexpected difference between Caliper timestamps: latest transformed {latest_transformed}, '
                f'earliest untransformed {earliest_untransformed}',
            )

        resolved_ddl_caliper_analytics = resolve_sql_template('generate_caliper_analytics.template.sql')
        if redshift.execute_ddl_script(resolved_ddl_caliper_analytics):
            return 'Caliper analytics tables successfully created.'
        else:
            raise BackgroundJobError('Caliper analytics tables creation failed.')
