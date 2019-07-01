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
import json

from flask import current_app as app
from nessie.externals import rds, redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.berkeley import reverse_term_ids, term_name_for_sis_id
from nessie.lib.util import encoded_tsv_row, hashed_datestamp, resolve_sql_template, resolve_sql_template_string, split_tsv_row
from nessie.merged import student_demographics

"""Logic for BOAC analytics job."""


class GenerateBoacAnalytics(BackgroundJob):
    s3_boa_path = f"s3://{app.config['LOCH_S3_BUCKET']}/" + app.config['LOCH_S3_BOAC_ANALYTICS_DATA_PATH']

    def run(self):
        app.logger.info(f'Starting BOAC analytics job...')

        term_id_series = reverse_term_ids()
        boac_snapshot_daily_path = f'{self.s3_boa_path}/term/{term_id_series[0]}/daily/{hashed_datestamp()}'
        resolved_ddl = resolve_sql_template(
            'create_boac_schema.template.sql',
            aws_access_key_id=app.config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=app.config['AWS_SECRET_ACCESS_KEY'],
            boac_snapshot_daily_path=boac_snapshot_daily_path,
            current_term_id=term_id_series[0],
            last_term_id=term_id_series[1],
            previous_term_id=term_id_series[2],
        )
        if not redshift.execute_ddl_script(resolved_ddl):
            raise BackgroundJobError(f'BOAC analytics creation job failed.')

        self.store_boa_demographics_data()

        boac_assignments_path = f'{self.s3_boa_path}/assignment_submissions_relative'
        for term_id in term_id_series:
            term_name = term_name_for_sis_id(term_id)
            resolved_ddl = resolve_sql_template(
                'unload_assignment_submissions.template.sql',
                aws_access_key_id=app.config['AWS_ACCESS_KEY_ID'],
                aws_secret_access_key=app.config['AWS_SECRET_ACCESS_KEY'],
                boac_assignments_path=boac_assignments_path,
                term_id=term_id,
                term_name=term_name,
            )
            if not redshift.execute_ddl_script(resolved_ddl):
                raise BackgroundJobError(f'Assignment submissions upload failed for term {term_id}.')

        return 'BOAC analytics creation job completed.'

    def store_boa_demographics_data(self):
        demographics_rows = student_demographics.generate_demographics_data()
        if not demographics_rows:
            app.logger.warn('No demographics rows found; will not refresh Redshift or RDS.')
            return
        if not self.update_redshift(demographics_rows):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')

        with rds.transaction() as transaction:
            if self.update_rds(demographics_rows, transaction):
                transaction.commit()
                app.logger.info('Refreshed RDS indexes.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Failed to refresh RDS indexes.')

    def update_redshift(self, rows):
        redshift_rows = []
        for row in rows:
            redshift_rows.append(encoded_tsv_row([row['sid'], json.dumps(row['feed'])]))
        s3_key = f"{app.config['LOCH_S3_BOAC_ANALYTICS_DATA_PATH']}/demographics.tsv"
        app.logger.info(f'Will stash {len(redshift_rows)} feeds in S3: {s3_key}')
        if not s3.upload_tsv_rows(redshift_rows, s3_key):
            raise BackgroundJobError('Error on S3 upload: aborting job.')
        app.logger.info('Will copy S3 feeds into Redshift...')
        query = resolve_sql_template_string(
            """
            COPY {redshift_schema_boac}.student_demographics
                FROM '{s3_path}'
                IAM_ROLE '{redshift_iam_role}'
                DELIMITER '\\t';
            """,
            s3_path=f'{self.s3_boa_path}/demographics.tsv',
        )
        return redshift.execute(query)

    def update_rds(self, rows, transaction):
        rds_schema = app.config['RDS_SCHEMA_STUDENT']
        demographics_rows = []
        ethnicities_rows = []
        for row in rows:
            sid = row['sid']
            feed = row['feed']
            filtered_ethnicities = row['filtered_ethnicities']
            demographics_rows.append(encoded_tsv_row([sid, feed['gender'], feed['underrepresented']]))
            for ethn in filtered_ethnicities:
                ethnicities_rows.append(encoded_tsv_row([sid, ethn]))
        if not transaction.execute(f'TRUNCATE {rds_schema}.demographics'):
            return False
        if not transaction.execute(f'TRUNCATE {rds_schema}.ethnicities'):
            return False
        if not transaction.insert_bulk(
                f'INSERT INTO {rds_schema}.demographics (sid, gender, minority) VALUES %s',
                [split_tsv_row(r) for r in demographics_rows],
        ):
            return False
        if not transaction.insert_bulk(
                f'INSERT INTO {rds_schema}.ethnicities (sid, ethnicity) VALUES %s',
                [split_tsv_row(r) for r in ethnicities_rows],
        ):
            return False
        return True
