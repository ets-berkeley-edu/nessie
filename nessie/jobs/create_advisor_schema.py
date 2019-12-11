"""
Copyright ©2020. The Regents of the University of California (Regents). All Rights Reserved.

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

from flask import current_app as app
from nessie.externals import calnet, rds, redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError, verify_external_schema
from nessie.lib.util import encoded_tsv_row, get_s3_calnet_daily_path, get_s3_sis_sysadm_daily_path, resolve_sql_template, resolve_sql_template_string

"""Logic for Advisor schema creation job."""


class CreateAdvisorSchema(BackgroundJob):

    def run(self):
        app.logger.info(f'Starting Advisor schema creation job...')
        app.logger.info(f'Executing SQL...')
        self.create_schema()
        app.logger.info('Redshift schema created.')
        self.import_advisor_attributes()
        app.logger.info('Advisor attributes imported.')
        self.create_rds_indexes()

        return 'Advisor schema creation job completed.'

    def create_schema(self):
        external_schema = app.config['REDSHIFT_SCHEMA_ADVISOR']
        redshift.drop_external_schema(external_schema)

        resolved_ddl = resolve_sql_template(
            'create_advisor_schema.template.sql',
            advisor_data_path=self.s3_path(),
        )
        if redshift.execute_ddl_script(resolved_ddl):
            verify_external_schema(external_schema, resolved_ddl)
        else:
            raise BackgroundJobError('Advisor schema creation job failed.')

    def import_advisor_attributes(self):
        csid_results = redshift.fetch(
            resolve_sql_template_string('SELECT DISTINCT advisor_sid FROM {redshift_schema_advisor_internal}.advisor_students'),
        )
        csids = [r['advisor_sid'] for r in csid_results]
        all_attributes = calnet.client(app).search_csids(csids)
        if len(csids) != len(all_attributes):
            ldap_csids = [l['csid'] for l in all_attributes]
            missing = set(csids) - set(ldap_csids)
            app.logger.warning(f'Looked for {len(csids)} advisor CSIDs but only found {len(all_attributes)} : missing {missing}')

        advisor_rows = []
        total_count = len(all_attributes)
        for index, a in enumerate(all_attributes):
            sid = a['csid']
            app.logger.info(f'CalNet import: Fetch attributes of advisor {sid} ({index + 1} of {total_count})')
            first_name, last_name = calnet.split_sortable_name(a)
            data = [
                a['uid'],
                sid,
                first_name,
                last_name,
                a['title'],
                calnet.get_dept_code(a),
                a['email'],
                a['campus_email'],
            ]
            advisor_rows.append(encoded_tsv_row(data))

        s3_key = f'{get_s3_calnet_daily_path()}/advisors/advisors.tsv'
        app.logger.info(f'Will stash {len(advisor_rows)} feeds in S3: {s3_key}')
        if not s3.upload_tsv_rows(advisor_rows, s3_key):
            raise BackgroundJobError('Error on S3 upload: aborting job.')

        app.logger.info('Will copy S3 feeds into Redshift...')
        query = resolve_sql_template_string(
            """
            TRUNCATE {redshift_schema_advisor_internal}.advisor_attributes;
            COPY {redshift_schema_advisor_internal}.advisor_attributes
                FROM '{loch_s3_calnet_data_path}/advisors/advisors.tsv'
                IAM_ROLE '{redshift_iam_role}'
                DELIMITER '\\t';
            """,
        )
        if not redshift.execute(query):
            app.logger.error('Error on Redshift copy: aborting job.')
            return False

    def create_rds_indexes(self):
        resolved_ddl = resolve_sql_template('index_advisors.template.sql')
        if rds.execute(resolved_ddl):
            app.logger.info('Created RDS indexes for advisor schema.')
        else:
            raise BackgroundJobError('Failed to create RDS indexes for advisor schema.')

    def s3_path(self):
        s3_sis_daily = get_s3_sis_sysadm_daily_path()
        if not s3.get_keys_with_prefix(s3_sis_daily):
            s3_sis_daily = get_s3_sis_sysadm_daily_path(datetime.now() - timedelta(days=1))
            if not s3.get_keys_with_prefix(s3_sis_daily):
                raise BackgroundJobError(f'No timely SIS S3 advisor data found')
            else:
                app.logger.info(f'Falling back to SIS S3 daily advisor data for yesterday')

        return '/'.join([
            f"s3://{app.config['LOCH_S3_BUCKET']}",
            s3_sis_daily,
            'advisors',
        ])
