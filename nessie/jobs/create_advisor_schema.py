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

from datetime import datetime, timedelta

from flask import current_app as app
from nessie.externals import calnet, rds, redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError, verify_external_schema
from nessie.lib.util import encoded_tsv_row, get_s3_calnet_daily_path, get_s3_sis_sysadm_daily_path, resolve_sql_template, resolve_sql_template_string

"""Logic for Advisor schema creation job."""


class CreateAdvisorSchema(BackgroundJob):

    external_schema = app.config['REDSHIFT_SCHEMA_ADVISOR']
    feature_flag_edl = app.config['FEATURE_FLAG_EDL_ADVISORS']

    def run(self):
        app.logger.info('Starting Advisor schema creation job...')
        self.create_schema()
        if self.import_advisor_attributes():
            # Create RDS indexes
            resolved_ddl = resolve_sql_template('index_advisors.template.sql')
            if not rds.execute(resolved_ddl):
                raise BackgroundJobError('Failed to create RDS indexes for advisor schema.')

            app.logger.info('Created RDS indexes for advisor schema.')
            return 'Advisor schema creation job completed.'
        else:
            raise BackgroundJobError('Failed to import advisor attributes from CalNet.')

    def create_schema(self):
        app.logger.info('Executing SQL...')
        redshift.drop_external_schema(self.external_schema)

        s3_sis_daily = get_s3_sis_sysadm_daily_path()
        if not s3.get_keys_with_prefix(s3_sis_daily):
            s3_sis_daily = _get_yesterdays_advisor_data()
        s3_path = '/'.join([f"s3://{app.config['LOCH_S3_BUCKET']}", s3_sis_daily, 'advisors'])

        sql_filename = 'edl_create_advisor_schema.template.sql' if self.feature_flag_edl else 'create_advisor_schema.template.sql'
        resolved_ddl = resolve_sql_template(sql_filename, advisor_data_path=s3_path)
        if not redshift.execute_ddl_script(resolved_ddl):
            raise BackgroundJobError(f'Redshift execute_ddl_script failed on {sql_filename}')

        verify_external_schema(self.external_schema, resolved_ddl)
        app.logger.info('Redshift schema created.')

    def import_advisor_attributes(self):
        if self.feature_flag_edl:
            sql = resolve_sql_template_string("""
                SELECT DISTINCT advisor_id
                FROM {redshift_schema_edl_external}.student_advisor_data
                WHERE academic_career_cd = 'UGRD' AND advisor_id ~ '[0-9]+'
            """)
            advisor_ids = [row['advisor_id'] for row in redshift.fetch(sql)]
        else:
            sql = resolve_sql_template_string("""
                SELECT DISTINCT advisor_sid
                FROM {redshift_schema_advisor_internal}.advisor_students
            """)
            advisor_ids = [row['advisor_sid'] for row in redshift.fetch(sql)]
        return _import_calnet_attributes(advisor_ids)


def _get_yesterdays_advisor_data():
    s3_sis_daily = get_s3_sis_sysadm_daily_path(datetime.now() - timedelta(days=1))
    if not s3.get_keys_with_prefix(s3_sis_daily):
        raise BackgroundJobError('No timely SIS S3 advisor data found')

    app.logger.info('Falling back to SIS S3 daily advisor data for yesterday')
    return s3_sis_daily


def _import_calnet_attributes(advisor_ids):
    calnet_attributes = calnet.client(app).search_csids(advisor_ids)
    calnet_row_count = len(calnet_attributes)
    if len(advisor_ids) != calnet_row_count:
        ldap_csids = [person['csid'] for person in calnet_attributes]
        missing = set(advisor_ids) - set(ldap_csids)
        app.logger.warning(f'Looked for {len(advisor_ids)} advisor CSIDs but only found {calnet_row_count} : missing {missing}')

    advisor_rows = []
    for index, a in enumerate(calnet_attributes):
        sid = a['csid']
        app.logger.info(f'CalNet import: Fetch attributes of advisor {sid} ({index + 1} of {calnet_row_count})')
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
    was_successful = redshift.execute(query)
    app.logger.info('Advisor attributes imported.' if was_successful else 'Error on Redshift copy: aborting job.')
    return was_successful
