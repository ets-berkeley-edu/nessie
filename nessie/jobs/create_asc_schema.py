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

from itertools import groupby
import json
import operator

from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.jobs.background_job import BackgroundJob, get_s3_asc_daily_path, resolve_sql_template_string
import psycopg2


"""Logic for ASC schema creation job."""


asc_schema = app.config['REDSHIFT_SCHEMA_ASC']
asc_schema_identifier = psycopg2.sql.Identifier(asc_schema)


class CreateAscSchema(BackgroundJob):

    def run(self):
        app.logger.info(f'Starting ASC schema creation job...')
        asc_rows = redshift.fetch(
            'SELECT * FROM {schema}.students ORDER by sid',
            schema=asc_schema_identifier,
        )

        profile_rows = []

        for sid, rows_for_student in groupby(asc_rows, operator.itemgetter('sid')):
            rows_for_student = list(rows_for_student)
            athletics_profile = {
                'athletics': [],
                'inIntensiveCohort': rows_for_student[0]['intensive'],
                'isActiveAsc': rows_for_student[0]['active'],
                'statusAsc': rows_for_student[0]['status_asc'],
            }
            for row in rows_for_student:
                athletics_profile['athletics'].append({
                    'groupCode': row['group_code'],
                    'groupName': row['group_name'],
                    'name': row['group_name'],
                    'teamCode': row['team_code'],
                    'teamName': row['team_name'],
                })

            profile_rows.append('\t'.join([str(sid), json.dumps(athletics_profile)]))

        s3_key = f'{get_s3_asc_daily_path()}/athletics_profiles.tsv'
        app.logger.info(f'Will stash {len(profile_rows)} feeds in S3: {s3_key}')
        if not s3.upload_data('\n'.join(profile_rows), s3_key):
            app.logger.error('Error on S3 upload: aborting job.')
            return False

        app.logger.info('Will copy S3 feeds into Redshift...')
        query = resolve_sql_template_string(
            """
            TRUNCATE {redshift_schema_asc}.student_profiles;
            COPY {redshift_schema_asc}.student_profiles
                FROM '{loch_s3_asc_data_path}/athletics_profiles.tsv'
                IAM_ROLE '{redshift_iam_role}'
                DELIMITER '\\t';
            VACUUM;
            ANALYZE;
            """,
        )
        if not redshift.execute(query):
            app.logger.error('Error on Redshift copy: aborting job.')
            return False

        app.logger.info('ASC schema creation complete.')
        return True
