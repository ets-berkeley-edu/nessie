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
import json
from operator import itemgetter
from tempfile import TemporaryFile

from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.berkeley import degree_program_url_for_major, feature_flag_edl
from nessie.lib.queries import get_demographics
from nessie.lib.util import get_s3_edl_daily_path, resolve_sql_template, write_to_tsv_file
from nessie.merged.student_demographics import GENDER_CODE_MAP, merge_from_details, UNDERREPRESENTED_GROUPS
import pandas as pd

"""Logic for EDL SIS schema creation job."""


class CreateEdlSchema(BackgroundJob):

    external_schema = app.config['REDSHIFT_SCHEMA_EDL_EXTERNAL']
    internal_schema = app.config['REDSHIFT_SCHEMA_EDL']

    def run(self):
        if feature_flag_edl():
            app.logger.info('Starting EDL schema creation job...')
            self.create_schema()
            self.generate_feeds()
            return 'EDL schema creation job completed.'
        else:
            return 'Skipped EDL schema creation job because feature-flag is false.'

    def create_schema(self):
        app.logger.info('Executing SQL...')
        template_sql = 'create_edl_schema.template.sql'
        resolved_ddl = resolve_sql_template(template_sql)
        if not redshift.execute_ddl_script(resolved_ddl):
            raise BackgroundJobError('EDL SIS schema creation job failed.')
        # Create staging schema
        resolved_ddl_staging = resolve_sql_template(
            template_sql,
            redshift_schema_edl=f'{self.internal_schema}_staging',
        )
        if redshift.execute_ddl_script(resolved_ddl_staging):
            app.logger.info(f"Schema '{self.internal_schema}_staging' found or created.")
        else:
            raise BackgroundJobError(f'{self.internal_schema} schema creation failed.')

        app.logger.info('Redshift EDL schema created.')

    def generate_feeds(self):
        self.generate_academic_plans_feeds()
        self.generate_degree_progress_feeds()
        self.generate_demographics_feeds()

    def generate_academic_plans_feeds(self):
        app.logger.info('Staging academic plans feeds...')
        rows = redshift.fetch(f'SELECT * FROM {self.internal_schema}.student_academic_plan_index ORDER by sid')
        with TemporaryFile() as feeds:
            for sid, rows_for_student in groupby(rows, itemgetter('sid')):
                rows_for_student = list(rows_for_student)
                feed = self.generate_academic_plans_feed(rows_for_student)
                write_to_tsv_file(feeds, [sid, json.dumps(feed)])
            self._upload_file_to_staging('student_academic_plans', feeds)

    def generate_academic_plans_feed(self, rows_for_student):
        majors = []
        minors = []
        subplans = []
        for row in rows_for_student:
            plan = {
                'degreeProgramUrl': degree_program_url_for_major(row['plan']),
                'description': row['plan'],
                'program': row['program'],
                'status': 'Active' if row['status'] == 'Active in Program' else row['status'],
            }
            if 'MIN' == row['plan_type']:
                minors.append(plan)
            else:
                majors.append(plan)
            if row['subplan']:
                subplans.append(row['subplan'])
        return {
            'plans': majors,
            'plansMinor': minors,
            'subplans': subplans,
        }

    def generate_degree_progress_feeds(self):
        app.logger.info('Staging degree progress feeds...')
        table = 'student_degree_progress'
        rows = redshift.fetch(f'SELECT * FROM {self.internal_schema}.{table}_index ORDER by sid')
        with TemporaryFile() as feeds:
            for sid, rows_for_student in groupby(rows, itemgetter('sid')):
                rows_for_student = list(rows_for_student)
                report_date = rows_for_student[0].get('report_date')
                feed = {
                    'reportDate': report_date.strftime('%Y-%m-%d'),
                    'requirements': {
                        row.get('requirement'): {
                            'name': row.get('requirement_desc'), 'status': row.get('status'),
                        } for row in rows_for_student
                    },
                }
                write_to_tsv_file(feeds, [sid, json.dumps(feed)])
            self._upload_file_to_staging(table, feeds)

    def generate_demographics_feeds(self):
        app.logger.info('Staging demographics feeds...')
        df_chunks = []
        limit = 10000
        offset = 0
        while True:
            rows = get_demographics(limit=limit, offset=offset)
            df_chunks.append(pd.DataFrame(rows))
            if len(rows) < limit:
                break
            offset += limit
        df = pd.concat(df_chunks, ignore_index=True)

        with TemporaryFile() as feeds:
            for sid, student in df.groupby('sid'):
                # TODO: Prefer gender identity once available (NS-1073)
                gender = student['gender'].drop_duplicates().dropna()
                if gender.count() > 1:
                    app.logger.warn(f'Found more than one gender for SID {sid}; selecting only the first.')
                ethnic_map = student.groupby(['ethnic_group'])['ethnicity'].agg(set).to_dict()
                ethnicities = self.simplified_ethnicities(ethnic_map)
                nationalities = student['citizenship_country'].dropna().unique().tolist()
                visa = student[['visa_type', 'visa_status']].drop_duplicates().to_dict('r')
                if len(visa) > 1:
                    app.logger.warn(f'Found more than one visa for SID {sid}; selecting only the first.')
                feed = {
                    'gender': GENDER_CODE_MAP[gender.iat[0]],
                    'ethnicities': ethnicities,
                    'nationalities': nationalities,
                    'underrepresented': not UNDERREPRESENTED_GROUPS.isdisjoint(ethnic_map.keys()),
                    'visa': visa[0],
                }
                write_to_tsv_file(feeds, [sid, json.dumps(feed)])
            self._upload_file_to_staging('student_demographics', feeds)

    def simplified_ethnicities(self, ethnic_map):
        simpler_list = []
        for group in ethnic_map.keys():
            merge_from_details(simpler_list, group, ethnic_map[group])
        if not simpler_list:
            simpler_list.append('Not Specified')
        return sorted(simpler_list)

    def _upload_file_to_staging(self, table, _file):
        tsv_filename = f'staging_{table}.tsv'
        s3_key = f'{get_s3_edl_daily_path()}/{tsv_filename}'

        app.logger.info(f'Will stash {table} feeds in S3: {s3_key}')
        if not s3.upload_file(_file, s3_key):
            raise BackgroundJobError('Error on S3 upload: aborting job.')

        app.logger.info('Will copy S3 feeds into Redshift...')
        if not redshift.copy_tsv_from_s3(f'{self.internal_schema}.{table}', s3_key):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')
