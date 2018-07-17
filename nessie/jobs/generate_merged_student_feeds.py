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


"""Logic for merged student profile and term generation."""


from itertools import groupby
import json
import operator

from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.jobs.background_job import BackgroundJob, get_s3_sis_api_daily_path, resolve_sql_template, resolve_sql_template_string
from nessie.lib import berkeley, queries
from nessie.lib.analytics import mean_course_analytics_for_user
from nessie.merged.sis_profile import get_merged_sis_profile
from nessie.merged.student_terms import get_canvas_courses_feed, get_merged_enrollment_term
import psycopg2.sql


class GenerateMergedStudentFeeds(BackgroundJob):

    destination_schema = app.config['REDSHIFT_SCHEMA_STUDENT']
    destination_schema_identifier = psycopg2.sql.Identifier(destination_schema)
    staging_schema = destination_schema + '_staging'
    staging_schema_identifier = psycopg2.sql.Identifier(staging_schema)

    def run(self, term_id=None):
        """Loop through all records stored in the Calnet external schema and write merged student data to the internal student schema."""
        app.logger.info(f'Starting merged profile generation job... (term_id={term_id})')
        staging_schema_ddl = resolve_sql_template('create_student_schema.template.sql', redshift_schema_student=self.staging_schema)
        if not redshift.execute_ddl_script(staging_schema_ddl):
            app.logger.error('Failed to create staging tables for merged profiles.')
            return False

        # Before starting the merge, clean up after any recently run external import jobs.
        redshift.execute('VACUUM; ANALYZE;')

        calnet_profiles = redshift.fetch(
            'SELECT ldap_uid, sid, first_name, last_name FROM {calnet_schema}.persons',
            calnet_schema=psycopg2.sql.Identifier(app.config['REDSHIFT_SCHEMA_CALNET']),
        )

        # Jobs for non-current terms generate enrollment feeds only.
        if term_id and term_id != berkeley.current_term_id():
            tables = ['student_enrollment_terms']
        else:
            tables = ['student_profiles', 'student_academic_status', 'student_majors', 'student_enrollment_terms']

        self.rows = {
            'student_profiles': [],
            'student_academic_status': [],
            'student_majors': [],
            'student_enrollment_terms': [],
        }

        index = 1
        for sid, profile_group in groupby(calnet_profiles, operator.itemgetter('sid')):
            app.logger.info(f'Generating feeds for sid {sid} ({index} of {len(calnet_profiles)})')
            index += 1
            merged_profile = self.generate_or_fetch_merged_profile(term_id, sid, list(profile_group)[0])

            if merged_profile:
                self.generate_merged_enrollment_terms(merged_profile, term_id)

        for table in tables:
            self.upload_table(table)
            if not self.verify_table(table):
                return False

        redshift.execute('BEGIN TRANSACTION')

        if term_id is None:
            # With no term specified, truncate all student tables.
            for table in tables:
                redshift.execute(
                    'TRUNCATE {schema}.{table}',
                    schema=self.destination_schema_identifier,
                    table=psycopg2.sql.Identifier(table),
                )
        else:
            # Otherwise drop rows related to the specified term only.
            redshift.execute(
                'DELETE FROM {schema}.student_enrollment_terms WHERE term_id=%s',
                params=(term_id,),
                schema=self.destination_schema_identifier,
            )
            app.logger.info(f'Dropped term {term_id} from {self.destination_schema}.student_enrollment_terms.')
            if term_id is None or term_id == berkeley.current_term_id():
                redshift.execute('DELETE FROM {schema}.student_profiles', schema=self.destination_schema_identifier)
                app.logger.info(f'Dropped {self.destination_schema}.student_profiles.')
                redshift.execute('DELETE FROM {schema}.student_academic_status', schema=self.destination_schema_identifier)
                app.logger.info(f'Dropped {self.destination_schema}.student_academic_status.')
                redshift.execute('DELETE FROM {schema}.student_majors', schema=self.destination_schema_identifier)
                app.logger.info(f'Dropped {self.destination_schema}.student_majors.')

        for table in tables:
            result = redshift.execute(
                'INSERT INTO {schema}.{table} (SELECT * FROM {staging_schema}.{table})',
                schema=self.destination_schema_identifier,
                staging_schema=self.staging_schema_identifier,
                table=psycopg2.sql.Identifier(table),
            )
            if not result:
                app.logger.error(f'Failed to populate table {self.destination_schema}.{table} from staging schema.')
                redshift.execute('ROLLBACK TRANSACTION')
                return False
            app.logger.info(f'Populated {self.destination_schema}.{table} from staging schema.')

        redshift.execute('DROP SCHEMA {staging_schema} CASCADE', staging_schema=self.staging_schema_identifier)
        transaction_result = redshift.execute('COMMIT TRANSACTION')
        if not transaction_result:
            app.logger.error(f'Final transaction commit failed for {self.destination_schema}.')
            return False
        app.logger.info(f'Dropped {self.staging_schema}.')

        # Clean up the workbench.
        redshift.execute('VACUUM')
        redshift.execute('ANALYZE')
        app.logger.info(f'Vacuumed and analyzed. Job complete.')

        return True

    def generate_or_fetch_merged_profile(self, term_id, sid, calnet_profile):
        merged_profile = None
        if term_id is None or term_id == berkeley.current_term_id():
            merged_profile = self.generate_merged_profile(sid, calnet_profile)
        else:
            profile_result = redshift.fetch(
                'SELECT profile FROM {schema}.student_profiles WHERE sid = %s',
                params=(sid,),
                schema=self.destination_schema_identifier,
            )
            merged_profile = profile_result and profile_result[0] and json.loads(profile_result[0].get('profile', '{}'))
            if not merged_profile:
                merged_profile = self.generate_merged_profile(sid, calnet_profile)
        if not merged_profile:
            app.logger.error(f'Failed to generate merged profile for sid {sid}.')
        return merged_profile

    def generate_merged_profile(self, sid, calnet_profile):
        uid = calnet_profile.get('ldap_uid')
        if not uid:
            return
        canvas_user_result = queries.get_user_for_uid(uid)
        canvas_profile = canvas_user_result[0] if canvas_user_result else {}
        sis_profile = get_merged_sis_profile(sid)
        merged_profile = {
            'sid': sid,
            'uid': uid,
            'firstName': calnet_profile.get('first_name'),
            'lastName': calnet_profile.get('last_name'),
            'name': ' '.join([calnet_profile.get('first_name'), calnet_profile.get('last_name')]),
            'canvasUserId': canvas_profile.get('canvas_id'),
            'canvasUserName': canvas_profile.get('name'),
            'sisProfile': sis_profile,
        }
        self.rows['student_profiles'].append('\t'.join([str(sid), json.dumps(merged_profile)]))

        if sis_profile:
            first_name = merged_profile['firstName'] or ''
            last_name = merged_profile['lastName'] or ''
            level = str(sis_profile.get('level', {}).get('code') or '')
            gpa = str(sis_profile.get('cumulativeGPA') or '')
            units = str(sis_profile.get('cumulativeUnits') or '')

            self.rows['student_academic_status'].append('\t'.join([str(sid), str(uid), first_name, last_name, level, gpa, units]))

            for plan in sis_profile.get('plans', []):
                self.rows['student_majors'].append('\t'.join([str(sid), plan['description']]))

        return merged_profile

    def generate_merged_enrollment_terms(self, merged_profile, term_id=None):
        sis_profile = merged_profile.get('sisProfile') or {}
        matriculation_term = sis_profile.get('matriculation')
        terms_for_student = berkeley.reverse_terms_until(matriculation_term or app.config['EARLIEST_TERM'])
        term_ids_for_student = [berkeley.sis_term_id_for_name(t) for t in terms_for_student]
        if term_id and term_id not in term_ids_for_student:
            return

        uid = merged_profile.get('uid')
        sid = merged_profile.get('sid')
        canvas_user_id = merged_profile.get('canvasUserId')
        canvas_courses_feed = get_canvas_courses_feed(uid)

        term_ids = [term_id] if term_id else term_ids_for_student
        for term_id in term_ids:
            term_feed = get_merged_enrollment_term(canvas_courses_feed, uid, sid, term_id)
            if term_feed and (len(term_feed['enrollments']) or len(term_feed['unmatchedCanvasSites'])):
                # Rebuild our Canvas courses list to remove any courses that were screened out during association (for instance,
                # dropped or athletic enrollments).
                canvas_courses = []
                for enrollment in term_feed.get('enrollments', []):
                    canvas_courses += enrollment['canvasSites']
                canvas_courses += term_feed.get('unmatchedCanvasSites', [])
                # Decorate the Canvas courses list with per-course statistics and return summary statistics.
                term_feed['analytics'] = mean_course_analytics_for_user(canvas_courses, canvas_user_id)
                self.rows['student_enrollment_terms'].append('\t'.join([str(sid), str(term_id), json.dumps(term_feed)]))

    def upload_table(self, table):
        rows = self.rows[table]
        s3_key = f'{get_s3_sis_api_daily_path()}/staging_{table}.tsv'
        app.logger.info(f'Will stash {len(rows)} feeds in S3: {s3_key}')
        if not s3.upload_data('\n'.join(rows), s3_key):
            app.logger.error('Error on S3 upload: aborting job.')
            return False

        app.logger.info('Will copy S3 feeds into Redshift...')
        query = resolve_sql_template_string(
            """
            COPY {staging_schema}.{table}
                FROM '{loch_s3_sis_api_data_path}/staging_{table}.tsv'
                IAM_ROLE '{redshift_iam_role}'
                DELIMITER '\\t';
            """,
            staging_schema=self.staging_schema,
            table=table,
        )
        if not redshift.execute(query):
            app.logger.error('Error on Redshift copy: aborting job.')
            return False

    def verify_table(self, table):
        result = redshift.fetch(
            'SELECT COUNT(*) FROM {schema}.{table}',
            schema=self.staging_schema_identifier,
            table=psycopg2.sql.Identifier(table),
        )
        if result and result[0] and result[0]['count']:
            count = result[0]['count']
            app.logger.info(f'Verified population of staging table {table} ({count} rows).')
            return True
        else:
            app.logger.error(f'Failed to verify population of staging table {table}: aborting job.')
            return False
