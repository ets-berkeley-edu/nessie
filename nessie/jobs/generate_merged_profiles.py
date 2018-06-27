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


"""Logic for merged student profile generation."""


from itertools import groupby
import json
import operator

from flask import current_app as app
from nessie.externals import redshift
from nessie.jobs.background_job import BackgroundJob, resolve_sql_template
from nessie.lib import queries
from nessie.merged.sis_profile import get_merged_sis_profile
import psycopg2.sql


class GenerateMergedProfiles(BackgroundJob):

    destination_schema = app.config['REDSHIFT_SCHEMA_STUDENT']
    destination_schema_identifier = psycopg2.sql.Identifier(destination_schema)
    staging_schema = destination_schema + '_staging'
    staging_schema_identifier = psycopg2.sql.Identifier(staging_schema)

    def run(self):
        """Loop through all records stored in the Calnet external schema and write merged profile data to the internal student schema."""
        app.logger.info(f'Starting merged profile generation job...')
        staging_schema_ddl = resolve_sql_template('create_student_schema.template.sql', redshift_schema_student=self.staging_schema)
        if not redshift.execute_ddl_script(staging_schema_ddl):
            app.logger.error('Failed to create staging tables for merged profiles.')
            return False

        calnet_profiles = redshift.fetch(
            'SELECT ldap_uid, sid, first_name, last_name FROM {calnet_schema}.persons',
            calnet_schema=psycopg2.sql.Identifier(app.config['REDSHIFT_SCHEMA_CALNET']),
        )
        for sid, profile_group in groupby(calnet_profiles, operator.itemgetter('sid')):
            self.generate_merged_profile(sid, list(profile_group)[0])

        for table in ['student_profiles', 'student_academic_status', 'student_majors']:
            result = redshift.fetch(
                'SELECT COUNT(*) FROM {schema}.{table}',
                schema=self.staging_schema_identifier,
                table=psycopg2.sql.Identifier(table),
            )
            if result and result[0] and result[0]['count']:
                count = result[0]['count']
                app.logger.info(f'Verified population of staging table {table} ({count} rows).')
            else:
                app.logger.error(f'Failed to verify population of staging table {table}: aborting job.')
                return False

        redshift.execute('BEGIN TRANSACTION')
        recreate_schema_ddl = resolve_sql_template('create_student_schema.template.sql')
        if not redshift.execute_ddl_script(recreate_schema_ddl):
            app.logger.error('Failed to recreate schema for merged profiles.')
            redshift.execute('ROLLBACK TRANSACTION')
            return False

        for table in ['student_profiles', 'student_academic_status', 'student_majors']:
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

        redshift.execute('DROP SCHEMA {staging_schema} CASCADE', staging_schema=self.staging_schema_identifier)
        transaction_result = redshift.execute('COMMIT TRANSACTION')
        if not transaction_result:
            app.logger.error(f'Final transaction commit failed for {self.destination_schema}.')
            return False
        return True

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
        result = redshift.execute(
            'INSERT INTO {schema}.student_profiles (sid, profile) VALUES (%s, %s)',
            params=(sid, json.dumps(merged_profile)),
            schema=self.staging_schema_identifier,
        )
        if not result:
            app.logger.error(f'Insert failed into {self.staging_schema}.student_profiles: (sid={sid})')

        if not sis_profile:
            return

        level = sis_profile.get('level', {}).get('code')
        gpa = sis_profile.get('cumulativeGPA')
        units = sis_profile.get('cumulativeUnits')
        result = redshift.execute(
            'INSERT INTO {schema}.student_academic_status (sid, level, gpa, units) VALUES (%s, %s, %s, %s)',
            params=(sid, level, gpa, units),
            schema=self.staging_schema_identifier,
        )
        if not result:
            app.logger.error(f'Insert failed into {self.staging_schema}.student_academic_status:\
                              (sid={sid}, level={level}, gpa={gpa}, units={units})')
        for plan in sis_profile.get('plans', []):
            major = plan['description']
            result = redshift.execute(
                'INSERT INTO {schema}.student_majors (sid, major) VALUES (%s, %s)',
                params=(sid, major),
                schema=self.staging_schema_identifier,
            )
            if not result:
                app.logger.error(f'Insert failed into {self.staging_schema}.student_majors: (sid={sid}, major={major})')
