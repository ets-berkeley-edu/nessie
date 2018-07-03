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
from nessie.externals import redshift
from nessie.jobs.background_job import BackgroundJob, resolve_sql_template, verify_external_schema
import psycopg2


"""Logic for ASC schema creation job."""


external_schema = app.config['REDSHIFT_SCHEMA_ASC_EXTERNAL']
internal_schema = app.config['REDSHIFT_SCHEMA_ASC']
internal_schema_identifier = psycopg2.sql.Identifier(internal_schema)


class CreateAscSchema(BackgroundJob):

    def run(self):
        app.logger.info(f'Starting ASC schema creation job...')
        redshift.drop_external_schema(external_schema)
        resolved_ddl = resolve_sql_template('create_asc_schema.template.sql')
        # TODO This DDL drops and recreates the internal schema before the external schema is verified. We
        # ought to set up proper staging in conjunction with verification. It's also possible that a persistent
        # external schema isn't needed.
        if redshift.execute_ddl_script(resolved_ddl):
            app.logger.info(f'ASC schema creation job completed.')
            if not verify_external_schema(external_schema, resolved_ddl):
                return False
        else:
            app.logger.error(f'ASC schema creation job failed.')
            return False
        asc_rows = redshift.fetch(
            'SELECT * FROM {schema}.students ORDER by sid',
            schema=internal_schema_identifier,
        )
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
            result = redshift.execute(
                'INSERT INTO {schema}.student_profiles (sid, profile) VALUES (%s, %s)',
                params=(sid, json.dumps(athletics_profile)),
                schema=internal_schema_identifier,
            )
            if not result:
                app.logger.error(f'Insert failed into {internal_schema}.student_profiles: (sid={sid})')
        return True
