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
from nessie.jobs.import_term_gpas import ImportTermGpas
from nessie.lib.metadata import update_merged_feed_status
from nessie.lib.queries import get_advisee_student_profile_feeds, get_all_student_ids, get_successfully_backfilled_students
from nessie.lib.util import encoded_tsv_row, get_s3_sis_api_daily_path, resolve_sql_template_string, split_tsv_row
from nessie.merged.sis_profile import parse_merged_sis_profile
from nessie.merged.student_terms import generate_enrollment_terms_map
import psycopg2.sql

"""Logic for merged student profile and term generation."""


class GenerateMergedStudentFeeds(BackgroundJob):

    rds_schema = app.config['RDS_SCHEMA_STUDENT']
    redshift_schema = app.config['REDSHIFT_SCHEMA_STUDENT']
    redshift_schema_identifier = psycopg2.sql.Identifier(redshift_schema)
    staging_schema = redshift_schema + '_staging'
    staging_schema_identifier = psycopg2.sql.Identifier(staging_schema)

    def run(self, term_id=None, backfill_new_students=True):
        app.logger.info(f'Starting merged profile generation job (backfill={backfill_new_students}).')

        # This version of the code will always generate feeds for all-terms and all-advisees, but we
        # expect support for term-specific or backfill-specific feed generation will return soon.
        if term_id != 'all':
            app.logger.warn(f'Term-specific generation was requested for {term_id}, but all terms will be generated.')

        app.logger.info('Cleaning up old data...')
        redshift.execute('VACUUM; ANALYZE;')

        if backfill_new_students:
            status = self.generate_with_backfills()
        else:
            status = self.generate_feeds()

        # Clean up the workbench.
        redshift.execute('VACUUM; ANALYZE;')
        app.logger.info(f'Vacuumed and analyzed.')

        return status

    def generate_with_backfills(self):
        """For students without a previous backfill, collect or generate any missing data."""
        previous_backfills = {row['sid'] for row in get_successfully_backfilled_students()}
        sids = {row['sid'] for row in get_all_student_ids()}
        new_sids = list(sids.difference(previous_backfills))
        if new_sids:
            app.logger.info(f'Found {len(new_sids)} new students, will backfill all terms.')
            ImportTermGpas().run(csids=new_sids)
            update_merged_feed_status(None, new_sids, [])
            app.logger.info(f'Updated merged feed status for {len(new_sids)} students.')
        else:
            app.logger.info(f'No new students to backfill.')
        return self.generate_feeds()

    def generate_feeds(self):
        # Translation between canvas_user_id and UID/SID is needed to merge Canvas analytics data and SIS enrollment-based data.
        advisees_by_canvas_id = {}
        advisees_by_sid = {}
        self.successes = []
        self.failures = []
        profile_tables = self.generate_student_profile_tables(advisees_by_canvas_id, advisees_by_sid)
        terms_tables = self.generate_enrollment_terms_table(advisees_by_canvas_id, advisees_by_sid)
        self.refresh_all_from_staging(profile_tables + terms_tables)
        return f'Merged profile generation complete: {len(self.successes)} successes, {len(self.failures)} failures.'

    def generate_student_profile_tables(self, advisees_by_canvas_id, advisees_by_sid):
        # In-memory storage for generated feeds prior to TSV output.
        self.rows = {
            'student_profiles': [],
            'student_academic_status': [],
            'student_majors': [],
            'student_holds': [],
        }
        tables = ['student_profiles', 'student_academic_status', 'student_majors', 'student_holds']

        self.truncate_staging_tables(tables)

        all_student_feeds = get_advisee_student_profile_feeds()
        if not all_student_feeds:
            app.logger.warn(f'No profile feeds returned, aborting job.')
            return False
        count = len(all_student_feeds)
        app.logger.info(f'Will generate feeds for {count} students.')
        for index, student_feeds in enumerate(all_student_feeds):
            sid = student_feeds['sid']
            merged_profile = self.generate_student_profile_from_feeds(student_feeds)
            if merged_profile:
                canvas_user_id = student_feeds['canvas_user_id']
                if canvas_user_id:
                    advisees_by_canvas_id[canvas_user_id] = {'sid': sid, 'uid': student_feeds['ldap_uid']}
                    advisees_by_sid[sid] = {'canvas_user_id': canvas_user_id}
                self.successes.append(sid)
            else:
                self.failures.append(sid)
        self.write_all_to_staging(tables)
        return tables

    def generate_student_profile_from_feeds(self, feeds):
        sid = feeds['sid']
        uid = feeds['ldap_uid']
        if not uid:
            return
        sis_profile = parse_merged_sis_profile(feeds.get('sis_profile_feed'), feeds.get('degree_progress_feed'))
        merged_profile = {
            'sid': sid,
            'uid': uid,
            'firstName': feeds.get('first_name'),
            'lastName': feeds.get('last_name'),
            'name': ' '.join([feeds.get('first_name'), feeds.get('last_name')]),
            'canvasUserId': feeds.get('canvas_user_id'),
            'canvasUserName': feeds.get('canvas_user_name'),
            'sisProfile': sis_profile,
        }
        self.rows['student_profiles'].append(encoded_tsv_row([sid, json.dumps(merged_profile)]))

        if sis_profile:
            first_name = merged_profile['firstName'] or ''
            last_name = merged_profile['lastName'] or ''
            level = str(sis_profile.get('level', {}).get('code') or '')
            gpa = str(sis_profile.get('cumulativeGPA') or '')
            units = str(sis_profile.get('cumulativeUnits') or '')

            self.rows['student_academic_status'].append(encoded_tsv_row([sid, uid, first_name, last_name, level, gpa, units]))

            for plan in sis_profile.get('plans', []):
                self.rows['student_majors'].append(encoded_tsv_row([sid, plan['description']]))
            for hold in sis_profile.get('holds', []):
                self.rows['student_holds'].append(encoded_tsv_row([sid, json.dumps(hold)]))

        return merged_profile

    def generate_enrollment_terms_table(self, advisees_by_canvas_id, advisees_by_sid):
        self.rows['student_enrollment_terms'] = []
        tables = ['student_enrollment_terms']
        self.truncate_staging_tables(tables)

        enrollment_terms_map = generate_enrollment_terms_map(advisees_by_canvas_id, advisees_by_sid)

        for (sid, term_feeds) in enrollment_terms_map.items():
            for (term_id, term_feed) in term_feeds.items():
                self.rows['student_enrollment_terms'].append(encoded_tsv_row([sid, term_id, json.dumps(term_feed)]))

        self.write_all_to_staging(tables)
        self.rows['student_enrollment_terms'] = None
        return tables

    def truncate_staging_tables(self, tables):
        # Remove any old data from staging tables.
        for table in tables:
            redshift.execute(
                'TRUNCATE {schema}.{table}',
                schema=self.staging_schema_identifier,
                table=psycopg2.sql.Identifier(table),
            )

    def write_all_to_staging(self, tables):
        for table in tables:
            if not self.rows[table]:
                continue
            self.upload_to_staging(table)
            self.verify_table(table)
        return True

    def refresh_all_from_staging(self, tables):
        with redshift.transaction() as transaction:
            for table in tables:
                self.refresh_from_staging(table, None, None, transaction)
            if not transaction.commit():
                raise BackgroundJobError(f'Final transaction commit failed for {self.redshift_schema}.')
        with rds.transaction() as transaction:
            if self.refresh_rds_indexes(None, transaction):
                transaction.commit()
                app.logger.info('Refreshed RDS indexes.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Failed to refresh RDS indexes.')

    def refresh_from_staging(self, table, term_id, sids, transaction):
        # If our job is restricted to a particular term id or set of sids, then drop rows from the destination table
        # matching those restrictions. If there are no restrictions, the entire destination table can be truncated.
        delete_conditions = []
        delete_params = []
        if (term_id and table == 'student_enrollment_terms'):
            delete_conditions.append('term_id = %s')
            delete_params.append(term_id)
        if sids:
            delete_conditions.append('sid = ANY(%s)')
            delete_params.append(sids)
        if not delete_conditions:
            transaction.execute(
                'TRUNCATE {schema}.{table}',
                schema=self.redshift_schema_identifier,
                table=psycopg2.sql.Identifier(table),
            )
            app.logger.info(f'Truncated destination table {self.redshift_schema}.{table}.')
        else:
            delete_sql = 'DELETE FROM {schema}.{table} WHERE ' + ' AND '.join(delete_conditions)
            transaction.execute(
                delete_sql,
                schema=self.redshift_schema_identifier,
                table=psycopg2.sql.Identifier(table),
                params=tuple(delete_params),
            )
            app.logger.info(
                f'Deleted existing rows from destination table {self.redshift_schema}.{table} '
                f"(term_id={term_id or 'all'}, {len(sids) if sids else 'all'} sids).")

        # Load new data from the staging tables into the destination table.
        result = transaction.execute(
            'INSERT INTO {schema}.{table} (SELECT * FROM {staging_schema}.{table})',
            schema=self.redshift_schema_identifier,
            staging_schema=self.staging_schema_identifier,
            table=psycopg2.sql.Identifier(table),
        )
        if not result:
            transaction.rollback()
            raise BackgroundJobError(f'Failed to populate table {self.redshift_schema}.{table} from staging schema.')
        app.logger.info(f'Populated {self.redshift_schema}.{table} from staging schema.')

        # Truncate staging table.
        transaction.execute(
            'TRUNCATE {schema}.{table}',
            schema=self.staging_schema_identifier,
            table=psycopg2.sql.Identifier(table),
        )
        app.logger.info(f'Truncated staging table {self.staging_schema}.{table}.')

    def refresh_rds_indexes(self, sids, transaction):
        def delete_existing_rows(table):
            if sids:
                sql = f'DELETE FROM {self.rds_schema}.{table} WHERE sid = ANY(%s)'
                params = (sids,)
            else:
                sql = f'TRUNCATE {self.redshift_schema}.{table}'
                params = None
            return transaction.execute(sql, params)

        # TODO LOAD THE RDS INDEXES FROM REDSHIFT TABLES RATHER THAN IN-MEMORY STORAGE.
        if len(self.rows['student_academic_status']):
            if not delete_existing_rows('student_academic_status'):
                return False
            result = transaction.insert_bulk(
                f"""INSERT INTO {self.rds_schema}.student_academic_status
                    (sid, uid, first_name, last_name, level, gpa, units) VALUES %s""",
                [split_tsv_row(r) for r in self.rows['student_academic_status']],
            )
            if not result:
                return False
        if len(self.rows['student_majors']):
            if not delete_existing_rows('student_majors'):
                return False
            result = transaction.insert_bulk(
                f'INSERT INTO {self.rds_schema}.student_majors (sid, major) VALUES %s',
                [split_tsv_row(r) for r in self.rows['student_majors']],
            )
            if not result:
                return False
        return True

    def upload_to_staging(self, table):
        rows = self.rows[table]
        s3_key = f'{get_s3_sis_api_daily_path()}/staging_{table}.tsv'
        app.logger.info(f'Will stash {len(rows)} feeds in S3: {s3_key}')
        if not s3.upload_tsv_rows(rows, s3_key):
            raise BackgroundJobError('Error on S3 upload: aborting job.')

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
            raise BackgroundJobError('Error on Redshift copy: aborting job.')

    def verify_table(self, table):
        result = redshift.fetch(
            'SELECT COUNT(*) FROM {schema}.{table}',
            schema=self.staging_schema_identifier,
            table=psycopg2.sql.Identifier(table),
        )
        if result and result[0] and result[0]['count']:
            count = result[0]['count']
            app.logger.info(f'Verified population of staging table {table} ({count} rows).')
        else:
            raise BackgroundJobError(f'Failed to verify population of staging table {table}: aborting job.')
