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


from datetime import datetime
from itertools import groupby
import json
import operator

from flask import current_app as app
from nessie.externals import rds, redshift, s3
from nessie.jobs.background_job import BackgroundJob
from nessie.jobs.import_term_gpas import ImportTermGpas
from nessie.lib import berkeley, queries
from nessie.lib.analytics import get_relative_submission_counts, mean_course_analytics_for_user
from nessie.lib.metadata import update_merged_feed_status
from nessie.lib.queries import get_all_student_ids, get_successfully_backfilled_students
from nessie.lib.util import get_s3_sis_api_daily_path, resolve_sql_template_string, split_tsv_row
from nessie.merged.sis_profile import get_holds, get_merged_sis_profile
from nessie.merged.student_terms import get_canvas_courses_feed, get_merged_enrollment_terms, merge_canvas_site_map
import psycopg2.sql


class GenerateMergedStudentFeeds(BackgroundJob):

    destination_schema = app.config['REDSHIFT_SCHEMA_STUDENT']
    destination_schema_identifier = psycopg2.sql.Identifier(destination_schema)
    staging_schema = destination_schema + '_staging'
    staging_schema_identifier = psycopg2.sql.Identifier(staging_schema)

    def run(self, term_id=None, backfill_new_students=False):
        app.logger.info(f'Starting merged profile generation job (term_id={term_id}, backfill={backfill_new_students}).')

        app.logger.info('Cleaning up old data...')
        redshift.execute('VACUUM; ANALYZE;')

        if backfill_new_students:
            status = ''
            previous_backfills = {row['sid'] for row in get_successfully_backfilled_students()}
            sids = {row['sid'] for row in get_all_student_ids()}
            old_sids = sids.intersection(previous_backfills)
            new_sids = sids.difference(previous_backfills)
            # Any students without a previous backfill will have feeds generated for all terms. Students with a previous
            # backfill get an update for the requested term only.
            if len(new_sids):
                app.logger.info(f'Found {len(new_sids)} new students, will backfill all terms.')
                ImportTermGpas().run(csids=new_sids)
                backfill_status = self.generate_feeds(sids=list(new_sids))
                if not backfill_status:
                    app.logger.warn('Backfill job aborted, will continue with non-backfill job.')
                    backfill_status = 'aborted'
                else:
                    app.logger.info(f'Backfill complete.')
                status += f'Backfill: {backfill_status}; non-backfill: '
                app.logger.info(f'Will continue merged feed job for {len(old_sids)} previously backfilled students.')
            continuation_status = self.generate_feeds(sids=list(old_sids), term_id=term_id)
            if not continuation_status:
                return False
            status += continuation_status
        else:
            status = self.generate_feeds(term_id)

        # Clean up the workbench.
        redshift.execute('VACUUM; ANALYZE;')
        app.logger.info(f'Vacuumed and analyzed.')

        return status

    def generate_feeds(self, term_id=None, sids=None):
        """Loop through all records stored in the Calnet external schema and write merged student data to the internal student schema."""
        calnet_profiles = self.fetch_calnet_profiles(sids)

        # Jobs targeted toward a specific sid set (such as backfills) may return no CalNet profiles. Warn, don't error.
        if not calnet_profiles:
            app.logger.warn(f'No CalNet profiles returned, aborting job. (sids={sids})')
            return False

        # Jobs for non-current terms generate enrollment feeds only.
        if term_id and term_id != berkeley.current_term_id():
            tables = ['student_enrollment_terms']
        else:
            tables = ['student_profiles', 'student_academic_status', 'student_majors', 'student_enrollment_terms', 'student_holds']

        # In-memory storage for generated feeds prior to TSV output.
        self.rows = {
            'student_profiles': [],
            'student_academic_status': [],
            'student_majors': [],
            'student_enrollment_terms': [],
            'student_holds': [],
        }

        # Track the results of course-level queries to avoid requerying.
        self.canvas_site_map = {}

        # Remove any old data from staging tables.
        for table in tables:
            redshift.execute(
                'TRUNCATE {schema}.{table}',
                schema=self.staging_schema_identifier,
                table=psycopg2.sql.Identifier(table),
            )

        app.logger.info(f'Will generate feeds for {len(calnet_profiles)} students (term_id={term_id}).')
        successes = []
        failures = []
        index = 1
        for sid, profile_group in groupby(calnet_profiles, operator.itemgetter('sid')):
            app.logger.info(f'Generating feeds for sid {sid} ({index} of {len(calnet_profiles)})')
            index += 1
            merged_profile = self.generate_or_fetch_merged_profile(term_id, sid, list(profile_group)[0])
            if merged_profile:
                self.generate_merged_enrollment_terms(merged_profile, term_id)
                self.parse_holds(sid)
                successes.append(sid)
            else:
                failures.append(sid)

        for table in tables:
            if not self.rows[table]:
                continue
            self.upload_to_staging(table)
            if not self.verify_table(table):
                return False

        with redshift.transaction() as transaction:
            for table in tables:
                if not self.refresh_from_staging(table, term_id, sids, transaction):
                    app.logger.error(f'Failed to refresh {self.destination_schema}.{table} from staging.')
                    return False
            if not transaction.commit():
                app.logger.error(f'Final transaction commit failed for {self.destination_schema}.')
                return False

        with rds.transaction() as transaction:
            if self.refresh_rds_indexes(sids, transaction):
                transaction.commit()
                app.logger.info('Refreshed RDS indexes.')
            else:
                transaction.rollback()
                app.logger.error('Failed to refresh RDS indexes.')
                return False

        update_merged_feed_status(term_id, successes, failures)
        app.logger.info(f'Updated merged feed status.')

        return f'Merged profile generation complete: {len(successes)} successes, {len(failures)} failures.'

    def fetch_calnet_profiles(self, sids=None):
        if sids:
            profiles = redshift.fetch(
                'SELECT ldap_uid, sid, first_name, last_name FROM {calnet_schema}.persons WHERE sid = ANY(%s)',
                calnet_schema=psycopg2.sql.Identifier(app.config['REDSHIFT_SCHEMA_CALNET']),
                params=(sids,),
            )
        else:
            profiles = redshift.fetch(
                'SELECT ldap_uid, sid, first_name, last_name FROM {calnet_schema}.persons',
                calnet_schema=psycopg2.sql.Identifier(app.config['REDSHIFT_SCHEMA_CALNET']),
            )
        return profiles

    def fetch_term_gpas(self, sid):
        return redshift.fetch(
            'SELECT term_id, gpa, units_taken_for_gpa FROM {student_schema}.student_term_gpas WHERE sid = %s',
            student_schema=psycopg2.sql.Identifier(app.config['REDSHIFT_SCHEMA_STUDENT']),
            params=(sid,),
        )

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
        app.logger.debug(f'Generating merged profile for SID {sid}')
        ts = datetime.now().timestamp()
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

        app.logger.debug(f'Merged profile generation complete for SID {sid} in {datetime.now().timestamp() - ts} seconds.')
        return merged_profile

    def generate_merged_enrollment_terms(self, merged_profile, term_id=None):
        if term_id and term_id not in berkeley.reverse_term_ids():
            return
        elif term_id:
            term_ids = [term_id]
        else:
            term_ids = berkeley.reverse_term_ids()

        uid = merged_profile.get('uid')
        sid = merged_profile.get('sid')
        canvas_user_id = merged_profile.get('canvasUserId')

        canvas_courses_feed = get_canvas_courses_feed(uid)
        merge_canvas_site_map(self.canvas_site_map, canvas_courses_feed)
        terms_feed = get_merged_enrollment_terms(uid, sid, term_ids, canvas_courses_feed, self.canvas_site_map)
        term_gpas = self.fetch_term_gpas(sid)

        relative_submission_counts = get_relative_submission_counts(canvas_user_id)

        for term_id in term_ids:
            app.logger.debug(f'Generating merged enrollment term (uid={uid}, sid={sid}, term_id={term_id})')
            ts = datetime.now().timestamp()
            term_feed = terms_feed.get(term_id)
            if term_feed and (len(term_feed['enrollments']) or len(term_feed['unmatchedCanvasSites'])):
                term_gpa = next((t for t in term_gpas if t['term_id'] == term_id), None)
                if term_gpa:
                    term_feed['termGpa'] = {
                        'gpa': float(term_gpa['gpa']),
                        'unitsTakenForGpa': float(term_gpa['units_taken_for_gpa']),
                    }
                # Rebuild our Canvas courses list to remove any courses that were screened out during association (for instance,
                # dropped or athletic enrollments).
                canvas_courses = []
                for enrollment in term_feed.get('enrollments', []):
                    canvas_courses += enrollment['canvasSites']
                canvas_courses += term_feed.get('unmatchedCanvasSites', [])
                # Decorate the Canvas courses list with per-course statistics and return summary statistics.
                app.logger.debug(f'Generating enrollment term analytics (uid={uid}, sid={sid}, term_id={term_id})')
                term_feed['analytics'] = mean_course_analytics_for_user(
                    canvas_courses,
                    canvas_user_id,
                    relative_submission_counts,
                    self.canvas_site_map,
                )
                self.rows['student_enrollment_terms'].append('\t'.join([str(sid), str(term_id), json.dumps(term_feed)]))
            app.logger.debug(
                f'Enrollment term merge complete (uid={uid}, sid={sid}, term_id={term_id}, '
                f'{datetime.now().timestamp() - ts} seconds)'
            )

    def parse_holds(self, sid):
        holds = get_holds(sid) or []
        for hold in holds:
            self.rows['student_holds'].append('\t'.join([str(sid), json.dumps(hold)]))

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
                schema=self.destination_schema_identifier,
                table=psycopg2.sql.Identifier(table),
            )
            app.logger.info(f'Truncated destination table {self.destination_schema}.{table}.')
        else:
            delete_sql = 'DELETE FROM {schema}.{table} WHERE ' + ' AND '.join(delete_conditions)
            transaction.execute(
                delete_sql,
                schema=self.destination_schema_identifier,
                table=psycopg2.sql.Identifier(table),
                params=tuple(delete_params),
            )
            app.logger.info(
                f'Deleted existing rows from destination table {self.destination_schema}.{table} '
                f"(term_id={term_id or 'all'}, {len(sids) if sids else 'all'} sids)."
            )

        # Load new data from the staging tables into the destination table.
        result = transaction.execute(
            'INSERT INTO {schema}.{table} (SELECT * FROM {staging_schema}.{table})',
            schema=self.destination_schema_identifier,
            staging_schema=self.staging_schema_identifier,
            table=psycopg2.sql.Identifier(table),
        )
        if not result:
            app.logger.error(f'Failed to populate table {self.destination_schema}.{table} from staging schema.')
            transaction.rollback()
            return False
        app.logger.info(f'Populated {self.destination_schema}.{table} from staging schema.')

        # Truncate staging table.
        transaction.execute(
            'TRUNCATE {schema}.{table}',
            schema=self.staging_schema_identifier,
            table=psycopg2.sql.Identifier(table),
        )
        app.logger.info(f'Truncated staging table {self.staging_schema}.{table}.')
        return True

    def refresh_rds_indexes(self, sids, transaction):
        def delete_existing_rows(table):
            if sids:
                sql = f'DELETE FROM {self.destination_schema}.{table} WHERE sid = ANY(%s)'
                params = (sids,)
            else:
                sql = f'TRUNCATE {self.destination_schema}.{table}'
                params = None
            return transaction.execute(sql, params)

        if len(self.rows['student_academic_status']):
            if not delete_existing_rows('student_academic_status'):
                return False
            result = transaction.insert_bulk(
                f"""INSERT INTO {self.destination_schema}.student_academic_status
                    (sid, uid, first_name, last_name, level, gpa, units) VALUES %s""",
                [tuple(split_tsv_row(r)) for r in self.rows['student_academic_status']],
            )
            if not result:
                return False
        if len(self.rows['student_majors']):
            if not delete_existing_rows('student_majors'):
                return False
            result = transaction.insert_bulk(
                f'INSERT INTO {self.destination_schema}.student_majors (sid, major) VALUES %s',
                [tuple(r.split('\t')) for r in self.rows['student_majors']],
            )
            if not result:
                return False
        return True

    def upload_to_staging(self, table):
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
