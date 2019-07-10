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
from time import sleep

from flask import current_app as app
from nessie.externals import rds, redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.jobs.generate_merged_enrollment_term import GenerateMergedEnrollmentTerm
from nessie.jobs.import_term_gpas import ImportTermGpas
from nessie.lib.berkeley import future_term_ids, legacy_term_ids, reverse_term_ids
from nessie.lib.metadata import get_merged_enrollment_term_job_status, queue_merged_enrollment_term_jobs, update_merged_feed_status
from nessie.lib.queries import get_advisee_student_profile_feeds, get_all_student_ids, get_successfully_backfilled_students
from nessie.lib.util import encoded_tsv_row, split_tsv_row
from nessie.merged.sis_profile import parse_merged_sis_profile
from nessie.merged.student_terms import generate_student_term_maps
from nessie.models import student_schema

"""Logic for merged student profile and term generation."""


class GenerateMergedStudentFeeds(BackgroundJob):

    rds_schema = app.config['RDS_SCHEMA_STUDENT']
    rds_dblink_to_redshift = app.config['REDSHIFT_DATABASE'] + '_redshift'
    redshift_schema = app.config['REDSHIFT_SCHEMA_STUDENT']

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
            ImportTermGpas().run(sids=new_sids)
            update_merged_feed_status(new_sids, [])
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

        (enrollment_terms_map, canvas_site_map) = generate_student_term_maps(advisees_by_sid)

        feed_path = app.config['LOCH_S3_BOAC_ANALYTICS_DATA_PATH'] + '/feeds/'
        s3.upload_json(advisees_by_canvas_id, feed_path + 'advisees_by_canvas_id.json')

        for term_id in (future_term_ids() + legacy_term_ids()):
            GenerateMergedEnrollmentTerm().refresh_student_enrollment_term(term_id, enrollment_terms_map.get(term_id, {}))

        # Avoid processing Canvas analytics data for future terms and pre-CS terms.
        canvas_integrated_term_ids = reverse_term_ids()
        for term_id in canvas_integrated_term_ids:
            s3.upload_json(enrollment_terms_map.get(term_id, {}), feed_path + f'enrollment_term_map_{term_id}.json')
            s3.upload_json(canvas_site_map.get(term_id, {}), feed_path + f'canvas_site_map_{term_id}.json')

        app.logger.info(f'Will queue analytics generation for {len(canvas_integrated_term_ids)} terms on worker nodes.')
        result = queue_merged_enrollment_term_jobs(self.job_id, canvas_integrated_term_ids)
        if not result:
            raise BackgroundJobError('Failed to queue enrollment term jobs.')

        student_schema.refresh_all_from_staging(profile_tables)
        with rds.transaction() as transaction:
            if self.refresh_rds_indexes(None, transaction):
                transaction.commit()
                app.logger.info('Refreshed RDS indexes.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Failed to refresh RDS indexes.')

        app.logger.info('Profile generation complete; waiting for enrollment term generation to finish.')

        while True:
            sleep(1)
            enrollment_results = get_merged_enrollment_term_job_status(self.job_id)
            if not enrollment_results:
                raise BackgroundJobError('Failed to refresh RDS indexes.')
            any_pending_job = next((row for row in enrollment_results if row['status'] == 'created' or row['status'] == 'started'), None)
            if not any_pending_job:
                break

        app.logger.info('Refreshing enrollment terms in RDS.')
        with rds.transaction() as transaction:
            if self.refresh_rds_enrollment_terms(None, transaction):
                transaction.commit()
                app.logger.info('Refreshed RDS enrollment terms.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Failed to refresh RDS enrollment terms.')

        status_string = f'Generated merged profiles ({len(self.successes)} successes, {len(self.failures)} failures).'
        errored = False
        for row in enrollment_results:
            status_string += f" {row['details']}"
            if row['status'] == 'error':
                errored = True

        student_schema.truncate_staging_table('student_enrollment_terms')
        if errored:
            raise BackgroundJobError(status_string)
        else:
            return status_string

    def generate_student_profile_tables(self, advisees_by_canvas_id, advisees_by_sid):
        # In-memory storage for generated feeds prior to TSV output.
        self.rows = {
            'student_profiles': [],
            'student_academic_status': [],
            'student_majors': [],
            'student_holds': [],
        }
        tables = ['student_profiles', 'student_academic_status', 'student_majors', 'student_holds']

        for table in tables:
            student_schema.truncate_staging_table(table)

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
        for table in tables:
            if self.rows[table]:
                student_schema.write_to_staging(table, self.rows[table])
        return tables

    def generate_student_profile_from_feeds(self, feeds):
        sid = feeds['sid']
        uid = feeds['ldap_uid']
        if not uid:
            return
        sis_profile = parse_merged_sis_profile(
            feeds.get('sis_profile_feed'),
            feeds.get('degree_progress_feed'),
            feeds.get('last_registration_feed'),
        )
        demographics = feeds.get('demographics_feed') and json.loads(feeds.get('demographics_feed'))
        merged_profile = {
            'sid': sid,
            'uid': uid,
            'firstName': feeds.get('first_name'),
            'lastName': feeds.get('last_name'),
            'name': ' '.join([feeds.get('first_name'), feeds.get('last_name')]),
            'canvasUserId': feeds.get('canvas_user_id'),
            'canvasUserName': feeds.get('canvas_user_name'),
            'sisProfile': sis_profile,
            'demographics': demographics,
        }
        self.rows['student_profiles'].append(encoded_tsv_row([sid, json.dumps(merged_profile)]))

        if sis_profile:
            first_name = merged_profile['firstName'] or ''
            last_name = merged_profile['lastName'] or ''
            level = str(sis_profile.get('level', {}).get('code') or '')
            gpa = str(sis_profile.get('cumulativeGPA') or '')
            units = str(sis_profile.get('cumulativeUnits') or '')
            transfer = str(sis_profile.get('transfer') or False)
            expected_grad_term = str(sis_profile.get('expectedGraduationTerm', {}).get('id') or '')

            self.rows['student_academic_status'].append(
                encoded_tsv_row([sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term]),
            )

            for plan in sis_profile.get('plans', []):
                self.rows['student_majors'].append(encoded_tsv_row([sid, plan['description']]))
            for hold in sis_profile.get('holds', []):
                self.rows['student_holds'].append(encoded_tsv_row([sid, json.dumps(hold)]))

        return merged_profile

    def refresh_rds_indexes(self, sids, transaction):
        if len(self.rows['student_academic_status']):
            if not self._delete_rds_rows('student_academic_status', sids, transaction):
                return False
            if not self._refresh_rds_academic_status(transaction):
                return False
            if not self._delete_rds_rows('student_names', sids, transaction):
                return False
            if not self._refresh_rds_names(transaction):
                return False
        if len(self.rows['student_majors']):
            if not self._delete_rds_rows('student_majors', sids, transaction):
                return False
            if not self._refresh_rds_majors(transaction):
                return False
        if len(self.rows['student_profiles']):
            if not self._delete_rds_rows('student_profiles', sids, transaction):
                return False
            if not self._refresh_rds_profiles(transaction):
                return False
        return True

    def refresh_rds_enrollment_terms(self, sids, transaction):
        if not self._delete_rds_rows('student_enrollment_terms', sids, transaction):
            return False
        if not self._refresh_rds_enrollment_terms(transaction):
            return False
        return True

    def _delete_rds_rows(self, table, sids, transaction):
        if sids:
            sql = f'DELETE FROM {self.rds_schema}.{table} WHERE sid = ANY(%s)'
            params = (sids,)
        else:
            sql = f'TRUNCATE {self.rds_schema}.{table}'
            params = None
        return transaction.execute(sql, params)

    def _refresh_rds_academic_status(self, transaction):
        # TODO LOAD THE RDS INDEXES FROM REDSHIFT TABLES RATHER THAN IN-MEMORY STORAGE.
        return transaction.insert_bulk(
            f"""INSERT INTO {self.rds_schema}.student_academic_status
                (sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term) VALUES %s""",
            [split_tsv_row(r) for r in self.rows['student_academic_status']],
        )

    def _refresh_rds_names(self, transaction):
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.student_names (
            SELECT DISTINCT sid, unnest(string_to_array(
                regexp_replace(upper(first_name), '[^\w ]', '', 'g'),
                ' '
            )) AS name FROM {self.rds_schema}.student_academic_status
            UNION
            SELECT DISTINCT sid, unnest(string_to_array(
                regexp_replace(upper(last_name), '[^\w ]', '', 'g'),
                ' '
            )) AS name FROM {self.rds_schema}.student_academic_status
            );""",
        )

    def _refresh_rds_majors(self, transaction):
        # TODO LOAD THE RDS INDEXES FROM REDSHIFT TABLES RATHER THAN IN-MEMORY STORAGE.
        return transaction.insert_bulk(
            f'INSERT INTO {self.rds_schema}.student_majors (sid, major) VALUES %s',
            [split_tsv_row(r) for r in self.rows['student_majors']],
        )

    def _refresh_rds_profiles(self, transaction):
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.student_profiles (
            SELECT *
                FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                    SELECT sid, profile
                    FROM {self.redshift_schema}.student_profiles
              $REDSHIFT$)
            AS redshift_profiles (
                sid VARCHAR,
                profile TEXT
            ));""",
        )

    def _refresh_rds_enrollment_terms(self, transaction):
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.student_enrollment_terms (
            SELECT *
                FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                    SELECT sid, term_id, enrollment_term
                    FROM {self.redshift_schema}.student_enrollment_terms
              $REDSHIFT$)
            AS redshift_enrollment_terms (
                sid VARCHAR,
                term_id VARCHAR,
                enrollment_term TEXT
            ));""",
        )
