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

from itertools import groupby
import json
import operator
from time import sleep

from flask import current_app as app
from nessie.externals import rds, redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.jobs.generate_merged_enrollment_term import GenerateMergedEnrollmentTerm
from nessie.lib.berkeley import current_term_id, future_term_id, future_term_ids, legacy_term_ids, reverse_term_ids
from nessie.lib.metadata import get_merged_enrollment_term_job_status, queue_merged_enrollment_term_jobs
from nessie.lib.queries import get_advisee_advisor_mappings, get_advisee_student_profile_elements
from nessie.lib.util import encoded_tsv_row
from nessie.merged.sis_profile import parse_merged_sis_profile
from nessie.merged.sis_profile_v1 import parse_merged_sis_profile_v1
from nessie.merged.student_demographics import add_demographics_rows, refresh_rds_demographics
from nessie.merged.student_terms import upload_student_term_maps
from nessie.models import student_schema

"""Logic for merged student profile and term generation."""


class GenerateMergedStudentFeeds(BackgroundJob):

    rds_schema = app.config['RDS_SCHEMA_STUDENT']
    rds_dblink_to_redshift = app.config['REDSHIFT_DATABASE'] + '_redshift'
    redshift_schema = app.config['REDSHIFT_SCHEMA_STUDENT']

    def run(self, term_id=None):
        app.logger.info(f'Starting merged profile generation job.')

        # This version of the code will always generate feeds for all-terms and all-advisees, but we
        # expect support for term-specific or backfill-specific feed generation will return soon.
        if term_id != 'all':
            app.logger.warn(f'Term-specific generation was requested for {term_id}, but all terms will be generated.')

        app.logger.info('Cleaning up old data...')
        redshift.execute('VACUUM; ANALYZE;')

        status = self.generate_feeds()

        # Clean up the workbench.
        redshift.execute('VACUUM; ANALYZE;')
        app.logger.info(f'Vacuumed and analyzed.')

        return status

    def generate_feeds(self):
        # Translation between canvas_user_id and UID/SID is needed to merge Canvas analytics data and SIS enrollment-based data.
        advisees_by_canvas_id = {}
        advisees_by_sid = {}
        self.successes = []
        self.failures = []
        profile_tables = self.generate_student_profile_tables(advisees_by_canvas_id, advisees_by_sid)
        if not profile_tables:
            raise BackgroundJobError('Failed to generate student profile tables.')

        feed_path = app.config['LOCH_S3_BOAC_ANALYTICS_DATA_PATH'] + '/feeds/'
        s3.upload_json(advisees_by_canvas_id, feed_path + 'advisees_by_canvas_id.json')

        upload_student_term_maps(advisees_by_sid)

        # Avoid processing Canvas analytics data for future terms and pre-CS terms.
        for term_id in (future_term_ids() + legacy_term_ids()):
            enrollment_term_map = s3.get_object_json(feed_path + f'enrollment_term_map_{term_id}.json')
            if enrollment_term_map:
                GenerateMergedEnrollmentTerm().refresh_student_enrollment_term(term_id, enrollment_term_map)

        canvas_integrated_term_ids = reverse_term_ids()
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

        app.logger.info('Exporting analytics data for archival purposes.')
        student_schema.unload_enrollment_terms([current_term_id(), future_term_id()])

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
        # TODO: store in Redis or filesystem to free up memory
        rows = {
            'student_profiles': [],
            'student_academic_status': [],
            'student_majors': [],
            'student_holds': [],
            'demographics': [],
            'ethnicities': [],
            'visas': [],
        }
        tables = [
            'student_profiles', 'student_academic_status', 'student_majors', 'student_holds',
            'demographics', 'ethnicities', 'visas',
        ]
        for table in tables:
            student_schema.truncate_staging_table(table)

        all_student_feed_elements = get_advisee_student_profile_elements()
        all_student_advisor_mappings = self.map_advisors_to_students()
        if not all_student_feed_elements:
            app.logger.error(f'No profile feeds returned, aborting job.')
            return False
        count = len(all_student_feed_elements)
        app.logger.info(f'Will generate feeds for {count} students.')
        for index, feed_elements in enumerate(all_student_feed_elements):
            sid = feed_elements['sid']
            merged_profile = self.generate_student_profile_feed(feed_elements, rows, all_student_advisor_mappings.get(sid, []))
            if merged_profile:
                canvas_user_id = feed_elements['canvas_user_id']
                if canvas_user_id:
                    advisees_by_canvas_id[canvas_user_id] = {'sid': sid, 'uid': feed_elements['ldap_uid']}
                    advisees_by_sid[sid] = {'canvas_user_id': canvas_user_id}
                self.successes.append(sid)
            else:
                self.failures.append(sid)
        for table in tables:
            if rows[table]:
                student_schema.write_to_staging(table, rows[table])
        return tables

    def generate_student_profile_feed(self, feed_elements, rows, advisors):
        sid = feed_elements['sid']
        uid = feed_elements['ldap_uid']
        if not uid:
            return
        if app.config['STUDENT_V1_API_PREFERRED']:
            sis_profile = parse_merged_sis_profile_v1(feed_elements)
        else:
            sis_profile = parse_merged_sis_profile(feed_elements)
        demographics = feed_elements.get('demographics_feed') and json.loads(feed_elements.get('demographics_feed'))
        if demographics:
            demographics = add_demographics_rows(sid, demographics, rows)

        advisor_feed = []
        for a in advisors:
            advisor_feed.append({
                'uid': a['advisor_uid'],
                'firstName': a['advisor_first_name'],
                'lastName': a['advisor_last_name'],
                'email': (a['advisor_campus_email'] or a['advisor_email']),
                'role': a['advisor_role'],
                'program': a['program'],
                'plan': a['plan'],
            })

        merged_profile = {
            'sid': sid,
            'uid': uid,
            'firstName': feed_elements.get('first_name'),
            'lastName': feed_elements.get('last_name'),
            'name': ' '.join([feed_elements.get('first_name'), feed_elements.get('last_name')]),
            'canvasUserId': feed_elements.get('canvas_user_id'),
            'canvasUserName': feed_elements.get('canvas_user_name'),
            'sisProfile': sis_profile,
            'demographics': demographics,
            'advisors': advisor_feed,
        }
        rows['student_profiles'].append(encoded_tsv_row([sid, json.dumps(merged_profile)]))

        if sis_profile:
            first_name = merged_profile['firstName'] or ''
            last_name = merged_profile['lastName'] or ''
            level = str(sis_profile.get('level', {}).get('code') or '')
            gpa = str(sis_profile.get('cumulativeGPA') or '')
            units = str(sis_profile.get('cumulativeUnits') or '')
            transfer = str(sis_profile.get('transfer') or False)
            expected_grad_term = str(sis_profile.get('expectedGraduationTerm', {}).get('id') or '')

            rows['student_academic_status'].append(
                encoded_tsv_row([sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term]),
            )

            for plan in sis_profile.get('plans', []):
                if plan.get('status') == 'Active':
                    rows['student_majors'].append(encoded_tsv_row([sid, plan['description']]))
            for hold in sis_profile.get('holds', []):
                rows['student_holds'].append(encoded_tsv_row([sid, json.dumps(hold)]))

        return merged_profile

    def map_advisors_to_students(self):
        advisors_by_student_id = {}
        for sid, rows in groupby(get_advisee_advisor_mappings(), operator.itemgetter('student_sid')):
            advisors_by_student_id[sid] = list(rows)
        return advisors_by_student_id

    def refresh_rds_indexes(self, sids, transaction):
        if not (
            self._delete_rds_rows('student_academic_status', sids, transaction)
            and self._refresh_rds_academic_status(transaction)
            and self._delete_rds_rows('student_holds', sids, transaction)
            and self._refresh_rds_holds(transaction)
            and self._delete_rds_rows('student_names', sids, transaction)
            and self._refresh_rds_names(transaction)
            and self._delete_rds_rows('student_majors', sids, transaction)
            and self._refresh_rds_majors(transaction)
            and self._delete_rds_rows('student_profiles', sids, transaction)
            and self._refresh_rds_profiles(transaction)
            and self._index_rds_email_address(transaction)
            and self._index_rds_entering_term(transaction)
            and refresh_rds_demographics(self.rds_schema, self.rds_dblink_to_redshift, self.redshift_schema, transaction)
        ):
            return False
        return True

    def refresh_rds_enrollment_terms(self, sids, transaction):
        if not (
            self._delete_rds_rows('student_enrollment_terms', sids, transaction)
            and self._refresh_rds_enrollment_terms(transaction)
            and self._index_rds_midpoint_deficient_grades(transaction)
            and self._index_rds_enrolled_units(transaction)
            and self._index_rds_term_gpa(transaction)
        ):
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
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.student_academic_status (
            SELECT *
            FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                SELECT DISTINCT sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term
                FROM {self.redshift_schema}.student_academic_status
              $REDSHIFT$)
            AS redshift_academic_status (
                sid VARCHAR,
                uid VARCHAR,
                first_name VARCHAR,
                last_name VARCHAR,
                level VARCHAR,
                gpa NUMERIC,
                units NUMERIC,
                transfer BOOLEAN,
                expected_grad_term VARCHAR
            ));""",
        )

    def _index_rds_email_address(self, transaction):
        return transaction.execute(
            f"""UPDATE {self.rds_schema}.student_academic_status sas
            SET email_address = lower(p.profile::json->'sisProfile'->>'emailAddress')
            FROM {self.rds_schema}.student_profiles p
            WHERE sas.sid = p.sid;""",
        )

    def _index_rds_entering_term(self, transaction):
        return transaction.execute(
            # Equivalent to lib.berkeley.sis_term_id_for_name.
            f"""UPDATE {self.rds_schema}.student_academic_status sas
            SET entering_term =
            substr(split_part(p.profile::json->'sisProfile'->>'matriculation', ' ', 2), 1, 1)
            ||
            substr(split_part(p.profile::json->'sisProfile'->>'matriculation', ' ', 2), 3, 2)
            ||
            CASE split_part(p.profile::json->'sisProfile'->>'matriculation', ' ', 1)
            WHEN 'Winter' THEN 0 WHEN 'Spring' THEN 2 WHEN 'Summer' THEN 5 WHEN 'Fall' THEN 8 END
            FROM {self.rds_schema}.student_profiles p
            WHERE p.sid = sas.sid
            AND p.profile::json->'sisProfile'->>'matriculation' IS NOT NULL;""",
        )

    def _refresh_rds_holds(self, transaction):
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.student_holds (
            SELECT *
                FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                    SELECT sid, feed
                    FROM {self.redshift_schema}.student_holds
              $REDSHIFT$)
            AS redshift_holds (
                sid VARCHAR,
                feed TEXT
            ));""",
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
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.student_majors (
            SELECT *
            FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                SELECT DISTINCT sid, major
                FROM {self.redshift_schema}.student_majors
              $REDSHIFT$)
            AS redshift_majors (
                sid VARCHAR,
                major VARCHAR
            ));""",
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

    def _index_rds_midpoint_deficient_grades(self, transaction):
        return transaction.execute(
            f"""UPDATE {self.rds_schema}.student_enrollment_terms t1
            SET midpoint_deficient_grade = TRUE
            FROM {self.rds_schema}.student_enrollment_terms t2, json_array_elements(t2.enrollment_term::json->'enrollments') enr
            WHERE t1.sid = t2.sid
            AND t1.term_id = t2.term_id
            AND enr->>'midtermGrade' IS NOT NULL;""",
        )

    def _index_rds_enrolled_units(self, transaction):
        return transaction.execute(
            f"""UPDATE {self.rds_schema}.student_enrollment_terms
            SET enrolled_units = (enrollment_term::json->>'enrolledUnits')::numeric
            WHERE enrollment_term::json->>'enrolledUnits' IS NOT NULL;""",
        )

    def _index_rds_term_gpa(self, transaction):
        return transaction.execute(
            f"""UPDATE {self.rds_schema}.student_enrollment_terms
            SET term_gpa = (enrollment_term::json->'termGpa'->>'gpa')::numeric
            WHERE (enrollment_term::json->'termGpa'->>'unitsTakenForGpa')::numeric > 0;""",
        )
