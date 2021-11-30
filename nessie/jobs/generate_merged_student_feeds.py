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

from contextlib import ExitStack
from itertools import groupby
import json
import operator
import tempfile

from flask import current_app as app
from nessie.externals import rds, redshift
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib import berkeley, queries
from nessie.lib.util import encoded_tsv_row, resolve_sql_template, write_to_tsv_file
from nessie.merged.sis_profile import parse_merged_sis_profile
from nessie.merged.student_demographics import add_demographics_rows, refresh_rds_demographics
from nessie.merged.student_terms import append_drops, append_term_gpa, empty_term_feed, merge_canvas_site_memberships, merge_enrollment
from nessie.models.student_schema_manager import refresh_all_from_staging, refresh_from_staging, truncate_staging_table, write_file_to_staging

"""Logic for merged student profile and term generation."""


class GenerateMergedStudentFeeds(BackgroundJob):

    rds_schema = app.config['RDS_SCHEMA_STUDENT']
    rds_dblink_to_redshift = app.config['REDSHIFT_DATABASE'] + '_redshift'
    student_schema = queries.student_schema()

    def run(self):
        app.logger.info('Starting merged profile generation job.')

        app.logger.info('Cleaning up old data...')
        redshift.execute('VACUUM; ANALYZE;')

        status = self.generate_feeds()

        # Clean up the workbench.
        redshift.execute('VACUUM; ANALYZE;')
        app.logger.info('Vacuumed and analyzed.')

        return status

    def generate_feeds(self):
        self.successes = []
        self.failures = []

        all_student_profile_elements = queries.get_all_student_profile_elements()
        profile_tables = self.generate_student_profile_tables(all_student_profile_elements)
        if not profile_tables:
            raise BackgroundJobError('Failed to generate student profile tables.')
        refresh_all_from_staging(profile_tables)

        self.update_redshift_academic_standing()
        self.update_rds_profile_indexes()

        result = f'Generated merged profiles ({len(self.successes)} successes, {len(self.failures)} failures).'

        app.logger.info('Profile generation complete; will generate enrollment terms.')
        row_count = self.generate_student_enrollments_table()
        if row_count:
            result += f' Generated merged enrollment terms ({row_count} feeds.)'
        else:
            raise BackgroundJobError('Failed to generate student enrollment tables.')

        self.refresh_rds_enrollment_terms()
        truncate_staging_table('student_enrollment_terms')

        return result

    def generate_student_profile_tables(self, all_student_feed_elements):
        tables = [
            'student_profiles', 'student_profile_index', 'student_majors', 'student_holds',
            'demographics', 'ethnicities', 'intended_majors', 'minors', 'visas',
        ]
        for table in tables:
            truncate_staging_table(table)

        all_student_advisor_mappings = self.map_advisors_to_students()
        if not all_student_feed_elements:
            app.logger.error('No profile feeds returned, aborting job.')
            return False
        count = len(all_student_feed_elements)
        app.logger.info(f'Will generate feeds for {count} students.')
        with ExitStack() as stack:
            feed_files = {table: stack.enter_context(tempfile.TemporaryFile()) for table in tables}
            feed_counts = {table: 0 for table in tables}
            for index, feed_elements in enumerate(all_student_feed_elements):
                sid = feed_elements['sid']
                merged_profile = self.generate_student_profile_feed(
                    feed_elements,
                    all_student_advisor_mappings.get(sid, []),
                    feed_files,
                    feed_counts,
                )
                if merged_profile:
                    self.successes.append(sid)
                else:
                    self.failures.append(sid)
            for table in tables:
                if feed_files[table]:
                    write_file_to_staging(table, feed_files[table], feed_counts[table])
        return tables

    def generate_student_profile_feed(self, feed_elements, advisors, feed_files, feed_counts):
        sid = feed_elements['sid']
        uid = feed_elements['ldap_uid']
        if not uid:
            return
        sis_profile = parse_merged_sis_profile(feed_elements)
        demographics = feed_elements.get('demographics_feed') and json.loads(feed_elements.get('demographics_feed'))
        if demographics:
            demographics = add_demographics_rows(sid, demographics, feed_files, feed_counts)

        advisor_feed = []
        for a in advisors:
            advisor_feed.append({
                'uid': a['advisor_uid'],
                'sid': a['advisor_sid'],
                'firstName': a['advisor_first_name'],
                'lastName': a['advisor_last_name'],
                'email': (a['advisor_campus_email'] or a['advisor_email']),
                'role': a['advisor_role'],
                'title': a['advisor_title'],
                'program': a['program'],
                'plan': a['plan'],
            })

        # For now, whether a student counts as "hist_enr" is determined by whether they show up in the Calnet schema.
        hist_enr = feed_elements.get('ldap_sid') is None

        merged_profile = {
            'sid': sid,
            'uid': uid,
            'canvasUserId': feed_elements.get('canvas_user_id'),
            'canvasUserName': feed_elements.get('canvas_user_name'),
            'sisProfile': sis_profile,
            'demographics': demographics,
            'advisors': advisor_feed,
        }
        self.fill_names(feed_elements, merged_profile, hist_enr)
        feed_counts['student_profiles'] += write_to_tsv_file(feed_files['student_profiles'], [sid, json.dumps(merged_profile)])

        if sis_profile:
            first_name = merged_profile['firstName'] or ''
            last_name = merged_profile['lastName'] or ''
            level = str(sis_profile.get('level', {}).get('code') or '')
            gpa = str(sis_profile.get('cumulativeGPA') or '')
            units = str(sis_profile.get('cumulativeUnits') or '')
            transfer = str(sis_profile.get('transfer') or False)
            expected_grad_term = str(sis_profile.get('expectedGraduationTerm', {}).get('id') or '')
            terms_in_attendance = str(sis_profile.get('termsInAttendance', {}) or '')

            feed_counts['student_profile_index'] += write_to_tsv_file(
                feed_files['student_profile_index'],
                [sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term, terms_in_attendance, hist_enr],
            )

            if hist_enr:
                return merged_profile

            # TODO In due time, populate these index tables for hist_enr students too.
            for plan in sis_profile.get('plans', []):
                if plan.get('status') == 'Active':
                    feed_counts['student_majors'] += write_to_tsv_file(
                        feed_files['student_majors'],
                        [sid, plan.get('program', None), plan.get('description', None)],
                    )
            for hold in sis_profile.get('holds', []):
                feed_counts['student_holds'] += write_to_tsv_file(feed_files['student_holds'], [sid, json.dumps(hold)])
            for intended_major in (sis_profile.get('intendedMajors') or []):
                feed_counts['intended_majors'] += write_to_tsv_file(feed_files['intended_majors'], [sid, intended_major.get('description', None)])
            for plan in sis_profile.get('plansMinor', []):
                if plan.get('status') == 'Active':
                    feed_counts['minors'] += write_to_tsv_file(feed_files['minors'], [sid, plan.get('description', None)])

        return merged_profile

    def fill_names(self, feed_elements, profile, hist_enr):
        if hist_enr:
            profile_feed = json.loads(feed_elements.get('sis_profile_feed', '{}'), strict=False)
            for name_type in ['PRF', 'PRI']:
                name_element = next((ne for ne in profile_feed.get('names', []) if ne['type']['code'] == name_type), None)
                if name_element:
                    break
            if name_element:
                profile['firstName'] = name_element.get('givenName')
                profile['lastName'] = name_element.get('familyName')
                profile['name'] = name_element.get('formattedName')
            else:
                app.logger.debug(f'No name parsed from SIS profile feed: {profile_feed}')
        else:
            profile['firstName'] = feed_elements.get('first_name')
            profile['lastName'] = feed_elements.get('last_name')
            profile['name'] = ' '.join([feed_elements.get('first_name'), feed_elements.get('last_name')])

    def map_advisors_to_students(self):
        advisors_by_student_id = {}
        for sid, rows in groupby(queries.get_advisee_advisor_mappings(), operator.itemgetter('student_sid')):
            advisors_by_student_id[sid] = list(rows)
        return advisors_by_student_id

    def update_redshift_academic_standing(self):
        redshift.execute(
            f"""TRUNCATE {self.student_schema}.academic_standing;
            INSERT INTO {self.student_schema}.academic_standing
                SELECT sid, term_id, acad_standing_action, acad_standing_status, action_date
                FROM {app.config['REDSHIFT_SCHEMA_EDL']}.academic_standing;""",
        )

    def update_rds_profile_indexes(self):
        with rds.transaction() as transaction:
            if self.refresh_rds_indexes(None, transaction):
                transaction.commit()
                app.logger.info('Refreshed RDS indexes.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Failed to refresh RDS indexes.')

        resolved_ddl_rds = resolve_sql_template('update_rds_indexes_student_profiles.template.sql')
        if rds.execute(resolved_ddl_rds):
            app.logger.info('RDS student profile indexes updated.')
        else:
            raise BackgroundJobError('Failed to update RDS student profile indexes.')

    def generate_student_enrollments_table(self):
        row_count = 0

        with tempfile.TemporaryFile() as feed_file:
            row_count = self.generate_term_feeds(feed_file)
            if row_count:
                table_name = 'student_enrollment_terms'
                truncate_staging_table(table_name)
                write_file_to_staging(table_name, feed_file, row_count)
                with redshift.transaction() as transaction:
                    refresh_from_staging(
                        table_name,
                        term_id=None,
                        transaction=transaction,
                    )

        app.logger.info(f'Enrollment term feed generation complete ({row_count} feeds).')
        return row_count

    def generate_term_feeds(self, feed_file):
        enrollment_stream = queries.stream_sis_enrollments()
        term_gpa_stream = queries.stream_term_gpas()
        canvas_site_stream = queries.stream_canvas_memberships()

        term_gpa_tracker = {'term_id': '9999', 'sid': '', 'term_gpas': []}
        canvas_site_tracker = {'term_id': '9999', 'sid': '', 'sites': []}

        row_count = 0

        try:
            term_gpa_results = groupby(term_gpa_stream, lambda r: (str(r['term_id']), r['sid']))
            canvas_site_results = groupby(canvas_site_stream, lambda r: (str(r['term_id']), r['sid']))

            for term_id, term_enrollments_grp in groupby(enrollment_stream, operator.itemgetter('sis_term_id')):
                term_id = str(term_id)
                term_name = berkeley.term_name_for_sis_id(term_id)
                app.logger.info(f'Generating enrollment feeds for term {term_id}...')
                for sid, enrollments_grp in groupby(term_enrollments_grp, operator.itemgetter('sid')):
                    term_feed = None
                    for is_dropped, enrollments_subgroup in groupby(enrollments_grp, operator.itemgetter('dropped')):
                        if not is_dropped:
                            term_feed = merge_enrollment(enrollments_subgroup, term_id, term_name)
                        else:
                            if not term_feed:
                                term_feed = empty_term_feed(term_id, term_name)
                            append_drops(term_feed, enrollments_subgroup)

                    while term_gpa_tracker['term_id'] > term_id or (term_gpa_tracker['term_id'] == term_id and term_gpa_tracker['sid'] < sid):
                        (term_gpa_tracker['term_id'], term_gpa_tracker['sid']), term_gpa_tracker['term_gpas'] =\
                            next(term_gpa_results, ((term_id, sid), []))
                    if term_gpa_tracker['term_id'] == term_id and term_gpa_tracker['sid'] == sid:
                        append_term_gpa(term_feed, term_gpa_tracker['term_gpas'])

                    while canvas_site_tracker['term_id'] > term_id or\
                            (canvas_site_tracker['term_id'] == term_id and canvas_site_tracker['sid'] < sid):
                        (canvas_site_tracker['term_id'], canvas_site_tracker['sid']), canvas_site_tracker['sites'] =\
                            next(canvas_site_results, ((term_id, sid), []))
                    if canvas_site_tracker['term_id'] == term_id and canvas_site_tracker['sid'] == sid:
                        merge_canvas_site_memberships(term_feed, canvas_site_tracker['sites'])

                    feed_file.write(encoded_tsv_row([sid, term_id, json.dumps(term_feed)]) + b'\n')
                    row_count += 1

        finally:
            enrollment_stream.close()
            term_gpa_stream.close()

        return row_count

    def refresh_rds_indexes(self, transaction):
        if not (
            self._delete_rds_rows('student_academic_status', transaction)
            and self._refresh_rds_academic_status(transaction)
            and self._delete_rds_rows('student_holds', transaction)
            and self._refresh_rds_holds(transaction)
            and self._delete_rds_rows('student_names', transaction)
            and self._refresh_rds_names(transaction)
            and self._delete_rds_rows('student_majors', transaction)
            and self._refresh_rds_majors(transaction)
            and self._delete_rds_rows('student_profiles', transaction)
            and self._refresh_rds_profiles(transaction)
            and self._delete_rds_rows('intended_majors', transaction)
            and self._refresh_rds_intended_majors(transaction)
            and self._delete_rds_rows('academic_standing', transaction)
            and self._refresh_rds_academic_standing(transaction)
            and self._delete_rds_rows('minors', transaction)
            and self._refresh_rds_minors(transaction)
            and self._index_rds_email_address(transaction)
            and self._index_rds_entering_term(transaction)
            and refresh_rds_demographics(self.rds_schema, self.rds_dblink_to_redshift, self.student_schema, transaction)
        ):
            return False
        return True

    def refresh_rds_enrollment_terms(self):
        app.logger.info('Refreshing enrollment terms in RDS.')
        with rds.transaction() as transaction:
            result = (
                self._delete_rds_rows('student_enrollment_terms', None, transaction)
                and self._delete_rds_rows('student_enrollment_terms_hist_enr', None, transaction)
                and self._refresh_rds_enrollment_terms(transaction)
                and self._index_rds_midpoint_deficient_grades(transaction)
                and self._index_rds_enrolled_units(transaction)
                and self._index_rds_term_gpa(transaction)
                and self._index_rds_epn_grading_option(transaction)
            )
            if result:
                transaction.commit()
                app.logger.info('Refreshed RDS enrollment terms.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Failed to refresh RDS enrollment terms.')

    def _delete_rds_rows(self, table, transaction):
        return transaction.execute(f'TRUNCATE {self.rds_schema}.{table}')

    def _refresh_rds_academic_standing(self, transaction):
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.academic_standing (
            SELECT *
            FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                SELECT DISTINCT sid, term_id, acad_standing_action, acad_standing_status, LEFT(action_date, 10)
                FROM {self.student_schema}.academic_standing
              $REDSHIFT$)
            AS redshift_academic_standing (
                sid VARCHAR,
                term_id VARCHAR,
                acad_standing_action VARCHAR,
                acad_standing_status VARCHAR,
                action_date VARCHAR
            ));""",
        )

    def _refresh_rds_academic_status(self, transaction):
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.student_academic_status (
            SELECT *
            FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                SELECT DISTINCT sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term, terms_in_attendance
                FROM {self.student_schema}.student_profile_index
                WHERE hist_enr IS FALSE
              $REDSHIFT$)
            AS redshift_profile_index (
                sid VARCHAR,
                uid VARCHAR,
                first_name VARCHAR,
                last_name VARCHAR,
                level VARCHAR,
                gpa NUMERIC,
                units NUMERIC,
                transfer BOOLEAN,
                expected_grad_term VARCHAR,
                terms_in_attendance INT
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
                    FROM {self.student_schema}.student_holds
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
                SELECT DISTINCT sid, college, major
                FROM {self.student_schema}.student_majors
              $REDSHIFT$)
            AS redshift_majors (
                sid VARCHAR,
                college VARCHAR,
                major VARCHAR
            ));""",
        )

    def _refresh_rds_profiles(self, transaction):
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.student_profiles (
            SELECT *
                FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                    SELECT sp.sid, sp.profile
                    FROM {self.student_schema}.student_profiles sp
                    JOIN {self.student_schema}.student_profile_index spi
                    ON sp.sid = spi.sid
                    AND spi.hist_enr IS FALSE
              $REDSHIFT$)
            AS redshift_profiles (
                sid VARCHAR,
                profile TEXT
            ));""",
        )

    def _refresh_rds_intended_majors(self, transaction):
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.intended_majors (
            SELECT DISTINCT *
                FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                    SELECT sid, major
                    FROM {self.student_schema}.intended_majors
              $REDSHIFT$)
            AS redshift_intended_majors (
                sid VARCHAR,
                major VARCHAR
            ));""",
        )

    def _refresh_rds_minors(self, transaction):
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.minors (
            SELECT DISTINCT *
                FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                    SELECT sid, minor
                    FROM {self.student_schema}.minors
              $REDSHIFT$)
            AS redshift_minors (
                sid VARCHAR,
                minor VARCHAR
            ));""",
        )

    def _refresh_rds_enrollment_terms(self, transaction):
        advisee_refresh = f"""INSERT INTO {self.rds_schema}.student_enrollment_terms (
            SELECT *
                FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                    SELECT set.sid, set.term_id, set.enrollment_term
                    FROM {self.student_schema}.student_enrollment_terms set
                    JOIN {self.student_schema}.student_profile_index spi
                    ON set.sid = spi.sid
                    AND spi.hist_enr IS FALSE
              $REDSHIFT$)
            AS redshift_enrollment_terms (
                sid VARCHAR,
                term_id VARCHAR,
                enrollment_term TEXT
            ));"""
        non_advisee_refresh = f"""INSERT INTO {self.rds_schema}.student_enrollment_terms_hist_enr (
            SELECT *
            FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                SELECT set.sid, set.term_id, set.enrollment_term
                FROM {self.student_schema}.student_enrollment_terms set
                JOIN {self.student_schema}.student_profile_index spi
                ON set.sid = spi.sid
                AND spi.hist_enr IS TRUE
            $REDSHIFT$)
            AS redshift_profiles (
                sid VARCHAR,
                term_id VARCHAR,
                enrollment_term TEXT
            ));
        """
        return transaction.execute(advisee_refresh) and transaction.execute(non_advisee_refresh)

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

    def _index_rds_epn_grading_option(self, transaction):
        return transaction.execute(
            f"""UPDATE {self.rds_schema}.student_enrollment_terms t1
            SET epn_grading_option = TRUE
            FROM {self.rds_schema}.student_enrollment_terms t2, json_array_elements(t2.enrollment_term::json->'enrollments') enr
            WHERE t1.sid = t2.sid
            AND t1.term_id = t2.term_id
            AND enr->>'gradingBasis' = 'EPN';""",
        )
