"""
Copyright ©2022. The Regents of the University of California (Regents). All Rights Reserved.

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
from nessie.merged.student_demographics import add_demographics_rows
from nessie.merged.student_terms import append_drops, append_term_gpa, empty_term_feed, merge_canvas_site_memberships, merge_enrollment
from nessie.models.student_schema_manager import refresh_all_from_staging, refresh_from_staging, truncate_staging_table, write_file_to_staging

"""Logic for merged student profile and term generation."""


class GenerateMergedStudentFeeds(BackgroundJob):

    rds_schema = app.config['RDS_SCHEMA_STUDENT']
    rds_dblink_to_redshift = app.config['REDSHIFT_DATABASE'] + '_redshift'
    student_schema = queries.student_schema()
    redshift_edl_schema = queries.edl_external_schema()

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
            major_divisions = self.get_majors_divisions()

            for index, feed_elements in enumerate(all_student_feed_elements):
                sid = feed_elements['sid']
                if self.generate_student_profile_feed(
                    feed_elements,
                    all_student_advisor_mappings.get(sid, []),
                    feed_files,
                    feed_counts,
                    major_divisions,
                ):
                    self.successes.append(sid)
                else:
                    self.failures.append(sid)
            for table in tables:
                if feed_files[table]:
                    write_file_to_staging(table, feed_files[table], feed_counts[table])
        return tables

    def generate_student_profile_feed(self, feed_elements, advisors, feed_files, feed_counts, major_divisions):
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

        names = self.get_names(feed_elements)

        base_profile = {
            'sid': sid,
            'uid': uid,
            'advisors': advisor_feed,
            'canvasUserId': feed_elements.get('canvas_user_id'),
            'canvasUserName': feed_elements.get('canvas_user_name'),
            **names,
        }
        merged_profile = {
            **base_profile,
            'demographics': demographics,
            'sisProfile': sis_profile,
        }
        profile_summary = {
            **base_profile,
            **self.summarize_sis_profile(sis_profile),
        }
        feed_counts['student_profiles'] += write_to_tsv_file(
            feed_files['student_profiles'],
            [sid, json.dumps(merged_profile), json.dumps(profile_summary)],
        )

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
                [sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term, terms_in_attendance],
            )

            for plan in sis_profile.get('plans', []):
                if plan.get('status') == 'Active':
                    plan_description = plan.get('description', None)

                    feed_counts['student_majors'] += write_to_tsv_file(
                        feed_files['student_majors'],
                        [sid, plan.get('program', None), plan_description, major_divisions.get(plan_description, None)],
                    )
            for hold in sis_profile.get('holds', []):
                feed_counts['student_holds'] += write_to_tsv_file(feed_files['student_holds'], [sid, json.dumps(hold)])
            for intended_major in (sis_profile.get('intendedMajors') or []):
                feed_counts['intended_majors'] += write_to_tsv_file(feed_files['intended_majors'], [sid, intended_major.get('description', None)])
            for plan in sis_profile.get('plansMinor', []):
                if plan.get('status') == 'Active':
                    feed_counts['minors'] += write_to_tsv_file(feed_files['minors'], [sid, plan.get('description', None)])

        return True

    def summarize_sis_profile(self, sis_profile):
        if not sis_profile:
            return {}
        profile_summary = {
            'academicCareerStatus': sis_profile.get('academicCareerStatus'),
            'academicStanding': sis_profile.get('academicStanding'),
            'cumulativeGPA': sis_profile.get('cumulativeGPA'),
            'cumulativeUnits': sis_profile.get('cumulativeUnits'),
            'currentTerm': sis_profile.get('currentTerm'),
            'degrees': sis_profile.get('degrees'),
            'expectedGraduationTerm': sis_profile.get('expectedGraduationTerm'),
            'level': self.get_sis_level_description(sis_profile),
            'majors': self.get_active_plan_descriptions(sis_profile),
            'matriculation': sis_profile.get('matriculation'),
            'termGpa': sis_profile.get('termGpa'),
            'termsInAttendance': sis_profile.get('termsInAttendance'),
            'transfer': sis_profile.get('transfer'),
        }
        if sis_profile.get('withdrawalCancel'):
            profile_summary['withdrawalCancel'] = sis_profile['withdrawalCancel']
            if not sis_profile['withdrawalCancel'].get('termId'):
                sis_profile['withdrawalCancel']['termId'] = berkeley.current_term_id()
        return profile_summary

    def get_active_plan_descriptions(self, sis_profile):
        return sorted(plan.get('description') for plan in sis_profile.get('plans', []) if plan.get('status') == 'Active')

    def get_names(self, feed_elements):
        names = {}
        if feed_elements.get('first_name') and feed_elements.get('last_name'):
            names['firstName'] = feed_elements['first_name']
            names['lastName'] = feed_elements['last_name']
            names['name'] = ' '.join([feed_elements['first_name'], feed_elements['last_name']])
        elif feed_elements.get('sis_profile_feed'):
            profile_feed = json.loads(feed_elements['sis_profile_feed'], strict=False)
            for name_type in ['PRF', 'PRI']:
                name_element = next((ne for ne in profile_feed.get('names', []) if ne['type']['code'] == name_type), None)
                if name_element:
                    break
            if name_element:
                names['firstName'] = name_element.get('givenName')
                names['lastName'] = name_element.get('familyName')
                names['name'] = name_element.get('formattedName')
            else:
                app.logger.debug(f'No name parsed from SIS profile feed: {profile_feed}')
        return names

    def get_sis_level_description(self, sis_profile):
        level = sis_profile.get('level', {}).get('description')
        if level == 'Not Set':
            return None
        else:
            return level

    def get_majors_divisions(self):
        rows = redshift.fetch(
            f"""SELECT academic_plan_nm, academic_division_shrt_nm
                FROM {self.redshift_edl_schema}.student_academic_plan_hierarchy_data;""",
        )

        return {r['academic_plan_nm']: r['academic_division_shrt_nm'] for r in rows}

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
        resolved_ddl_rds = resolve_sql_template('update_rds_indexes_student_profiles.template.sql')
        if rds.execute(resolved_ddl_rds):
            app.logger.info('RDS student profile indexes updated.')
        else:
            raise BackgroundJobError('Failed to update RDS student profile indexes.')

    def generate_student_enrollments_table(self):
        table_name = 'student_enrollment_terms'
        truncate_staging_table(table_name)
        row_count = self.generate_term_feeds(table_name)
        if row_count:
            with redshift.transaction() as transaction:
                refresh_from_staging(
                    table_name,
                    term_id=None,
                    transaction=transaction,
                )
        app.logger.info(f'Enrollment term feed generation complete ({row_count} feeds).')
        return row_count

    def generate_term_feeds(self, table_name):
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
                term_row_count = 0

                with tempfile.TemporaryFile() as feed_file:
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
                        term_row_count += 1

                    if term_row_count:
                        write_file_to_staging(table_name, feed_file, term_row_count, term_id=term_id)
                        row_count += term_row_count

        finally:
            enrollment_stream.close()
            term_gpa_stream.close()

        return row_count

    def refresh_rds_enrollment_terms(self):
        resolved_ddl_rds = resolve_sql_template('update_rds_indexes_student_enrollment_terms.template.sql')
        if rds.execute(resolved_ddl_rds):
            app.logger.info('RDS student enrollment term indexes updated.')
        else:
            raise BackgroundJobError('Failed to update RDS student enrollment term indexes.')
