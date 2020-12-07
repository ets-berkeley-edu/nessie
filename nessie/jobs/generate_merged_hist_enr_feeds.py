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

import json
import tempfile

from flask import current_app as app
from nessie.externals import rds, redshift
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib import queries
from nessie.lib.berkeley import reverse_term_ids
from nessie.lib.util import encoded_tsv_row, resolve_sql_template
from nessie.merged.sis_profile import parse_merged_sis_profile
from nessie.merged.student_terms import map_sis_enrollments, merge_dropped_classes, merge_term_gpas
from nessie.models import student_schema

"""Logic to generate client-friendly merge of available data on non-current students."""

BATCH_QUERY_MAXIMUM = 5000


class GenerateMergedHistEnrFeeds(BackgroundJob):

    redshift_schema = app.config['REDSHIFT_SCHEMA_STUDENT']

    def run(self):
        app.logger.info('Starting merged non-advisee profile generation job.')

        app.logger.info('Cleaning up old data...')
        redshift.execute('VACUUM; ANALYZE;')

        status = self.generate_feeds()

        # Clean up the workbench.
        redshift.execute('VACUUM; ANALYZE;')
        app.logger.info('Vacuumed and analyzed.')

        return status

    def generate_feeds(self):
        non_advisee_sids = queries.get_fetched_non_advisees()
        non_advisee_sids = [r['sid'] for r in non_advisee_sids]

        profile_count = self.generate_student_profile_table(non_advisee_sids)
        enrollment_count = self.generate_student_enrollments_table(non_advisee_sids)

        if profile_count and enrollment_count:
            resolved_ddl_rds = resolve_sql_template('update_rds_indexes_student_profiles_hist_enr.template.sql')
            if rds.execute(resolved_ddl_rds):
                app.logger.info('RDS indexes updated.')
            else:
                raise BackgroundJobError('Failed to refresh RDS copies of non-advisee data.')
        else:
            app.logger.warning('No non-advisee data loaded into Redshift; will not refresh RDS copies.')

        return f'Generated {profile_count} non-advisee profiles, {enrollment_count} enrollments.'

    def generate_student_profile_table(self, non_advisee_sids):
        profile_count = 0
        with tempfile.TemporaryFile() as feed_file, tempfile.TemporaryFile() as index_file, tempfile.TemporaryFile() as names_file:
            tables = {
                'student_profiles_hist_enr': feed_file,
                'student_profile_index_hist_enr': index_file,
                'student_names_hist_enr': names_file,
            }
            # Work in batches so as not to overload memory.
            for i in range(0, len(non_advisee_sids), BATCH_QUERY_MAXIMUM):
                sids = non_advisee_sids[i:i + BATCH_QUERY_MAXIMUM]
                profile_count += self.collect_merged_profiles(sids, feed_file, index_file, names_file)
            if profile_count:
                with redshift.transaction() as transaction:
                    for table_name, data in tables.items():
                        student_schema.truncate_staging_table(table_name)
                        student_schema.write_file_to_staging(table_name, data, profile_count)
                        student_schema.refresh_from_staging(
                            table_name,
                            None,
                            non_advisee_sids,
                            transaction,
                        )
        app.logger.info('Non-advisee profile generation complete.')
        return profile_count

    def collect_merged_profiles(self, sids, feed_file, index_file, names_file):
        successes = []
        sis_profile_feeds = queries.get_non_advisee_api_feeds(sids)
        for row in sis_profile_feeds:
            sid = row['sid']
            uid = row['uid']
            sis_api_feed = row['sis_feed']
            sis_profile = parse_merged_sis_profile({
                'sis_profile_feed': sis_api_feed,
                'last_registration_feed': row['last_registration_feed'],
            })
            merged_profile = {
                'sid': sid,
                'uid': uid,
                'sisProfile': sis_profile,
            }
            self.fill_names_from_sis_profile(sis_api_feed, merged_profile)
            feed_file.write(encoded_tsv_row([sid, uid, json.dumps(merged_profile)]) + b'\n')

            first_name = merged_profile.get('firstName', '')
            last_name = merged_profile.get('lastName', '')
            level = str(sis_profile.get('level', {}).get('code') or '')
            gpa = str(sis_profile.get('cumulativeGPA') or '')
            units = str(sis_profile.get('cumulativeUnits') or '')
            transfer = str(sis_profile.get('transfer') or False)
            expected_grad_term = str(sis_profile.get('expectedGraduationTerm', {}).get('id') or '')
            terms_in_attendance = str(sis_profile.get('termsInAttendance', {}) or '')
            index_file.write(
                encoded_tsv_row([sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term, terms_in_attendance]) + b'\n',
            )

            names_file.write(
                encoded_tsv_row([
                    sid,
                    merged_profile.get('uid'),
                    merged_profile.get('firstName'),
                    merged_profile.get('lastName'),
                ]) + b'\n',
            )
            successes.append(sid)
        return len(successes)

    def fill_names_from_sis_profile(self, api_json, profile):
        api_feed = json.loads(api_json, strict=False)
        for name_type in ['PRF', 'PRI']:
            name_element = next((ne for ne in api_feed.get('names', []) if ne['type']['code'] == name_type), None)
            if name_element:
                break
        if name_element:
            profile['firstName'] = name_element.get('givenName')
            profile['lastName'] = name_element.get('familyName')
            profile['name'] = name_element.get('formattedName')
        else:
            app.logger.debug(f'No name parsed in {api_json}')

    def generate_student_enrollments_table(self, non_advisee_sids):
        # Split all S3/Redshift operations by term in hope of not overloading memory or other resources.
        # (Using finer-grained batches of SIDs would probably involve replacing the staging table by a Spectrum
        # external table.)
        total_count = 0
        table_name = 'student_enrollment_terms_hist_enr'
        student_schema.truncate_staging_table(table_name)
        for term_id in reverse_term_ids(include_future_terms=True, include_legacy_terms=True):
            with tempfile.TemporaryFile() as feed_file:
                term_count = self.collect_merged_enrollments(non_advisee_sids, term_id, feed_file)
                if term_count:
                    student_schema.write_file_to_staging(
                        table_name,
                        feed_file,
                        term_count,
                        term_id,
                    )
            if term_count:
                with redshift.transaction() as transaction:
                    student_schema.refresh_from_staging(
                        table_name,
                        term_id,
                        non_advisee_sids,
                        transaction,
                    )
                total_count += term_count
        app.logger.info('Non-advisee term enrollment generation complete.')
        return total_count

    def collect_merged_enrollments(self, sids, term_id, feed_file):
        rows = queries.get_non_advisee_sis_enrollments(sids, term_id)
        enrollments_by_student = map_sis_enrollments(rows)
        merge_dropped_classes(enrollments_by_student, queries.get_non_advisee_enrollment_drops(sids, term_id))
        merge_term_gpas(enrollments_by_student, queries.get_non_advisee_term_gpas(sids, term_id))
        enrollments_by_student = enrollments_by_student.get(term_id, {})
        for (sid, enrollments_feed) in enrollments_by_student.items():
            feed_file.write(encoded_tsv_row([sid, term_id, json.dumps(enrollments_feed)]) + b'\n')
        return len(enrollments_by_student.keys())
