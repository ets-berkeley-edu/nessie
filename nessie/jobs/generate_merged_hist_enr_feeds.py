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

from itertools import groupby
import json
import operator
import tempfile

from flask import current_app as app
from nessie.externals import rds, redshift
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib import berkeley, queries
from nessie.lib.util import encoded_tsv_row, resolve_sql_template
from nessie.merged.sis_profile import parse_merged_sis_profile
from nessie.merged.student_terms import append_drops, append_term_gpa, empty_term_feed, merge_enrollment
from nessie.models.student_schema_manager import refresh_from_staging, truncate_staging_table, write_file_to_staging

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
                'student_profiles': feed_file,
                'student_profile_index': index_file,
                'student_names_hist_enr': names_file,
            }
            # Work in batches so as not to overload memory.
            for i in range(0, len(non_advisee_sids), BATCH_QUERY_MAXIMUM):
                sids = non_advisee_sids[i:i + BATCH_QUERY_MAXIMUM]
                profile_count += self.collect_merged_profiles(sids, feed_file, index_file, names_file)
            if profile_count:
                with redshift.transaction() as transaction:
                    for table_name, data in tables.items():
                        truncate_staging_table(table_name)
                        write_file_to_staging(table_name, data, profile_count)
                        refresh_from_staging(
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
                encoded_tsv_row(
                    [sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term, terms_in_attendance, True]) + b'\n',
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
        table_name = 'student_enrollment_terms_hist_enr'
        truncate_staging_table(table_name)
        with tempfile.TemporaryFile() as feed_file:
            row_count = self.generate_term_feeds(non_advisee_sids, feed_file)
            if row_count:
                write_file_to_staging(table_name, feed_file, row_count)
                with redshift.transaction() as transaction:
                    refresh_from_staging(
                        table_name,
                        term_id=None,
                        sids=non_advisee_sids,
                        transaction=transaction,
                    )
        app.logger.info('Non-advisee term enrollment generation complete.')
        return row_count

    def generate_term_feeds(self, sids, feed_file):
        enrollment_stream = queries.stream_sis_enrollments(sids=sids)
        term_gpa_stream = queries.stream_term_gpas(sids=sids)
        term_gpa_tracker = {'term_id': '9999', 'sid': '', 'term_gpas': []}

        row_count = 0

        try:
            term_gpa_results = groupby(term_gpa_stream, lambda r: (str(r['term_id']), r['sid']))

            for term_id, term_enrollments_grp in groupby(enrollment_stream, operator.itemgetter('sis_term_id')):
                term_id = str(term_id)
                term_name = berkeley.term_name_for_sis_id(term_id)
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
                        (term_gpa_tracker['term_id'], term_gpa_tracker['sid']), term_gpa_tracker['term_gpas'] = next(term_gpa_results)
                    if term_gpa_tracker['term_id'] == term_id and term_gpa_tracker['sid'] == sid:
                        append_term_gpa(term_feed, term_gpa_tracker['term_gpas'])

                    feed_file.write(encoded_tsv_row([sid, term_id, json.dumps(term_feed)]) + b'\n')
                    row_count += 1

        finally:
            enrollment_stream.close()
            term_gpa_stream.close()

        return row_count
