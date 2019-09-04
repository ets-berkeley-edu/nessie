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
import tempfile

from flask import current_app as app
from nessie.externals import redshift
from nessie.jobs.background_job import BackgroundJob
from nessie.lib import queries
from nessie.lib.berkeley import reverse_term_ids
from nessie.lib.util import encoded_tsv_row
from nessie.merged.sis_profile import parse_merged_sis_profile
from nessie.merged.student_terms import map_sis_enrollments
from nessie.models import student_schema

"""Logic to generate client-friendly merge of available data on non-current students."""

BATCH_QUERY_MAXIMUM = 5000


class GenerateMergedHistEnrFeeds(BackgroundJob):

    redshift_schema = app.config['REDSHIFT_SCHEMA_STUDENT']

    def run(self):
        app.logger.info(f'Starting merged non-advisee profile generation job.')

        app.logger.info('Cleaning up old data...')
        redshift.execute('VACUUM; ANALYZE;')

        status = self.generate_feeds()

        # Clean up the workbench.
        redshift.execute('VACUUM; ANALYZE;')
        app.logger.info(f'Vacuumed and analyzed.')

        return status

    def generate_feeds(self):

        # Process all unprocessed SIDS which have SIS Students API data.
        unmerged_sids = queries.get_non_advisee_unmerged_student_ids()
        unmerged_sids = [r['sid'] for r in unmerged_sids]

        profile_count = self.generate_student_profile_table(unmerged_sids)
        self.generate_student_enrollments_table(unmerged_sids)

        return f'Generated {profile_count} non-advisee profiles.'

    def generate_student_profile_table(self, unmerged_sids):
        profile_count = 0
        table_name = 'student_profiles_hist_enr'
        with tempfile.TemporaryFile() as feed_file:
            # Work in batches so as not to overload memory.
            for i in range(0, len(unmerged_sids), BATCH_QUERY_MAXIMUM):
                sids = unmerged_sids[i:i + BATCH_QUERY_MAXIMUM]
                profile_count += self.collect_merged_profiles(sids, feed_file)
            if profile_count:
                student_schema.truncate_staging_table(table_name)
                student_schema.write_file_to_staging(table_name, feed_file, profile_count)
        student_schema.refresh_all_from_staging([table_name])
        app.logger.info('Non-advisee profile generation complete.')

    def collect_merged_profiles(self, sids, feed_file):
        successes = []
        sis_profile_feeds = queries.get_non_advisee_api_feeds(sids)
        for row in sis_profile_feeds:
            sid = row['sid']
            feed = row['feed']
            parsed_profile = parse_merged_sis_profile(feed, None, None)
            feed_file.write(encoded_tsv_row([sid, row['uid'], json.dumps(parsed_profile)]) + b'\n')
            successes.append(sid)
        return len(successes)

    def generate_student_enrollments_table(self, unmerged_sids):
        # Split all S3/Redshift operations by term in hope of not overloading memory or other resources.
        # (Using finer-grained batches of SIDs would probably involve replacing the staging table by a Spectrum
        # external table.)
        table_name = 'student_enrollment_terms_hist_enr'
        student_schema.truncate_staging_table(table_name)
        for term_id in reverse_term_ids(include_future_terms=True, include_legacy_terms=True):
            with tempfile.TemporaryFile() as feed_file:
                term_count = self.collect_merged_enrollments(unmerged_sids, term_id, feed_file)
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
                        None,
                        transaction,
                    )
        app.logger.info('Non-advisee term enrollment generation complete.')

    def collect_merged_enrollments(self, sids, term_id, feed_file):
        sis_enrollments = queries.get_non_advisee_sis_enrollments(sids, term_id)
        enrollments_by_student = map_sis_enrollments(sis_enrollments)
        enrollments_by_student = enrollments_by_student.get(term_id, {})
        for (sid, enrollments_feed) in enrollments_by_student.items():
            feed_file.write(encoded_tsv_row([sid, term_id, json.dumps(enrollments_feed)]) + b'\n')
        return len(enrollments_by_student.keys())
