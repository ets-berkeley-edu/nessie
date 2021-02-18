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
from nessie.externals import redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib import queries
from nessie.lib.analytics import merge_analytics_for_course, merge_assignment_submissions_for_user
from nessie.lib.berkeley import reverse_term_ids
from nessie.lib.util import encoded_tsv_row
from nessie.models.student_schema_manager import drop_staged_enrollment_term, refresh_from_staging, \
    write_file_to_staging


class GenerateMergedEnrollmentTerm(BackgroundJob):

    def run(self, term_id):
        merged_enrollment_term = self.merge_analytics_data_for_term(term_id)
        self.refresh_student_enrollment_term(term_id, merged_enrollment_term)
        return f'Generated merged feeds for term {term_id} ({self.course_count} courses, {self.user_count} users).'

    def refresh_student_enrollment_term(self, term_id, enrollment_term_map):
        with tempfile.TemporaryFile() as enrollment_term_file:
            for (sid, sid_term_feed) in enrollment_term_map.items():
                enrollment_term_file.write(encoded_tsv_row([sid, term_id, json.dumps(sid_term_feed)]) + b'\n')

            drop_staged_enrollment_term(term_id)
            write_file_to_staging('student_enrollment_terms', enrollment_term_file, len(enrollment_term_map), term_id)

        with redshift.transaction() as transaction:
            refresh_from_staging('student_enrollment_terms', term_id, None, transaction, truncate_staging=False)
            if not transaction.commit():
                raise BackgroundJobError(f'Final transaction commit failed on enrollment term refresh (term_id={term_id}).')

    def merge_analytics_data_for_term(self, term_id):
        feed_path = app.config['LOCH_S3_BOAC_ANALYTICS_DATA_PATH'] + '/feeds/'

        advisees_by_canvas_id_path = feed_path + 'advisees_by_canvas_id.json'
        advisees_by_canvas_id = s3.get_object_json(advisees_by_canvas_id_path)
        if not advisees_by_canvas_id:
            raise BackgroundJobError(f'Failed to retrieve advisee map at {advisees_by_canvas_id_path}, aborting')

        enrollment_term_map_path = feed_path + f'enrollment_term_map_{term_id}.json'
        enrollment_term_map = s3.get_object_json(enrollment_term_map_path)
        if not enrollment_term_map:
            raise BackgroundJobError(f'Failed to retrieve enrollment term map at {enrollment_term_map_path}, aborting')

        canvas_site_map = self.merge_canvas_analytics_for_term(term_id, feed_path)

        self.merge_course_analytics_for_term(term_id, canvas_site_map, enrollment_term_map, advisees_by_canvas_id)
        self.merge_advisee_assignment_submissions_for_term(term_id, enrollment_term_map, advisees_by_canvas_id)
        return enrollment_term_map

    def merge_canvas_analytics_for_term(self, term_id, feed_path):
        if term_id not in reverse_term_ids():
            return {}
        canvas_site_map_path = feed_path + f'canvas_site_map_{term_id}.json'
        canvas_site_map = s3.get_object_json(canvas_site_map_path)
        if not canvas_site_map:
            raise BackgroundJobError(f'Failed to retrieve Canvas site map at {canvas_site_map_path}, aborting')
        return canvas_site_map

    def merge_course_analytics_for_term(self, term_id, canvas_site_map, enrollment_term_map, advisees_by_canvas_id):
        app.logger.info(f'Starting non-assignment-submissions analytics merge for {len(canvas_site_map)} Canvas courses')
        course_count = 0
        for (canvas_course_id, canvas_map_entry) in canvas_site_map.items():
            course_count += 1
            if course_count % 100 == 0:
                app.logger.debug(f'Merging Canvas course {course_count} of {len(canvas_site_map)}')
            merge_analytics_for_course(term_id, canvas_map_entry, enrollment_term_map, advisees_by_canvas_id)
        app.logger.info(f'Course analytics merge complete: {course_count} courses merged.')
        self.course_count = course_count

    def merge_advisee_assignment_submissions_for_term(self, term_id, enrollment_term_map, advisees_by_canvas_id):
        advisee_ids = advisees_by_canvas_id.keys()
        app.logger.info(f'Starting assignment-submissions analytics merge for term {term_id} (up to {len(advisee_ids)} advisees)')
        user_count = 0

        # Track analytics already merged in the event a retry is needed on the submission stats query.
        merged_analytics = {}
        ids_without_merged_analytics = set(advisee_ids)

        submission_counts_for_term_query = queries.get_advisee_submissions_sorted(term_id)
        for canvas_user_id, sites_grp in groupby(submission_counts_for_term_query, key=operator.itemgetter('reference_user_id')):
            user_count += 1
            if user_count % 100 == 0:
                app.logger.debug(f'Merging Canvas user {user_count} of up to {len(advisee_ids)}')
            if merged_analytics.get(canvas_user_id):
                # We must have already handled calculations for this user on a download that subsequently errored out.
                continue
            sid = advisees_by_canvas_id.get(str(canvas_user_id), {}).get('sid')
            if not sid:
                app.logger.info(f'Advisee submissions query returned canvas_user_id {canvas_user_id}, but no match in advisees map')
                merged_analytics[canvas_user_id] = 'skipped'
                continue
            advisee_term_feed = enrollment_term_map.get(sid)
            if not advisee_term_feed:
                # Nothing to merge.
                merged_analytics[canvas_user_id] = 'skipped'
                continue

            relative_submission_counts = {}
            for canvas_course_id, subs_grp in groupby(sites_grp, key=operator.itemgetter('canvas_course_id')):
                relative_submission_counts[canvas_course_id] = list(subs_grp)
            merge_assignment_submissions_for_user(
                advisee_term_feed,
                canvas_user_id,
                relative_submission_counts,
            )
            merged_analytics[canvas_user_id] = 'merged'
            ids_without_merged_analytics.remove(str(canvas_user_id))

        # Add in empty data structures for the few students who have never seen a Canvas assignment.
        for canvas_user_id in ids_without_merged_analytics:
            sid = advisees_by_canvas_id.get(str(canvas_user_id), {}).get('sid')
            advisee_term_feed = enrollment_term_map.get(sid)
            if not advisee_term_feed:
                # Nothing to merge.
                continue
            merge_assignment_submissions_for_user(advisee_term_feed, canvas_user_id, {})

        app.logger.info(f'Assignment submissions merge for term {term_id} complete: {user_count} users merged.')
        self.user_count = user_count
