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
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
import json
from timeit import default_timer as timer

from flask import current_app as app
from nessie.externals import sis_student_api
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.berkeley import edl_demographics_to_json, edl_registration_to_json
from nessie.lib.queries import get_edl_student_registrations
from nessie.lib.util import encoded_tsv_row
import numpy as np


class AbstractRegistrationsJob(BackgroundJob):

    include_demographics = True

    @abstractmethod
    def run(self, load_mode='new'):
        pass

    def get_registration_data_per_sids(self, rows, sids, include_demographics=True):
        self.include_demographics = include_demographics
        return self._query_edl(rows, sids) if app.config['FEATURE_FLAG_EDL_REGISTRATIONS'] else self._query_student_api(rows, sids)

    def _query_edl(self, rows, sids):
        successes = []
        for edl_row in get_edl_student_registrations(sids):
            sid = edl_row['student_id']
            if sid not in successes:
                # Based on the SQL order_by, the first result per SID will be 'last_registration'.
                successes.append(sid)
                rows['last_registrations'].append(
                    encoded_tsv_row([sid, json.dumps(edl_registration_to_json(edl_row))]),
                )
            rows['term_gpas'].append(
                encoded_tsv_row(
                    [
                        sid,
                        edl_row['term_id'],
                        edl_row['current_term_gpa'] or '0',
                        edl_row.get('unt_taken_gpa') or '0',  # TODO: Does EDL give us 'unitsTakenForGpa'?
                    ],
                ),
            )
            if self.include_demographics:
                rows['demographics'].append(
                    encoded_tsv_row([sid, json.dumps(edl_demographics_to_json(edl_row))]),
                )
        failures = list(np.setdiff1d(sids, successes))
        return successes, failures

    def _query_student_api(self, rows, sids):
        successes = []
        failures = []
        app_obj = app._get_current_object()
        start_loop = timer()

        with ThreadPoolExecutor(max_workers=app.config['STUDENT_API_MAX_THREADS']) as executor:
            for result in executor.map(self._async_get_feed, repeat(app_obj), sids):
                sid = result['sid']
                full_feed = result['feed']
                if full_feed:
                    successes.append(sid)
                    rows['last_registrations'].append(
                        encoded_tsv_row([sid, json.dumps(full_feed.get('last_registration', {}))]),
                    )
                    gpa_feed = full_feed.get('term_gpas', {})
                    if gpa_feed:
                        for term_id, term_data in gpa_feed.items():
                            row = [
                                sid,
                                term_id,
                                (term_data.get('gpa') or '0'),
                                (term_data.get('unitsTakenForGpa') or '0'),
                            ]
                            rows['term_gpas'].append(encoded_tsv_row(row))
                    else:
                        app.logger.info(f'No past UGRD registrations found for SID {sid}.')
                    demographics = full_feed.get('demographics', {})
                    if demographics:
                        rows['api_demographics'].append(
                            encoded_tsv_row([sid, json.dumps(demographics)]),
                        )
                else:
                    failures.append(sid)
                    app.logger.error(f'Registration history import failed for SID {sid}.')
        app.logger.info(f'Wanted {len(sids)} students; got {len(successes)} in {timer() - start_loop} secs')
        return successes, failures

    def _async_get_feed(self, app_obj, sid):
        with app_obj.app_context():
            app.logger.info(f'Fetching registration history for SID {sid}')
            feed = sis_student_api.get_term_gpas_registration_demog(sid, self.include_demographics)
            result = {
                'sid': sid,
                'feed': feed,
            }
        return result
