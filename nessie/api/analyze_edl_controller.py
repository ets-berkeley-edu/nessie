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

from contextlib import contextmanager

from flask import current_app as app
from nessie.api.auth_helper import auth_required
from nessie.api.errors import BadRequestError
from nessie.jobs.abstract.abstract_registrations_job import AbstractRegistrationsJob
from nessie.jobs.generate_merged_hist_enr_feeds import GenerateMergedHistEnrFeeds
from nessie.lib.berkeley import feature_flag_edl
from nessie.lib.http import tolerant_jsonify

#
# TODO: Toss out this controller when EDL cutover is done.
#


@app.route('/api/analyze_edl/import_registrations/<sid>')
@auth_required
def analyze_edl_registration_data(sid):
    # TODO: All 'analyze_edl' API endpoints must start with safety_check().
    _safety_check()
    result = {}

    class MockRegistrationsJob(AbstractRegistrationsJob):
        def run(self, load_mode='new'):
            pass

    job = MockRegistrationsJob()
    demographics_key = 'demographics' if feature_flag_edl() else 'api_demographics'

    for key in ('edl', 'sis'):
        with _override_edl_feature_flag(key == 'edl'):
            result[key] = {
                'term_gpas': [],
                'last_registrations': [],
                demographics_key: [],
            }
            job.get_registration_data_per_sids(result[key], [sid])

    return tolerant_jsonify(result)


@app.route('/api/analyze_edl/term_gpa/<term_id>/<sid>')
@auth_required
def analyze_term_gpa(term_id, sid):
    _safety_check()
    result = {}

    class MockFeedFile:
        def write(self, tsv):
            result[key] = tsv
    for key in ('edl', 'sis'):
        with _override_edl_feature_flag(key == 'edl'):
            GenerateMergedHistEnrFeeds().collect_merged_enrollments(
                sids=[sid],
                term_id=term_id,
                feed_file=MockFeedFile(),
            )
    return tolerant_jsonify(result)


# get_all_advisee_term_gpas

@contextmanager
def _override_edl_feature_flag(value):
    """Temporarily override."""
    key = 'FEATURE_FLAG_ENTERPRISE_DATA_LAKE'
    old_value = app.config[key]
    app.config[key] = value
    yield
    app.config[key] = old_value


def _safety_check():
    if 'EB_ENVIRONMENT' in app.config:
        raise BadRequestError("'EDL data comparison can ONLY be run in developer's local environment'")
