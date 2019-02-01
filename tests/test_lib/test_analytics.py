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

import io
from itertools import groupby
import operator

from nessie.lib import analytics, queries
from nessie.lib.mockingdata import MockRows, register_mock
from nessie.merged.student_terms import get_canvas_site_map, merge_memberships_into_site_map


class TestAnalytics:
    """Analytics."""

    def test_ordinal(self):
        """Format a whole number as a position."""
        assert analytics.ordinal(1) == '1st'
        assert analytics.ordinal(2) == '2nd'
        assert analytics.ordinal(3) == '3rd'
        for i in range(4, 20):
            assert analytics.ordinal(i) == f'{i}th'
        assert analytics.ordinal(21) == '21st'
        assert analytics.ordinal(22) == '22nd'
        assert analytics.ordinal(23) == '23rd'


def get_relative_submission_counts():
    all_counts = queries.get_advisee_submissions_sorted()
    for canvas_user_id, sites_grp in groupby(all_counts, key=operator.itemgetter('reference_user_id')):
        if canvas_user_id == 9000100:
            relative_submission_counts = {}
            for canvas_course_id, subs_grp in groupby(sites_grp, key=operator.itemgetter('canvas_course_id')):
                relative_submission_counts[canvas_course_id] = list(subs_grp)
            return relative_submission_counts
    return {}


class TestAnalyticsFromAssignmentsSubmitted:
    canvas_user_id = 9000100
    canvas_course_id = 7654321

    def test_from_fixture(self, app):
        digested = analytics.assignments_submitted(
            self.canvas_user_id,
            self.canvas_course_id,
            get_relative_submission_counts(),
        )
        assert digested['student']['raw'] == 8
        assert digested['student']['percentile'] == 64
        assert digested['student']['roundedUpPercentile'] == 81
        assert digested['courseDeciles'][0] == 0
        assert digested['courseDeciles'][9] == 10
        assert digested['courseDeciles'][10] == 17
        assert round(digested['courseMean']['raw']) == 7
        assert digested['courseMean']['percentile'] == 50

    def test_small_difference(self, app):
        """Notices that small difference."""
        rows = [
            'reference_user_id,sid,canvas_course_id,canvas_user_id,submissions_turned_in',
            ','.join(['9000100', '9000000', str(self.canvas_course_id), '9000000', '1']),
            ','.join(['9000100', str(self.canvas_user_id), str(self.canvas_course_id), str(self.canvas_user_id), '3']),
        ]
        for i in range(101, 301):
            rows.append(','.join(['9000100', str(i), str(self.canvas_course_id), str(i), '2']))
        mr = MockRows(io.StringIO('\n'.join(rows)))
        with register_mock(queries.get_advisee_submissions_sorted, mr):
            worst = analytics.assignments_submitted(
                '9000000',
                self.canvas_course_id,
                get_relative_submission_counts(),
            )
            best = analytics.assignments_submitted(
                self.canvas_user_id,
                self.canvas_course_id,
                get_relative_submission_counts(),
            )
            median = analytics.assignments_submitted(
                '101',
                self.canvas_course_id,
                get_relative_submission_counts(),
            )
            for digested in [worst, best, median]:
                assert digested['boxPlottable'] is False
                assert digested['student']['percentile'] is not None
            assert worst['displayPercentile'] == '0th'
            assert worst['student']['raw'] == 1
            assert median['displayPercentile'] == '99th'
            assert median['student']['raw'] == 2
            assert median['student']['roundedUpPercentile'] == 99
            assert median['student']['percentile'] != 99
            assert best['displayPercentile'] == '100th'
            assert best['student']['raw'] == 3

    def test_when_no_data(self, app):
        mr = MockRows(io.StringIO('reference_user_id,sid,canvas_course_id,canvas_user_id,submissions_turned_in'))
        with register_mock(queries.get_advisee_submissions_sorted, mr):
            digested = analytics.assignments_submitted(
                self.canvas_user_id,
                self.canvas_course_id,
                get_relative_submission_counts(),
            )
        assert digested['student']['raw'] is None
        assert digested['student']['percentile'] is None
        assert digested['boxPlottable'] is False
        assert digested['courseDeciles'] is None
        assert digested['courseMean'] is None


class TestStudentAnalytics:
    canvas_user_id = 9000100
    canvas_course_id = 7654321

    def canvas_site_map(self, app):
        (canvas_site_map, advisee_site_map) = get_canvas_site_map()
        merge_memberships_into_site_map(canvas_site_map)
        return canvas_site_map

    def test_from_fixture(self, app):
        digested = analytics.student_analytics(self.canvas_user_id, self.canvas_course_id, self.canvas_site_map(app))
        score = digested['currentScore']
        assert score['student']['raw'] == 84
        assert score['student']['percentile'] == 73
        assert score['student']['roundedUpPercentile'] == 76
        assert score['courseDeciles'][0] == 47
        assert score['courseDeciles'][9] == 94
        assert score['courseDeciles'][10] == 104
        assert round(score['courseMean']['raw']) == 77
        assert score['courseMean']['percentile'] == 50
        last_activity = digested['lastActivity']
        assert last_activity['student']['raw'] == 1535275620
        assert last_activity['student']['percentile'] == 93
        assert last_activity['student']['roundedUpPercentile'] == 90
        assert last_activity['courseDeciles'][0] == 0
        assert last_activity['courseDeciles'][9] == 1535264940
        assert last_activity['courseDeciles'][10] == 1535533860
        assert round(last_activity['courseMean']['raw']) == 1534450050
        assert last_activity['courseMean']['percentile'] == 50

    def test_with_empty_redshift(self, app):
        bad_course_id = 'NoSuchSite'
        digested = analytics.student_analytics(self.canvas_user_id, bad_course_id, self.canvas_site_map(app))
        _error = {'error': 'Redshift query returned no results'}
        assert digested == {'currentScore': _error, 'lastActivity': _error}

    def test_when_no_data(self, app):
        exclusive_rows = 'canvas_course_id,canvas_user_id,course_scores,last_activity_at,sis_enrollment_status\n' \
            '7654321,1,1,1,E'
        mr = MockRows(io.StringIO(exclusive_rows))
        with register_mock(queries.get_all_enrollments_in_advisee_canvas_sites, mr):
            digested = analytics.student_analytics(self.canvas_user_id, self.canvas_course_id, self.canvas_site_map(app))
        score = digested['currentScore']
        assert score['student']['raw'] is None
        assert score['student']['percentile'] is None
        assert score['boxPlottable'] is False
        assert score['courseDeciles'] is None
        assert score['courseMean'] is None
        last_activity = digested['lastActivity']
        assert last_activity['student']['raw'] == 0

    def test_mean_with_zero_dates(self, app):
        site_map = self.canvas_site_map(app)
        enrollments = []
        for user_id in range(9000071, 9000075):
            enrollments.append({
                'course_id': 7654321, 'canvas_user_id': user_id, 'current_score': 100, 'last_activity_at': 0,
            })
        enrollments[1]['last_activity_at'] = 1535340480
        enrollments[2]['last_activity_at'] = 1535340481
        enrollments[3]['last_activity_at'] = 1535340482
        site_map[7654321]['enrollments'] = enrollments
        digested = analytics.student_analytics(9000071, self.canvas_course_id, site_map)
        low_student_score = digested['lastActivity']['student']
        assert(low_student_score['raw']) == 0
        assert(low_student_score['matrixyPercentile']) == 0
        assert(low_student_score['roundedUpPercentile']) == 25
        mean = digested['lastActivity']['courseMean']
        assert mean['raw'] == 1535340481
        assert mean['percentile'] == 50
        assert mean['roundedUpPercentile'] > 50
        middling_student = analytics.student_analytics(9000073, self.canvas_course_id, site_map)
        middling_student_score = middling_student['lastActivity']['student']
        assert(middling_student_score['raw']) == mean['raw']
        assert(middling_student_score['matrixyPercentile']) == mean['matrixyPercentile']
        assert(middling_student_score['roundedUpPercentile']) == mean['roundedUpPercentile']

    def test_mean_with_low_dates(self, app):
        site_map = self.canvas_site_map(app)
        enrollments = []
        for user_id in range(9000071, 9000075):
            enrollments.append({
                'course_id': 7654321, 'canvas_user_id': user_id, 'current_score': 100, 'last_activity_at': 0,
            })
        enrollments[0]['last_activity_at'] = 1535240480
        enrollments[1]['last_activity_at'] = 1535340480
        enrollments[2]['last_activity_at'] = 1535340481
        enrollments[3]['last_activity_at'] = 1535340482
        site_map[7654321]['enrollments'] = enrollments
        digested = analytics.student_analytics(9000071, self.canvas_course_id, site_map)
        mean = digested['lastActivity']['courseMean']
        assert mean['raw'] < 1535340481
        assert mean['percentile'] == 50
        assert mean['roundedUpPercentile'] < 50
