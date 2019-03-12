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

from nessie.merged.student_terms import generate_student_term_maps


class TestMergedSisEnrollments:
    oski_sid = '11667051'
    oski_canvas_id = '9000100'
    advisees_by_canvas_id = {oski_canvas_id: {'sid': oski_sid}}
    advisees_by_sid = {oski_sid: {'canvas_user_id': oski_canvas_id}}

    def test_merges_drops(self, app, student_tables):
        terms_feed, canvas_site_feed = generate_student_term_maps(self.advisees_by_canvas_id, self.advisees_by_sid)
        drops = terms_feed['2178'][self.oski_sid]['droppedSections']
        assert 2 == len(drops)
        assert drops[0] == {'displayName': 'ENV,RES C9', 'component': 'STD', 'sectionNumber': '001', 'withdrawAfterDeadline': True}
        assert drops[1] == {'displayName': 'HISTORY 10CH', 'component': 'LEC', 'sectionNumber': '003', 'withdrawAfterDeadline': False}
        enrollments = terms_feed['2178'][self.oski_sid]['enrollments']
        assert any(enr['displayName'] == 'ENV,RES C9' for enr in enrollments) is False

    def test_includes_midterm_grades(self, app, student_tables):
        terms_feed, canvas_site_feed = generate_student_term_maps(self.advisees_by_canvas_id, self.advisees_by_sid)
        term_feed = terms_feed['2178'][self.oski_sid]
        assert '2178' == term_feed['termId']
        enrollments = term_feed['enrollments']
        assert 4 == len(enrollments)
        assert 'D-' == enrollments[0]['midtermGrade']
        assert 'BURMESE 1A' == enrollments[0]['displayName']
        assert 90100 == enrollments[0]['sections'][0]['ccn']

    def test_includes_course_site_section_mappings(self, app):
        """Maps Canvas sites to SIS courses and sections."""
        terms_feed, canvas_site_feed = generate_student_term_maps(self.advisees_by_canvas_id, self.advisees_by_sid)
        term_feed = terms_feed['2178'][self.oski_sid]
        enrollments = term_feed['enrollments']
        assert len(enrollments[0]['canvasSites']) == 1
        assert enrollments[0]['canvasSites'][0]['canvasCourseId'] == 7654320
        assert enrollments[0]['sections'][0]['canvasCourseIds'] == [7654320]
        assert len(enrollments[1]['canvasSites']) == 2
        assert enrollments[1]['canvasSites'][0]['canvasCourseId'] == 7654321
        assert enrollments[1]['canvasSites'][1]['canvasCourseId'] == 7654330
        assert enrollments[1]['sections'][0]['canvasCourseIds'] == [7654321, 7654330]
        assert len(enrollments[2]['canvasSites']) == 2
        assert enrollments[2]['canvasSites'][0]['canvasCourseId'] == 7654323
        assert enrollments[2]['canvasSites'][1]['canvasCourseId'] == 7654330
        assert (enrollments[2]['sections'][0]['canvasCourseIds']) == [7654323, 7654330]
        assert (enrollments[2]['sections'][1]['canvasCourseIds']) == [7654330]
