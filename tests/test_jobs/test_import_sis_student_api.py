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

from nessie.externals import redshift
from tests.util import mock_s3


class TestImportSisStudentApi:

    def test_import_sis_student_api(self, app, metadata_db, student_tables, caplog):
        from nessie.jobs.import_sis_student_api import ImportSisStudentApi
        initial_rows = redshift.fetch('SELECT * FROM student_test.sis_api_profiles ORDER BY sid')
        assert len(initial_rows) == 0
        with mock_s3(app):
            result = ImportSisStudentApi().run_wrapped()
        assert result == 'SIS student API import job completed: 3 succeeded, 6 failed.'
        rows = redshift.fetch('SELECT * FROM student_test.sis_api_profiles ORDER BY sid')
        assert len(rows) == 3
        assert rows[0]['sid'] == '11667051'
        feed = json.loads(rows[0]['feed'], strict=False)
        assert feed['names'][0]['familyName'] == 'Bear'
        assert feed['registrations'][0]['term']['id'] == '2178'
        assert rows[1]['sid'] == '1234567890'
        feed = json.loads(rows[1]['feed'], strict=False)
        # Needed to test proper sis_profile merging of last_registrations table.
        assert not feed.get('registrations')
        assert rows[2]['sid'] == '2345678901'
        feed = json.loads(rows[2]['feed'], strict=False)
        assert feed['registrations'][0]['term']['id'] == '2178'
