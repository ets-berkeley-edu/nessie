"""
Copyright ©2018. The Regents of the University of California (Regents). All Rights Reserved.

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


class TestImportSisEnrollmentsApi:

    def test_import_sis_enrollments_api(self, app, metadata_db, student_tables, caplog):
        from nessie.jobs.import_sis_enrollments_api import ImportSisEnrollmentsApi
        with mock_s3(app):
            result = ImportSisEnrollmentsApi().run_wrapped()
        assert result == 'SIS enrollments API import completed for term 2178: 1 succeeded, 7 returned no enrollments, 0 failed.'
        rows = redshift.fetch('SELECT * FROM student_test.sis_api_drops_and_midterms')
        assert len(rows) == 1
        assert rows[0]['sid'] == '11667051'
        feed = json.loads(rows[0]['feed'])
        assert feed['droppedPrimarySections'][0]['displayName'] == 'MUSIC 41C'
        assert feed['droppedPrimarySections'][0]['component'] == 'TUT'
        assert feed['droppedPrimarySections'][0]['sectionNumber'] == '002'
        assert feed['midtermGrades']['90100'] == 'D+'
