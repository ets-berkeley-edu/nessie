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

from decimal import Decimal
import logging

from nessie.externals import redshift
from tests.util import capture_app_logs, mock_s3


class TestImportTermGpas:

    def test_import_term_gpas(self, app, metadata_db, student_tables, caplog):
        from nessie.jobs.import_term_gpas import ImportTermGpas
        caplog.set_level(logging.DEBUG)
        with capture_app_logs(app):
            with mock_s3(app):
                result = ImportTermGpas().run_wrapped()
            assert result == 'Term GPA import completed: 1 succeeded, 0 returned no registrations, 7 failed.'
            rows = redshift.fetch('SELECT * FROM student_test.student_term_gpas')
            assert len(rows) == 7
            for row in rows:
                assert row['sid'] == '11667051'
            row_2178 = next(r for r in rows if r['term_id'] == '2178')
            assert row_2178['gpa'] == Decimal('3.000')
            assert row_2178['units_taken_for_gpa'] == Decimal('8.0')
