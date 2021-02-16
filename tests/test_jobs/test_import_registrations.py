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

from decimal import Decimal
import json
import logging

from nessie.externals import rds, redshift
from nessie.lib.queries import student_schema
from tests.util import capture_app_logs, mock_s3, override_config


class TestImportRegistrations:

    def test_import_registrations(self, app, metadata_db, student_tables, caplog):
        from nessie.jobs.import_registrations import ImportRegistrations

        rows = redshift.fetch(f'SELECT * FROM {student_schema()}.student_term_gpas')
        assert len(rows) == 0
        rows = redshift.fetch(f'SELECT * FROM {student_schema()}.student_last_registrations')
        assert len(rows) == 0
        caplog.set_level(logging.DEBUG)
        with capture_app_logs(app):
            with mock_s3(app):
                result = ImportRegistrations().run_wrapped()
            assert result == 'Registrations import completed: 2 succeeded, 8 failed.'
            rows = redshift.fetch(f'SELECT * FROM {student_schema()}.student_term_gpas ORDER BY sid')
            assert len(rows) == 11
            for row in rows[0:6]:
                assert row['sid'] == '11667051'
            for row in rows[7:10]:
                assert row['sid'] == '1234567890'
            row_2168 = next(r for r in rows if r['term_id'] == '2168')
            assert row_2168['gpa'] == Decimal('3.000')
            assert row_2168['units_taken_for_gpa'] == Decimal('8.0')

            rows = redshift.fetch(f'SELECT * FROM {student_schema()}.student_last_registrations ORDER BY sid')
            assert len(rows) == 2
            assert rows[0]['sid'] == '11667051'
            assert rows[1]['sid'] == '1234567890'
            feed = json.loads(rows[1]['feed'], strict=False)
            assert feed['term']['id'] == '2172'
            assert feed['academicLevels'][0]['level']['description'] == 'Sophomore'

            rows = redshift.fetch(f'SELECT * FROM {student_schema()}.student_api_demographics ORDER BY sid')
            assert len(rows) == 2
            assert rows[0]['sid'] == '11667051'
            assert rows[1]['sid'] == '1234567890'
            feed = json.loads(rows[1]['feed'], strict=False)
            assert feed['gender']['genderOfRecord']['description'] == 'Female'

    def test_metadata_tracked(self, app, metadata_db, student_tables, caplog):
        from nessie.jobs.import_registrations import ImportRegistrations
        rows = rds.fetch('SELECT * FROM nessie_metadata_test.registration_import_status')
        assert len(rows) == 0
        caplog.set_level(logging.DEBUG)
        with capture_app_logs(app):
            with mock_s3(app):
                ImportRegistrations().run_wrapped()
                rows = rds.fetch('SELECT * FROM nessie_metadata_test.registration_import_status')
                assert len(rows) == 10
                assert len([r for r in rows if r['status'] == 'failure']) == 8
                assert next(r['status'] for r in rows if r['sid'] == '11667051') == 'success'
                result = ImportRegistrations().run_wrapped()
                assert result == 'Registrations import completed: 0 succeeded, 8 failed.'
                result = ImportRegistrations().run_wrapped(load_mode='all')
                assert result == 'Registrations import completed: 2 succeeded, 8 failed.'
                rds.execute("DELETE FROM nessie_metadata_test.registration_import_status WHERE sid = '11667051'")
                result = ImportRegistrations().run_wrapped()
                assert result == 'Registrations import completed: 1 succeeded, 8 failed.'
                assert next(r['status'] for r in rows if r['sid'] == '11667051') == 'success'
                rds.execute("UPDATE nessie_metadata_test.registration_import_status SET status='failure' WHERE sid = '11667051'")
                result = ImportRegistrations().run_wrapped()
                assert result == 'Registrations import completed: 1 succeeded, 8 failed.'
                assert next(r['status'] for r in rows if r['sid'] == '11667051') == 'success'

    def test_import_registrations_batch_mode(self, app, metadata_db, student_tables, caplog):
        from nessie.jobs.import_registrations import ImportRegistrations
        with mock_s3(app):
            ImportRegistrations().run_wrapped()
            rows = rds.fetch('SELECT * FROM nessie_metadata_test.registration_import_status')
            assert len(rows) == 10

            with override_config(app, 'CYCLICAL_API_IMPORT_BATCH_SIZE', 9):

                def _success_history_after_batch_import():
                    result = ImportRegistrations().run_wrapped(load_mode='batch')
                    assert result == 'Registrations import completed: 1 succeeded, 8 failed.'
                    rows = rds.fetch("SELECT * FROM nessie_metadata_test.registration_import_status WHERE status = 'success' ORDER BY updated_at")
                    assert len(rows) == 2
                    assert rows[0]['updated_at'] < rows[1]['updated_at']
                    return (rows[0]['sid'], rows[1]['sid'])

                sid_1, sid_2 = _success_history_after_batch_import()
                assert _success_history_after_batch_import() == (sid_2, sid_1)
                assert _success_history_after_batch_import() == (sid_1, sid_2)
