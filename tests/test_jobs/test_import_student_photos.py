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

import logging

from nessie.externals import rds, s3
from tests.util import capture_app_logs, mock_s3


class TestImportStudentPhotos:

    def test_import_student_photos(self, app, metadata_db, student_tables, caplog):
        from nessie.jobs.import_student_photos import ImportStudentPhotos
        caplog.set_level(logging.DEBUG)
        with capture_app_logs(app):
            with mock_s3(app):
                result = ImportStudentPhotos().run_wrapped()
                assert result == 'Student photo import completed: 1 succeeded, 8 had no photo available, 0 failed.'
                response = s3.get_keys_with_prefix('cal1card-data/photos')
                assert len(response) == 1
                assert response[0] == 'cal1card-data/photos/61889.jpg'

            success_rows = rds.fetch(f"SELECT * FROM {app.config['RDS_SCHEMA_METADATA']}.photo_import_status WHERE status = 'success'")
            assert len(success_rows) == 1
            assert success_rows[0]['sid'] == '11667051'

            failure_rows = rds.fetch(f"SELECT * FROM {app.config['RDS_SCHEMA_METADATA']}.photo_import_status WHERE status = 'failure'")
            assert len(failure_rows) == 0

            not_found_rows = rds.fetch(f"SELECT * FROM {app.config['RDS_SCHEMA_METADATA']}.photo_import_status WHERE status = 'photo_not_found'")
            assert len(not_found_rows) == 8
