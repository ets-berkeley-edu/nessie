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

from nessie.jobs.import_sis_enrollments_api import ImportSisEnrollmentsApi
from nessie.models import json_cache
from tests.util import assert_background_job_status, capture_app_logs


class TestImportSisEnrollmentsApi:

    def test_enrollments_api_import_loop(self, app, caplog, metadata_db):
        """Loops through provided ids and attempts SIS enrollments API import."""
        bad_csid = '9999999'
        brigitte_csid = '11667051'

        with capture_app_logs(app):
            result = ImportSisEnrollmentsApi().run_wrapped(csids=[bad_csid, brigitte_csid])
            assert result is True
            assert_background_job_status('ImportSisEnrollmentsApi')
            assert 'Starting SIS enrollments API import job for term 2178, 2 students' in caplog.text
            assert 'SIS enrollments API import failed for CSID 9999999' in caplog.text
            assert 'SIS enrollments API import job completed: 1 succeeded, 1 failed' in caplog.text

            assert json_cache.fetch(f'term_Fall 2017-sis_drops_and_midterms_{bad_csid}') is False
            brigitte_drops_and_midterms = json_cache.fetch(f'term_Fall 2017-sis_drops_and_midterms_{brigitte_csid}')
            assert brigitte_drops_and_midterms['midtermGrades']['90100'] == 'D+'
