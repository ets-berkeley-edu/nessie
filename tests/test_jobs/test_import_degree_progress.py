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

from nessie.jobs.import_degree_progress import ImportDegreeProgress
from nessie.models import json_cache
from tests.util import assert_background_job_status, capture_app_logs


class TestImportDegreeProgress:

    def test_import_loop(self, app, caplog, metadata_db):
        """Loops through provided ids and attempts degree progress API import."""
        non_undergrad_csid = '9999999'
        brigitte_csid = '11667051'

        with capture_app_logs(app):
            result = ImportDegreeProgress().run_wrapped(csids=[non_undergrad_csid, brigitte_csid])
            assert_background_job_status('ImportDegreeProgress')
            assert result is True
            assert 'Starting SIS degree progress API import job for 2 students' in caplog.text
            assert 'SIS get_degree_progress failed for CSID 9999999' in caplog.text
            assert 'SIS degree progress API import job completed: 1 succeeded, 1 failed' in caplog.text

            assert json_cache.fetch(f'sis_degree_progress_api_{non_undergrad_csid}') is False
            cached_brigitte_progress = json_cache.fetch(f'sis_degree_progress_api_{brigitte_csid}')
            brigitte_progress = cached_brigitte_progress['UC_AA_PROGRESS']['PROGRESSES']['PROGRESS']

            assert brigitte_progress['RPT_DATE'] == '2017-03-03'
            assert brigitte_progress['REQUIREMENTS']['REQUIREMENT'][1]['NAME'] == 'American History (R-0002)'
