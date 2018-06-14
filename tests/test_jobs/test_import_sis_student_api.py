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

from nessie.jobs.import_sis_student_api import ImportSisStudentApi
from nessie.models import json_cache
import pytest
from tests.util import capture_app_logs


class TestImportSisStudentApi:

    def test_student_api_import_loop(self, app, caplog):
        """Loops through provided ids and attempts SIS student API import."""
        bad_csid = '9999999'
        brigitte_csid = '11667051'
        doolittle_csid = '2345678901'

        with capture_app_logs(app):
            result = ImportSisStudentApi().run(csids=[bad_csid, brigitte_csid, doolittle_csid])
            assert result is True
            assert 'Starting SIS student API import job for 3 students' in caplog.text
            assert 'SIS student API import failed for CSID 9999999' in caplog.text
            assert 'SIS student API import job completed: 2 succeeded, 1 failed' in caplog.text

            assert json_cache.fetch(f'sis_student_api_{bad_csid}') is None
            doolittle_status = json_cache.fetch(f'sis_student_api_{doolittle_csid}')
            assert doolittle_status['academicStatuses'][0]['cumulativeGPA']['average'] == pytest.approx(3.3, 0.01)
            brigitte_status = json_cache.fetch(f'sis_student_api_{brigitte_csid}')
            assert brigitte_status['academicStatuses'][0]['currentRegistration']['academicCareer']['code'] == 'UCBX'
            assert brigitte_status['academicStatuses'][1]['currentRegistration']['academicCareer']['code'] == 'UGRD'
            assert brigitte_status['academicStatuses'][1]['studentPlans'][0]['academicPlan']['plan']['description'] == 'English BA'
