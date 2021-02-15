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

import json

from nessie.externals import redshift
from nessie.lib.queries import student_schema
import pytest
from tests.util import mock_s3


class TestImportDegreeProgress:

    @pytest.mark.skip(reason='We mock Redshift with local Postgres. Unfortunately, it does not handle Spectrum syntax.')
    def test_import_degree_progress(self, app, metadata_db, student_tables, caplog):
        from nessie.jobs.import_degree_progress import ImportDegreeProgress
        with mock_s3(app):
            result = ImportDegreeProgress().run_wrapped()
        assert result == 'SIS degree progress API import job completed: 1 succeeded, 9 returned no information, 0 failed.'
        rows = redshift.fetch(f'SELECT * FROM {student_schema()}.sis_api_degree_progress')
        assert len(rows) == 1
        assert rows[0]['sid'] == '11667051'
        feed = json.loads(rows[0]['feed'])
        assert feed['requirements']['entryLevelWriting']['status'] == 'Satisfied'
