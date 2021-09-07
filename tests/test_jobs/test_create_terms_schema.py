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

from datetime import datetime

import mock
from nessie.externals import rds
from nessie.lib.util import resolve_sql_template
import pytest


@pytest.fixture
def term_definitions(app):
    rds_schema = app.config['RDS_SCHEMA_TERMS']
    rds.execute(f'DROP SCHEMA {rds_schema} CASCADE')
    rds.execute(resolve_sql_template('create_rds_indexes.template.sql'))
    rds.execute(f"""INSERT INTO {rds_schema}.term_definitions
        (term_id, term_name, term_begins, term_ends)
        VALUES
        ('2172', 'Spring 2017', '2017-01-10', '2017-05-12'),
        ('2175', 'Summer 2017', '2017-05-22', '2017-08-11'),
        ('2178', 'Fall 2017', '2017-08-16', '2017-12-15'),
        ('2182', 'Spring 2018', '2018-01-09', '2018-05-11'),
        ('2185', 'Summer 2018', '2018-05-21', '2018-08-10'),
        ('2188', 'Fall 2018', '2018-08-15', '2018-12-14'),
        ('2192', 'Spring 2019', '2019-01-15', '2019-05-17'),
        ('2195', 'Summer 2019', '2019-05-28', '2019-08-16'),
        ('2198', 'Fall 2019', '2019-08-21', '2019-12-20'),
        ('2202', 'Spring 2020', '2020-01-14', '2020-05-15'),
        ('2205', 'Summer 2020', '2020-05-26', '2020-08-14'),
        ('2208', 'Fall 2020', '2020-08-19', '2020-12-18')
    """)


class TestCreateSisTermsSchema:

    def refresh_term_index(self, app):
        from nessie.jobs.create_terms_schema import CreateTermsSchema

        CreateTermsSchema().refresh_current_term_index()
        rds_schema = app.config['RDS_SCHEMA_TERMS']
        rows = rds.fetch(f'SELECT * FROM {rds_schema}.current_term_index')
        assert len(rows) == 1
        return rows[0]

    @mock.patch('nessie.jobs.create_terms_schema.datetime', autospec=True)
    def test_early_spring(self, mock_datetime, app, term_definitions):
        mock_datetime.now.return_value = datetime(year=2018, month=1, day=9, hour=12, minute=21)
        terms = self.refresh_term_index(app)
        assert terms['current_term_name'] == 'Spring 2018'
        assert terms['future_term_name'] == 'Spring 2018'

    @mock.patch('nessie.jobs.create_terms_schema.datetime', autospec=True)
    def test_mid_spring(self, mock_datetime, app, term_definitions):
        mock_datetime.now.return_value = datetime(year=2018, month=3, day=13, hour=12, minute=21)
        terms = self.refresh_term_index(app)
        assert terms['current_term_name'] == 'Spring 2018'
        assert terms['future_term_name'] == 'Summer 2018'

    @mock.patch('nessie.jobs.create_terms_schema.datetime', autospec=True)
    def test_late_spring(self, mock_datetime, app, term_definitions):
        mock_datetime.now.return_value = datetime(year=2018, month=5, day=11, hour=12, minute=21)
        terms = self.refresh_term_index(app)
        assert terms['current_term_name'] == 'Spring 2018'
        assert terms['future_term_name'] == 'Fall 2018'

    @mock.patch('nessie.jobs.create_terms_schema.datetime', autospec=True)
    def test_post_spring_grace_period(self, mock_datetime, app, term_definitions):
        mock_datetime.now.return_value = datetime(year=2018, month=5, day=20, hour=12, minute=21)
        terms = self.refresh_term_index(app)
        assert terms['current_term_name'] == 'Spring 2018'
        assert terms['future_term_name'] == 'Fall 2018'

    @mock.patch('nessie.jobs.create_terms_schema.datetime', autospec=True)
    def test_early_summer(self, mock_datetime, app, term_definitions):
        mock_datetime.now.return_value = datetime(year=2018, month=5, day=21, hour=12, minute=21)
        terms = self.refresh_term_index(app)
        assert terms['current_term_name'] == 'Summer 2018'
        assert terms['future_term_name'] == 'Fall 2018'

    @mock.patch('nessie.jobs.create_terms_schema.datetime', autospec=True)
    def test_post_summer_grace_period(self, mock_datetime, app, term_definitions):
        mock_datetime.now.return_value = datetime(year=2018, month=8, day=14, hour=12, minute=21)
        terms = self.refresh_term_index(app)
        assert terms['current_term_name'] == 'Summer 2018'
        assert terms['future_term_name'] == 'Fall 2018'

    @mock.patch('nessie.jobs.create_terms_schema.datetime', autospec=True)
    def test_early_fall(self, mock_datetime, app, term_definitions):
        mock_datetime.now.return_value = datetime(year=2018, month=8, day=15, hour=12, minute=21)
        terms = self.refresh_term_index(app)
        assert terms['current_term_name'] == 'Fall 2018'
        assert terms['future_term_name'] == 'Fall 2018'

    @mock.patch('nessie.jobs.create_terms_schema.datetime', autospec=True)
    def test_mid_fall(self, mock_datetime, app, term_definitions):
        mock_datetime.now.return_value = datetime(year=2018, month=10, day=13, hour=12, minute=21)
        terms = self.refresh_term_index(app)
        assert terms['current_term_name'] == 'Fall 2018'
        assert terms['future_term_name'] == 'Spring 2019'

    @mock.patch('nessie.jobs.create_terms_schema.datetime', autospec=True)
    def test_post_fall_grace_period(self, mock_datetime, app, term_definitions):
        mock_datetime.now.return_value = datetime(year=2018, month=12, day=24, hour=12, minute=21)
        terms = self.refresh_term_index(app)
        assert terms['current_term_name'] == 'Fall 2018'
        assert terms['future_term_name'] == 'Spring 2019'
