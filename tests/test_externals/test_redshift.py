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


from nessie.externals import redshift
from nessie.jobs.background_job import resolve_sql_template
import psycopg2.sql
import pytest
from tests.util import capture_app_logs


@pytest.fixture()
def schema(app):
    schema = psycopg2.sql.Identifier(app.config['REDSHIFT_SCHEMA_BOAC'])
    redshift.execute('CREATE SCHEMA IF NOT EXISTS {schema}', schema=schema)
    yield
    redshift.execute('DROP SCHEMA IF EXISTS {schema} CASCADE', schema=schema)


@pytest.fixture()
def ensure_drop_schema(app):
    yield
    schema = psycopg2.sql.Identifier(app.config['REDSHIFT_SCHEMA_BOAC'])
    redshift.execute('DROP SCHEMA IF EXISTS {schema} CASCADE', schema=schema)


class TestRedshift:
    """Redshift client."""

    def test_connection_error_handling(self, app, caplog):
        """Handles and logs connection errors."""
        with capture_app_logs(app):
            app.config['REDSHIFT_HOST'] = 'H.C. Earwicker'
            redshift.execute('SELECT 1')
            assert 'could not translate host name "H.C. Earwicker" to address' in caplog.text

    @pytest.mark.testext
    def test_schema_creation_drop(self, app, caplog, ensure_drop_schema):
        """Can create and drop schemata on a real Redshift instance."""
        schema_name = app.config['REDSHIFT_SCHEMA_BOAC']
        schema = psycopg2.sql.Identifier(schema_name)
        with capture_app_logs(app):
            result = redshift.execute('CREATE SCHEMA {schema}', schema=schema)
            assert result == 'CREATE SCHEMA'

            result = redshift.execute('CREATE SCHEMA {schema}', schema=schema)
            assert result is None
            assert f'Schema "{schema_name}" already exists' in caplog.text

            result = redshift.execute('DROP SCHEMA {schema}', schema=schema)
            assert result == 'DROP SCHEMA'

    @pytest.mark.testext
    def test_execute_ddl_script(self, app, ensure_drop_schema):
        """Executes filled SQL template files one statement at a time."""

        # TODO Test CREATE EXTERNAL SCHEMA and CREATE EXTERNAL TABLE statements.

        resolved_ddl = resolve_sql_template('test_db.template.sql')
        redshift.execute_ddl_script(resolved_ddl)

        schema = psycopg2.sql.Identifier(app.config['REDSHIFT_SCHEMA_BOAC'])
        result = redshift.fetch('SELECT COUNT(*) FROM {schema}.students', schema=schema)
        assert len(result) == 1
        assert result[0].count == 7

        result = redshift.fetch("SELECT last_name FROM {schema}.students WHERE first_name = 'Paul' ORDER BY last_name", schema=schema)
        assert len(result) == 2
        assert result[0].last_name == 'Farestveit'
        assert result[1].last_name == 'Kerschen'
