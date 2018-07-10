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


import os

import nessie.factory
import pytest

# Test environment defaults to 'test' unless 'testext' is explicitly specified.

if os.environ.get('NESSIE_ENV') != 'testext':
    os.environ['NESSIE_ENV'] = 'test'


# When NESSIE_ENV is 'testext', only tests marked @pytest.mark.testext will run. Otherwise,
# all other tests will run.

def pytest_cmdline_preparse(args):
    if os.environ['NESSIE_ENV'] == 'testext':
        args[:] = ['-m', 'testext'] + args
    else:
        args[:] = ['-m', 'not testext'] + args


# Because app and db fixtures are only created once per pytest run, individual tests
# are not able to modify application configuration values before the app is created.
# Per-test customizations could be supported via a fixture scope of 'function' and
# the @pytest.mark.parametrize annotation.

@pytest.fixture(scope='session')
def app(request):
    """Fixture application object, shared by all tests."""
    _app = nessie.factory.create_app()

    # Create app context before running tests.
    ctx = _app.app_context()
    ctx.push()

    # Pop the context after running tests.
    def teardown():
        ctx.pop()
    request.addfinalizer(teardown)

    return _app


# TODO Perform DB schema creation and deletion outside an app context, enabling test-specific app configurations.
@pytest.fixture(scope='session')
def db(app, request):
    """Fixture database object, shared by all tests."""
    from nessie.models import development_db
    # Drop all tables before re-loading the schemas.
    # If we dropped at teardown instead, an interrupted test run would block the next test run.
    development_db.clear()
    _db = development_db.load()

    return _db


@pytest.fixture(scope='function', autouse=True)
def db_session(db, request):
    """Fixture database session used for the scope of a single test.

    All executions are wrapped in a session and then rolled back to keep individual tests isolated.
    """
    # Mixing SQL-using test fixtures with SQL-using decorators seems to cause timing issues with pytest's
    # fixture finalizers. Instead of using a finalizer to roll back the session and close connections,
    # we begin by cleaning up any previous invocations.
    # This fixture is marked 'autouse' to ensure that cleanup happens at the start of every test, whether
    # or not it has an explicit database dependency.
    db.session.rollback()
    try:
        db.session.get_bind().close()
    # The session bind will close only if it was provided a specific connection via this fixture.
    except AttributeError:
        pass
    db.session.remove()

    connection = db.engine.connect()
    options = dict(bind=connection, binds={})
    _session = db.create_scoped_session(options=options)
    db.session = _session

    return _session


@pytest.fixture()
def metadata_db(app):
    """Use Postgres to mock the Redshift metadata schema on local test runs."""
    from nessie.externals import redshift
    schema = app.config['REDSHIFT_SCHEMA_METADATA']
    redshift.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
    redshift.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
    redshift.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.background_job_status
    (
        job_id VARCHAR NOT NULL,
        status VARCHAR NOT NULL,
        instance_id VARCHAR,
        error VARCHAR(4096),
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL
    )""")
    redshift.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.canvas_sync_job_status
    (
       job_id VARCHAR NOT NULL,
       filename VARCHAR NOT NULL,
       canvas_table VARCHAR NOT NULL,
       source_url VARCHAR NOT NULL,
       source_size BIGINT,
       destination_url VARCHAR,
       destination_size BIGINT,
       status VARCHAR NOT NULL,
       details VARCHAR,
       instance_id VARCHAR,
       created_at TIMESTAMP NOT NULL,
       updated_at TIMESTAMP NOT NULL
    )""")
    redshift.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.canvas_synced_snapshots
    (
        filename VARCHAR NOT NULL,
        canvas_table VARCHAR NOT NULL,
        url VARCHAR NOT NULL,
        size BIGINT NOT NULL,
        created_at TIMESTAMP NOT NULL,
        deleted_at TIMESTAMP
    )""")


@pytest.fixture()
def cleanup_s3(app):
    yield
    from nessie.externals import s3
    keys = s3.get_keys_with_prefix(app.config['LOCH_S3_PREFIX_TESTEXT'])
    s3.delete_objects(keys)


def pytest_itemcollected(item):
    """Print docstrings during test runs for more readable output."""
    par = item.parent.obj
    node = item.obj
    pref = par.__doc__.strip() if par.__doc__ else par.__class__.__name__
    suf = node.__doc__.strip() if node.__doc__ else node.__name__
    if pref or suf:
        item._nodeid = ' '.join((pref, suf))
