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


@pytest.fixture()
def metadata_db(app):
    """Use Postgres to locally mock the metadata schema."""
    from nessie.externals import rds

    rds_schema = app.config['RDS_SCHEMA_METADATA']
    rds.execute(f'DROP SCHEMA IF EXISTS {rds_schema} CASCADE')
    rds.execute(f'CREATE SCHEMA IF NOT EXISTS {rds_schema}')
    rds.execute(f"""CREATE TABLE IF NOT EXISTS {rds_schema}.background_job_status
    (
        job_id VARCHAR NOT NULL,
        status VARCHAR NOT NULL,
        instance_id VARCHAR,
        details VARCHAR(4096),
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL
    )""")
    rds.execute(f"""CREATE TABLE IF NOT EXISTS {rds_schema}.canvas_sync_job_status
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
    rds.execute(f"""CREATE TABLE IF NOT EXISTS {rds_schema}.canvas_synced_snapshots
    (
        filename VARCHAR NOT NULL,
        canvas_table VARCHAR NOT NULL,
        url VARCHAR NOT NULL,
        size BIGINT NOT NULL,
        created_at TIMESTAMP NOT NULL,
        deleted_at TIMESTAMP
    )""")
    rds.execute(f"""CREATE TABLE IF NOT EXISTS {rds_schema}.registration_import_status
    (
        sid VARCHAR NOT NULL PRIMARY KEY,
        status VARCHAR NOT NULL,
        updated_at TIMESTAMP NOT NULL
    );""")
    rds.execute(f"""CREATE TABLE IF NOT EXISTS {rds_schema}.merged_enrollment_term_job_queue
    (
       id SERIAL PRIMARY KEY,
       master_job_id VARCHAR NOT NULL,
       term_id VARCHAR NOT NULL,
       status VARCHAR NOT NULL,
       details VARCHAR,
       instance_id VARCHAR,
       created_at TIMESTAMP NOT NULL,
       updated_at TIMESTAMP NOT NULL
    )""")
    rds.execute(f"""CREATE TABLE IF NOT EXISTS {rds_schema}.photo_import_status
    (
        sid VARCHAR NOT NULL PRIMARY KEY,
        status VARCHAR NOT NULL,
        updated_at TIMESTAMP NOT NULL
    );""")


@pytest.fixture()
def student_tables(app):
    """Use Postgres to mock the Redshift student schemas on local test runs."""
    from nessie.externals import rds, redshift
    from nessie.lib.util import resolve_sql_template_string, resolve_sql_template
    rds.execute('DROP SCHEMA sis_internal_test CASCADE')
    rds.execute(resolve_sql_template('create_rds_indexes.template.sql'))
    fixture_path = f"{app.config['BASE_DIR']}/fixtures"
    with open(f'{fixture_path}/students.sql', 'r') as sql_file:
        student_sql = sql_file.read()
    params = {}
    for key in [
        'sis_degree_progress_11667051',
        'sis_student_api_v1_11667051',
        'sis_student_api_v1_1234567890',
        'sis_student_api_v1_2345678901',
        'sis_student_api_v1_5000000000',
    ]:
        with open(f'{fixture_path}/{key}.json', 'r') as f:
            feed = f.read()
            if key.startswith('sis_student_api'):
                feed = json.dumps(json.loads(feed)['apiResponse']['response']['any']['students'][0])
            params[key] = feed
    redshift.execute(resolve_sql_template_string(student_sql), params=params)
    yield
    for schema in ['advisee_test', 'asc_test', 'coe_test', 'student_test', 'undergrads_test']:
        rds.execute(f'DROP SCHEMA {schema} CASCADE')
        redshift.execute(f'DROP SCHEMA {schema} CASCADE')
    redshift.execute('DROP SCHEMA calnet_test CASCADE')


@pytest.fixture()
def sis_note_tables(app):
    """Use Postgres to mock the Redshift SIS note schemas on local test runs."""
    from nessie.externals import redshift
    internal_schema = app.config['REDSHIFT_SCHEMA_SIS_ADVISING_NOTES_INTERNAL']
    redshift.execute(f'DROP SCHEMA IF EXISTS {internal_schema} CASCADE')
    redshift.execute(f'CREATE SCHEMA {internal_schema}')
    redshift.execute(f"""CREATE TABLE {internal_schema}.advising_note_attachments
    (
        advising_note_id character varying(513),
        sid character varying(256),
        student_note_nr character varying(256),
        created_by character varying(256),
        user_file_name character varying(256),
        sis_file_name character varying(781)
    )""")
    redshift.execute(f"""INSERT INTO {internal_schema}.advising_note_attachments
        (advising_note_id, sid, student_note_nr, created_by, user_file_name, sis_file_name)
        VALUES
        ('12345678-00012', '12345678', '00012', '123', 'Sp_19_French_1B.pdf', '12345678_00012_1.pdf'),
        ('23456789-00003', '23456789', '00003', '123', 'Advising_Notes_test_attachment_document', '23456789_00003_1'),
        ('34567890-00014', '34567890', '00014', '123', 'notes._500,1M1L4H0N_ref_].16SVX', '34567890_00014_2.16SVX')
    """)
    yield
    redshift.execute(f'DROP SCHEMA {internal_schema} CASCADE')


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
