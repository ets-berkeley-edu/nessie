"""
Copyright ©2022. The Regents of the University of California (Regents). All Rights Reserved.

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

from contextlib import contextmanager
from datetime import datetime
import io
import re

from flask import current_app as app
from nessie.externals import s3
from nessie.lib.db import get_psycopg_cursor, get_psycopg_cursor_streaming
from nessie.lib.util import get_s3_sis_daily_path
import psycopg2
import psycopg2.extras
import psycopg2.sql

"""Client code to run queries against Redshift."""

# Batch size to use when streaming large result sets.
CURSOR_ITERSIZE = 1000


def execute(sql, **kwargs):
    """Execute SQL write operation with optional keyword arguments for formatting, returning a status string."""
    with _get_cursor() as cursor:
        if not cursor:
            return None
        return _execute(sql, operation='write', cursor=cursor, **kwargs)


def execute_ddl_script(sql):
    """Handle Redshift DDL scripts, which are exceptional in a number of ways.

    * CREATE EXTERNAL SCHEMA must be executed separately from any later references to that schema.
      The simplest way to enforce that requirement is to split the multi-statement SQL string by semicolon,
      and execute each statement in turn.
    * DROP EXTERNAL TABLE and CREATE EXTERNAL TABLE will fail with a 'cannot run inside a transaction block'
      message unless autocommit is enabled.

    WARNING: This will break horribly if a semicolon terminated statement is inside a block quote.
    """
    statements = sql.split(';')
    # Remove any trailing debris after the last SQL statement.
    del statements[-1]
    with _get_cursor() as cursor:
        if not cursor:
            app.logger.error('Failed to get cursor to execute DDL script; aborting.')
            return False
        for index, statement in enumerate(statements):
            app.logger.info(f'Executing DDL script {index + 1} of {len(statements)}')
            result = _execute(statement, operation='write', cursor=cursor)
            if not result:
                app.logger.error(f'Aborting DDL script. Error executing statement: {statement}')
                return False
    return True


def copy_tsv_from_s3(table, s3_key):
    # In a test environment, retrieve object contents from mock S3 and use Postgres COPY FROM STDIN.
    if app.config['NESSIE_ENV'] == 'test':
        try:
            buf = io.StringIO(s3.get_object_text(s3_key))
            with _get_cursor(operation='read') as cursor:
                cursor.copy_from(buf, table)
            return True
        except psycopg2.Error as e:
            error_str = str(e)
            if e.pgcode:
                error_str += f'{e.pgcode}: {e.pgerror}\n'
            app.logger.warning(error_str)
            return False
    # Real Redshift accepts an S3 URL with IAM role.
    else:
        iam_role = app.config['REDSHIFT_IAM_ROLE']
        s3_prefix = 's3://' + app.config['LOCH_S3_BUCKET'] + '/'
        return execute(f"COPY {table} FROM '{s3_prefix}{s3_key}' IAM_ROLE '{iam_role}' DELIMITER '\\t';")


def create_external_schema(external_schema, role):
    query = f"""
        CREATE EXTERNAL SCHEMA {external_schema}
        FROM data catalog
        DATABASE '{external_schema}'
        IAM_ROLE '{role}'
        CREATE EXTERNAL DATABASE IF NOT EXISTS;
        """

    if not execute(query):
        app.logger.error('Error on Redshift create schema')
        return False
    else:
        return True


def drop_external_schema(schema_name):
    app.logger.info(f'Dropping external schema {schema_name}')

    # If the external schema has previously been dropped without benefit of this function,
    # external tables may be in place but not visible from Redshift. Running an apparently
    # redundant 'CREATE EXTERNAL SCHEMA' will make the external tables visible again, and
    # thereby make it possible to explicity drop them.
    if not fetch(f'SELECT * FROM SVV_EXTERNAL_SCHEMAS WHERE schemaname=\'{schema_name}\''):
        external_db = fetch(f'SELECT * FROM SVV_EXTERNAL_DATABASES WHERE databasename=\'{schema_name}\'')
        if external_db:
            iam_role = app.config['REDSHIFT_IAM_ROLE']
            sql = f"""CREATE EXTERNAL SCHEMA {schema_name}
                FROM data catalog DATABASE \'{schema_name}\'
                IAM_ROLE \'{iam_role}\'
                CREATE EXTERNAL DATABASE IF NOT EXISTS
            """
            execute(sql)

    sql = f'SELECT * FROM SVV_EXTERNAL_TABLES WHERE schemaname=\'{schema_name}\''
    results = fetch(sql)
    if results:
        def _get_tablename(r):
            # We tolerate both dict and object when parsing result row
            return r['tablename'] if 'tablename' in r else r.tablename

        tables = [_get_tablename(r) for r in results]
        for table in tables:
            sql = f'DROP TABLE {schema_name}.{table} CASCADE'
            execute(sql)
    sql = f'DROP SCHEMA IF EXISTS {schema_name}'
    execute(sql)


def fetch(sql, **kwargs):
    """Execute SQL read operation with optional keyword arguments for formatting."""
    if kwargs.pop('stream_results', None):
        if app.config['NESSIE_ENV'] == 'test':
            cursor = _get_streaming_cursor()
            _execute_streaming(sql, cursor, **kwargs)
            return cursor
        else:
            # AWS recommends unloading large Redshift result sets to S3 and streaming from there.
            with _get_cursor() as cursor:
                if not cursor:
                    return None
                s3_prefix = _execute_unload(sql, cursor, **kwargs)
                if not s3_prefix:
                    return None
                return s3.get_tsv_stream(s3_prefix)
    else:
        # If streaming is not requested, return an array of dictionaries.
        with _get_cursor(operation='read') as cursor:
            if not cursor:
                return None
            rows = _execute(sql, 'read', cursor, **kwargs)
            if rows is None:
                return None
            else:
                return copy_for_pandas(rows)


def copy_for_pandas(rows):
    # For Pandas compatibility, copy psycopg's list-like object of dict-like objects to a real list of dicts. A handful of colimns need to be forced
    # to numeric values.
    def _transform_row(row):
        copied = row.copy()
        for key in {'current_score', 'last_activity_at', 'submissions_turned_in'}:
            if key in copied:
                try:
                    copied[key] = float(copied[key])
                except (TypeError, ValueError):
                    copied[key] = None
        for key in {'canvas_course_id', 'canvas_user_id'}:
            if key in copied:
                copied[key] = int(copied[key])
        return copied
    return [_transform_row(r) for r in rows]


class Transaction():
    def __init__(self, cursor):
        self.cursor = cursor
        self.execute('BEGIN TRANSACTION')

    def execute(self, sql, **kwargs):
        return _execute(sql, 'write', self.cursor, **kwargs)

    def commit(self):
        return self.execute('COMMIT TRANSACTION')

    def rollback(self):
        return self.execute('ROLLBACK TRANSACTION')


@contextmanager
def transaction():
    with _get_cursor(autocommit=False) as cursor:
        yield Transaction(cursor)


@contextmanager
def _get_cursor(autocommit=True, operation='write'):
    try:
        with get_psycopg_cursor(
            operation=operation,
            autocommit=autocommit,
            **_connection_args(),
        ) as cursor:
            yield cursor
    except psycopg2.Error as e:
        _handle_psycopg2_error(e)
        yield None


def _get_streaming_cursor():
    try:
        cursor = get_psycopg_cursor_streaming(**_connection_args())
        cursor.itersize = CURSOR_ITERSIZE
        return cursor
    except psycopg2.Error as e:
        _handle_psycopg2_error(e)
        return None


def _handle_psycopg2_error(e):
    error_str = str(e)
    if e.pgcode:
        error_str += f'{e.pgcode}: {e.pgerror}\n'
    app.logger.warning(error_str)


def _connection_args():
    return {
        'dbname': app.config.get('REDSHIFT_DATABASE'),
        'host': app.config.get('REDSHIFT_HOST'),
        'port': app.config.get('REDSHIFT_PORT'),
        'user': app.config.get('REDSHIFT_USER'),
        'password': app.config.get('REDSHIFT_PASSWORD'),
    }


def _execute(sql, operation, cursor, **kwargs):
    """Execute SQL string with optional keyword arguments for formatting.

    If 'operation' is set to 'write', a transaction is enforced and a status string is returned. If 'operation' is
    set to 'read', results are returned as an array of named tuples.
    """
    result = None
    silent = kwargs.pop('silent', False)
    try:
        params = None
        if kwargs:
            params = kwargs.pop('params', None)
            sql = psycopg2.sql.SQL(sql).format(**kwargs)
        # Don't log sensitive credentials in the SQL.
        sql_for_log = re.sub(r"CREDENTIALS '[^']+'", "CREDENTIALS '<credentials>'", str(sql))
        ts = datetime.now().timestamp()
        cursor.execute(sql, params)
        if operation == 'read':
            result = [row for row in cursor]
            query_time = datetime.now().timestamp() - ts
            if not silent:
                app.logger.debug(f'Redshift query returned {len(result)} rows in {query_time} seconds:\n{sql_for_log}\n{params or ""}')
        else:
            result = cursor.statusmessage
            query_time = datetime.now().timestamp() - ts
            if not silent:
                app.logger.debug(f'Redshift query returned status {result} in {query_time} seconds:\n{sql_for_log}\n{params or ""}')
    except psycopg2.Error as e:
        error_str = str(e)
        if e.pgcode:
            error_str += f'{e.pgcode}: {e.pgerror}\n'
        error_str += f'on SQL: {sql_for_log}'
        app.logger.warning(error_str)
    return result


def _execute_streaming(sql, cursor, **kwargs):
    params = None
    if kwargs:
        params = kwargs.pop('params', None)
        sql = psycopg2.sql.SQL(sql).format(**kwargs)
    # Don't log sensitive credentials in the SQL.
    sql_for_log = re.sub(r"CREDENTIALS '[^']+'", "CREDENTIALS '<credentials>'", str(sql))
    cursor.execute(sql, params)
    app.logger.debug(f'Redshift query (cursor {cursor.name} streaming results:\n{sql_for_log}\n{params or ""}')


def _execute_unload(sql, cursor, **kwargs):
    params = None
    unload_path = None

    if kwargs:
        params = kwargs.pop('params', None)
        unload_path = kwargs.pop('unload_path', None)
        sql = psycopg2.sql.SQL(sql).format(**kwargs).as_string(cursor.connection)

    destination_prefix = f'{get_s3_sis_daily_path()}/unloads/{unload_path}/'
    destination_path = f"s3://{app.config['LOCH_S3_BUCKET']}/{destination_prefix}"
    iam_role = app.config['REDSHIFT_IAM_ROLE']

    # Escape SQL for insertion into UNLOAD
    sql = sql.replace('\'', '\\\'')
    sql = f"""UNLOAD ('{sql}') TO '{destination_path}' IAM_ROLE '{iam_role}'
              DELIMITER AS '\\t' NULL AS ''
              ADDQUOTES ALLOWOVERWRITE ENCRYPTED ESCAPE GZIP HEADER PARALLEL OFF"""
    cursor.execute(sql, params)

    # Don't log sensitive credentials in the SQL.
    sql_for_log = re.sub(r"CREDENTIALS '[^']+'", "CREDENTIALS '<credentials>'", str(sql))
    app.logger.debug(f'Redshift unloaded query results to {unload_path}: {sql_for_log}')
    return destination_prefix
