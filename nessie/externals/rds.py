"""
Copyright ©2025. The Regents of the University of California (Regents). All Rights Reserved.

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

from flask import current_app as app
from nessie.lib.db import get_psycopg_cursor, get_psycopg_cursor_streaming
import psycopg2
import psycopg2.extras

"""Client code to run queries against RDS."""

# Batch size to use when streaming large result sets.
CURSOR_ITERSIZE = 1000


def execute(sql, params=None, log_query=True, rds_uri=None):
    with _get_cursor(rds_uri=rds_uri) as cursor:
        if not cursor:
            return None
        else:
            return _execute(sql, cursor, params, 'write', log_query)


def fetch(sql, params=None, log_query=True, rds_uri=None, stream=False):
    if stream:
        cursor = _get_streaming_cursor()
        _execute_streaming(sql, cursor, params)
        return cursor
    else:
        with _get_cursor(operation='read', rds_uri=rds_uri) as cursor:
            if not cursor:
                return None
            else:
                return _execute(sql, cursor, params, 'read', log_query)


class Transaction():
    def __init__(self, cursor):
        self.cursor = cursor
        self.execute('BEGIN TRANSACTION')

    def execute(self, sql, params=None, log_query=True):
        return _execute(sql, self.cursor, params, 'write', log_query)

    def insert_bulk(self, sql, rows):
        return _insert_bulk(sql, self.cursor, rows)

    def commit(self):
        return self.execute('COMMIT TRANSACTION')

    def rollback(self):
        return self.execute('ROLLBACK TRANSACTION')


@contextmanager
def transaction(rds_uri=None):
    with _get_cursor(autocommit=False, rds_uri=rds_uri) as cursor:
        yield Transaction(cursor)


@contextmanager
def _get_cursor(autocommit=True, operation='write', rds_uri=None):
    uri = rds_uri or app.config.get('SQLALCHEMY_DATABASE_URI')
    with get_psycopg_cursor(
        operation=operation,
        autocommit=autocommit,
        uri=uri,
    ) as cursor:
        yield cursor


def _get_streaming_cursor(rds_uri=None):
    uri = rds_uri or app.config.get('SQLALCHEMY_DATABASE_URI')
    try:
        cursor = get_psycopg_cursor_streaming(uri=uri)
        cursor.itersize = CURSOR_ITERSIZE
        return cursor
    except psycopg2.Error as e:
        _handle_psycopg2_error(e)
        return None


def _execute(sql, cursor, params=None, operation='write', log_query=True):
    result = None
    try:
        ts = datetime.now().timestamp()
        cursor.execute(sql, params)
        result = cursor.statusmessage
        query_time = datetime.now().timestamp() - ts
        if log_query:
            app.logger.debug(f'RDS query returned status {result} in {query_time} seconds: \n{sql}\n{params or ""}')
    except psycopg2.Error as e:
        _log_db_error(e, sql)
    if operation == 'read':
        rows = cursor.fetchall()
        return [dict(r) for r in rows]
    else:
        return result


def _execute_streaming(sql, cursor, params):
    cursor.execute(sql, params)
    if not params:
        params_to_log = ''
    else:
        params_to_log = str(params)
        if len(params_to_log) > 1000:
            params_to_log = params_to_log[0:1000] + '...'
    app.logger.debug(f'Redshift query (cursor {cursor.name} streaming results:\n{sql}\n{params_to_log}')


def _handle_psycopg2_error(e):
    error_str = str(e)
    if e.pgcode:
        error_str += f'{e.pgcode}: {e.pgerror}\n'
    app.logger.warning(error_str)


def _insert_bulk(sql, cursor, rows):
    result = None
    try:
        psycopg2.extras.execute_values(cursor, sql, rows, page_size=5000)
        result = cursor.statusmessage
    except psycopg2.Error as e:
        _log_db_error(e, sql)
    return result


def _log_db_error(e, sql):
    error_str = str(e)
    if e.pgcode:
        error_str += f'{e.pgcode}: {e.pgerror}\n'
    error_str += f'on SQL: {sql}'
    app.logger.warning(error_str)
