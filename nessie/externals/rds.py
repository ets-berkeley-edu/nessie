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


"""Client code to run queries against RDS."""

from contextlib import contextmanager
from datetime import datetime

from flask import current_app as app
from nessie.lib.db import get_psycopg_cursor
import psycopg2
import psycopg2.extras


@contextmanager
def get_cursor():
    with get_psycopg_cursor(operation='write', autocommit=False, uri=app.config.get('SQLALCHEMY_DATABASE_URI')) as cursor:
        yield cursor


def execute(cursor, sql, params=None):
    result = None
    try:
        ts = datetime.now().timestamp()
        cursor.execute(sql, params)
        result = cursor.statusmessage
        query_time = datetime.now().timestamp() - ts
        app.logger.debug(f'RDS query returned status {result} in {query_time} seconds:\n{sql}\n{params or ""}')
    except psycopg2.Error as e:
        _log_db_error(e, sql)
    return result


def insert_bulk(cursor, sql, rows):
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
    app.logger.warn({'message': error_str})
