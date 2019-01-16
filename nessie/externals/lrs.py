"""
Copyright ©2019. The Regents of the University of California (Regents). All Rights Reserved.

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
from nessie.lib.db import get_psycopg_cursor
import psycopg2
import psycopg2.extras

"""Client code to run queries against LRS."""


def execute(sql):
    """Execute SQL write operation."""
    with _get_cursor('write') as cursor:
        if not cursor:
            return None
        return _execute(sql, 'write', cursor)


def fetch(sql):
    """Execute SQL read operation."""
    with _get_cursor('read') as cursor:
        if not cursor:
            return None
        return _execute(sql, 'read', cursor)


@contextmanager
def _get_cursor(operation):
    try:
        with get_psycopg_cursor(
            operation=operation,
            autocommit=True,
            uri=app.config.get('LRS_DATABASE_URI'),
        ) as cursor:
            yield cursor
    except psycopg2.Error as e:
        error_str = str(e)
        if e.pgcode:
            error_str += f'{e.pgcode}: {e.pgerror}\n'
        app.logger.warning({'message': error_str})
        yield None


def _execute(sql, operation, cursor):
    """Execute SQL string with optional keyword arguments for formatting.

    If 'operation' is set to 'write', a transaction is enforced and a status string is returned. If 'operation' is
    set to 'read', results are returned as an array of named tuples.
    """
    result = None
    try:
        ts = datetime.now().timestamp()
        cursor.execute(sql)
        if operation == 'read':
            result = [row for row in cursor]
            query_time = datetime.now().timestamp() - ts
            app.logger.debug(f'LRS query returned {len(result)} rows in {query_time} seconds:\n{sql}')
        else:
            result = cursor.statusmessage
            query_time = datetime.now().timestamp() - ts
            app.logger.debug(f'LRS query returned status {result} in {query_time} seconds:\n{sql}')
    except psycopg2.Error as e:
        error_str = str(e)
        if e.pgcode:
            error_str += f'{e.pgcode}: {e.pgerror}\n'
        error_str += f'on SQL: {sql}'
        app.logger.warning({'message': error_str})
    return result
