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


"""Client code to run queries against Redshift."""


from contextlib import contextmanager
from flask import current_app as app
import psycopg2
import psycopg2.extras
import psycopg2.sql


def execute(sql, **kwargs):
    """Execute SQL write operation with optional keyword arguments for formatting, returning a status string."""
    return _execute(sql, 'write', **kwargs)


def fetch(sql, **kwargs):
    """Execute SQL read operation with optional keyword arguments for formatting, returning an array of named tuples."""
    return _execute(sql, 'read', **kwargs)


def _execute(sql, operation, **kwargs):
    """Execute SQL string with optional keyword arguments for formatting.

    If 'operation' is set to 'write', a transaction is enforced and a status string is returned. If 'operation' is
    set to 'read', results are returned as an array of named tuples.
    """
    result = None
    try:
        sql = psycopg2.sql.SQL(sql).format(**kwargs)
        with get_cursor(operation) as cursor:
            cursor.execute(sql)
            if operation == 'read':
                result = [row for row in cursor]
            else:
                result = cursor.statusmessage
    except psycopg2.Error as e:
        error_str = str(e)
        if e.pgcode:
            error_str += f'{e.pgcode}: {e.pgerror}\n'
        error_str += f'on SQL: {sql}'
        app.logger.debug({'message': error_str})
    return result


@contextmanager
def get_cursor(operation='read'):
    connection = None
    cursor = None
    if operation == 'write':
        cursor_factory = None
    else:
        cursor_factory = psycopg2.extras.NamedTupleCursor
    try:
        connection = psycopg2.connect(
            dbname=app.config.get('REDSHIFT_DATABASE'),
            host=app.config.get('REDSHIFT_HOST'),
            port=app.config.get('REDSHIFT_PORT'),
            user=app.config.get('REDSHIFT_USER'),
            password=app.config.get('REDSHIFT_PASSWORD'),
        )
        yield connection.cursor(cursor_factory=cursor_factory)
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            if operation == 'write':
                connection.commit()
            connection.close()
