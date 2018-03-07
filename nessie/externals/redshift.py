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


def execute(sql, cursor_factory=None):
    """Execute SQL, returning results if any in raw psycopg2 format (an array of unnamed tuples)."""
    result = None
    try:
        with get_cursor(cursor_factory) as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
    except psycopg2.Error as e:
        error_str = str(e)
        if e.pgcode:
            error_str += f'{e.pgcode}: {e.pgerror}\n'
        error_str += f'on SQL: {sql}'
        app.logger.debug({'message': error_str})
    return result


def fetch_tuples(sql):
    """Return results, if any, as named tuples."""
    return execute(sql, psycopg2.extras.NamedTupleCursor)


@contextmanager
def get_cursor(cursor_factory=None):
    connection = None
    cursor = None
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
            connection.close()
