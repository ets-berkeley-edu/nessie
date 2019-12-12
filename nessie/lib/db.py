"""
Copyright ©2020. The Regents of the University of California (Regents). All Rights Reserved.

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

import psycopg2
import psycopg2.extras
import psycopg2.sql


@contextmanager
def get_psycopg_cursor(operation='read', autocommit=True, **kwargs):
    connection = None
    cursor = None
    if operation == 'write':
        cursor_factory = None
    else:
        cursor_factory = psycopg2.extras.DictCursor
    try:
        if kwargs.get('uri'):
            connection = psycopg2.connect(kwargs['uri'])
        else:
            connection = psycopg2.connect(**kwargs)
        # Autocommit is required for EXTERNAL TABLE creation and deletion.
        if autocommit:
            connection.autocommit = True
        yield connection.cursor(cursor_factory=cursor_factory)
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
