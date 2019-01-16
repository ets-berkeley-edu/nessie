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

from flask import current_app as app
from nessie import db


def select_column(sql):
    connection = db.engine.connect()
    result_proxy = connection.execute(sql)
    result = [row[0] for row in result_proxy]
    connection.close()
    return result


@contextmanager
def advisory_lock(lock_id):
    if not lock_id:
        yield
        return
    with db.engine.connect() as lock_connection:
        # Detaching will protect the server-session PID from action in the yield block.
        # Because detached connections are permanently removed from the pool, it also
        # ensures that the lock will be released when the connection is closed.
        lock_connection.detach()
        locked = try_advisory_lock(lock_connection, lock_id)
        if locked:
            try:
                yield
            except Exception as e:
                app.logger.exception(e)
                raise e
            finally:
                # Explicit unlocking should not be necessary, but the logging might be useful.
                advisory_unlock(lock_connection, lock_id)


def try_advisory_lock(connection, lock_id):
    result = connection.execute(f'SELECT pg_try_advisory_lock({lock_id}) as locked, pg_backend_pid() as pid')
    (locked, pid) = next(result)
    if locked:
        app.logger.info(f'Granted advisory lock {lock_id} for PID {pid}')
    else:
        app.logger.warn(f'Was not granted advisory lock {lock_id} for PID {pid}')
    return locked


def advisory_unlock(connection, lock_id):
    result = connection.execute(f'SELECT pg_advisory_unlock({lock_id}) as unlocked, pg_backend_pid() as pid')
    (unlocked, pid) = next(result)
    if unlocked:
        app.logger.info(f'Released advisory lock {lock_id} for PID {pid}')
    else:
        app.logger.error(f'Failed to release advisory lock {lock_id} for PID {pid}')
    # Guard against the possibility of duplicate successful lock requests from this connection.
    while unlocked:
        result = connection.execute(f'SELECT pg_advisory_unlock({lock_id}) as unlocked')
        unlocked = next(result).unlocked


def get_granted_lock_ids():
    return select_column("SELECT objid from pg_locks where locktype = 'advisory' and granted = true")
