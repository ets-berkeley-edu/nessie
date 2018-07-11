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


from nessie import db


def select_column(sql):
    connection = db.engine.connect()
    result_proxy = connection.execute(sql)
    result = [row[0] for row in result_proxy]
    connection.close()
    return result


def select_row(sql):
    connection = db.engine.connect()
    result_proxy = connection.execute(sql)
    result = next(result_proxy)
    connection.close()
    return result


def select_scalar(sql):
    connection = db.engine.connect()
    result_proxy = connection.execute(sql)
    result = next(result_proxy)
    connection.close()
    return result[0]


def try_advisory_lock(lock_id):
    return select_row(f'SELECT pg_try_advisory_lock({lock_id}) as locked, pg_backend_pid() as pid')


def advisory_unlock(lock_id):
    initial_unlock = select_row(f'SELECT pg_advisory_unlock({lock_id}) as unlocked, pg_backend_pid() as pid')
    unlocked = initial_unlock.unlocked
    # Guard against the possibility of duplicate successful lock requests from this connection.
    while unlocked:
        unlocked = select_scalar(f'SELECT pg_advisory_unlock({lock_id})')
    return initial_unlock


def get_granted_lock_ids():
    return select_column("SELECT objid from pg_locks where locktype = 'advisory' and granted = true")
