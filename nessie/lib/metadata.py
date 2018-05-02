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


from flask import current_app as app
from nessie.externals import redshift, s3
import psycopg2.sql


def create_canvas_sync_status(job_id, filename, canvas_table, source_url):
    sql = """INSERT INTO {schema}.canvas_sync_job_status
               (job_id, filename, canvas_table, source_url, status, created_at, updated_at)
               VALUES (%s, %s, %s, %s, 'created', current_timestamp, current_timestamp)
               """
    return redshift.execute(
        sql,
        params=(job_id, filename, canvas_table, source_url),
        schema=_schema(),
    )


def update_canvas_sync_status(job_id, key, status, **kwargs):
    filename = key.split('/')[-1]
    destination_url = s3.build_s3_url(key, credentials=False)
    details = kwargs.get('details')
    sql = """UPDATE {schema}.canvas_sync_job_status
             SET destination_url=%s, status=%s, details=%s, updated_at=current_timestamp
             WHERE job_id=%s AND filename=%s"""
    return redshift.execute(
        sql,
        params=(destination_url, status, details, job_id, filename),
        schema=_schema(),
    )


def create_canvas_snapshot(key, size):
    canvas_table, filename = key.split('/')[-2:]
    url = s3.build_s3_url(key, credentials=False)
    sql = """INSERT INTO {schema}.canvas_synced_snapshots
             (filename, canvas_table, url, size, created_at)
             VALUES (%s, %s, %s, %s, current_timestamp)"""
    return redshift.execute(
        sql,
        params=(filename, canvas_table, url, size),
        schema=_schema(),
    )


def delete_canvas_snapshots(keys):
    filenames = [key.split('/')[-1] for key in keys]
    sql = 'UPDATE {schema}.canvas_synced_snapshots SET deleted_at=current_timestamp WHERE filename IN %s'
    return redshift.execute(sql, params=[tuple(filenames)], schema=_schema())


def _schema():
    return psycopg2.sql.Identifier(app.config['REDSHIFT_SCHEMA_METADATA'])
