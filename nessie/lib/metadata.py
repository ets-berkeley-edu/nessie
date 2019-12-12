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

from datetime import datetime
import os

from flask import current_app as app
from nessie.externals import rds, s3


def create_canvas_sync_status(job_id, filename, canvas_table, source_url):
    sql = f"""INSERT INTO {_rds_schema()}.canvas_sync_job_status
               (job_id, filename, canvas_table, source_url, status, instance_id, created_at, updated_at)
               VALUES (%s, %s, %s, %s, 'created', %s, current_timestamp, current_timestamp)
               """
    return rds.execute(
        sql,
        params=(job_id, filename, canvas_table, source_url, _instance_id()),
    )


def get_failures_from_last_sync():
    last_job_id = None
    failures = []

    job_id_result = rds.fetch(
        f"""SELECT MAX(job_id) AS last_job_id FROM {_rds_schema()}.canvas_sync_job_status WHERE job_id LIKE %s""",
        params=['sync%%'],
    )
    if not job_id_result:
        app.logger.error('Failed to retrieve id for last sync job')
    else:
        last_job_id = job_id_result[0]['last_job_id']
        failures_query = f"""SELECT * FROM {_rds_schema()}.canvas_sync_job_status WHERE job_id = %s
            AND (status NOT IN ('complete', 'duplicate') OR destination_size != source_size)"""
        failures = rds.fetch(failures_query, params=[last_job_id])
    return {'job_id': last_job_id, 'failures': failures}


def update_canvas_sync_status(job_id, key, status, **kwargs):
    filename = key.split('/')[-1]
    destination_url = s3.build_s3_url(key)

    sql = f"""UPDATE {_rds_schema()}.canvas_sync_job_status
             SET destination_url=%s, status=%s, updated_at=current_timestamp"""
    params = [destination_url, status]
    for key in ['details', 'source_size', 'destination_size']:
        if kwargs.get(key):
            sql += f', {key}=%s'
            params.append(kwargs[key])
    sql += ' WHERE job_id=%s AND filename=%s'
    params += [job_id, filename]

    return rds.execute(
        sql,
        params=tuple(params),
    )


def create_canvas_snapshot(key, size):
    canvas_table, filename = key.split('/')[-2:]
    url = s3.build_s3_url(key)
    sql = f"""INSERT INTO {_rds_schema()}.canvas_synced_snapshots
             (filename, canvas_table, url, size, created_at)
             VALUES (%s, %s, %s, %s, current_timestamp)"""
    return rds.execute(
        sql,
        params=(filename, canvas_table, url, size),
    )


def delete_canvas_snapshots(keys):
    filenames = [key.split('/')[-1] for key in keys]
    sql = f'UPDATE {_rds_schema()}.canvas_synced_snapshots SET deleted_at=current_timestamp WHERE filename IN %s'
    return rds.execute(sql, params=[tuple(filenames)])


def background_job_status_by_date(created_date):
    sql = f'SELECT * FROM {_rds_schema()}.background_job_status WHERE cast(created_at as date) = %s'
    return rds.fetch(
        sql,
        params=[created_date.strftime('%Y-%m-%d')],
    )


def most_recent_background_job_status(job_id_prefix, status=None):
    sql = f'SELECT * FROM {_rds_schema()}.background_job_status WHERE job_id LIKE %s'
    params = [f'{job_id_prefix}%']
    if status:
        sql += f' AND status = %s'
        params += [status]
    sql += ' ORDER BY updated_at DESC LIMIT 1'
    result = rds.fetch(
        sql,
        params,
    )
    if result and result[0]:
        return result[0]


def create_background_job_status(job_id):
    sql = f"""INSERT INTO {_rds_schema()}.background_job_status
               (job_id, status, instance_id, created_at, updated_at)
               VALUES (%s, 'started', %s, current_timestamp, current_timestamp)
               """
    return rds.execute(
        sql,
        params=(job_id, _instance_id()),
    )


def update_background_job_status(job_id, status, details=None):
    if details:
        details = details[:4096]
    sql = f"""UPDATE {_rds_schema()}.background_job_status
             SET status=%s, updated_at=current_timestamp, details=%s
             WHERE job_id=%s"""
    return rds.execute(
        sql,
        params=(status, details, job_id),
    )


def update_registration_import_status(successes, failures):
    rds.execute(
        f'DELETE FROM {_rds_schema()}.registration_import_status WHERE sid = ANY(%s)',
        params=(successes + failures, ),
    )
    now = datetime.utcnow().isoformat()
    success_records = [tuple([sid, 'success', now]) for sid in successes]
    failure_records = [tuple([sid, 'failure', now]) for sid in failures]
    rows = success_records + failure_records
    with rds.transaction() as transaction:
        result = transaction.insert_bulk(
            f"""INSERT INTO {_rds_schema()}.registration_import_status
                (sid, status, updated_at)
                VALUES %s
            """,
            rows,
        )
        if result:
            transaction.commit()
        else:
            transaction.rollback()
            app.logger.error('Error saving merged feed status updates to RDS.')


def update_photo_import_status(successes, failures, photo_not_found):
    rds.execute(
        f'DELETE FROM {_rds_schema()}.photo_import_status WHERE sid = ANY(%s)',
        params=(successes + failures + photo_not_found, ),
    )
    now = datetime.utcnow().isoformat()
    success_records = [tuple([sid, 'success', now]) for sid in successes]
    failure_records = [tuple([sid, 'failure', now]) for sid in failures]
    photo_not_found_records = [tuple([sid, 'photo_not_found', now]) for sid in photo_not_found]
    rows = success_records + failure_records + photo_not_found_records
    with rds.transaction() as transaction:
        result = transaction.insert_bulk(
            f"""INSERT INTO {_rds_schema()}.photo_import_status
                (sid, status, updated_at)
                VALUES %s
            """,
            rows,
        )
        if result:
            transaction.commit()
        else:
            transaction.rollback()
            app.logger.error('Error saving photo import status updates to RDS.')


def queue_merged_enrollment_term_jobs(master_job_id, term_ids):
    now = datetime.now().replace(microsecond=0).isoformat()

    def insertable_tuple(term_id):
        return tuple([
            master_job_id,
            term_id,
            'created',
            None,
            now,
            now,
        ])
    with rds.transaction() as transaction:
        insert_result = transaction.insert_bulk(
            f"""INSERT INTO {_rds_schema()}.merged_enrollment_term_job_queue
               (master_job_id, term_id, status, instance_id, created_at, updated_at)
                VALUES %s""",
            [insertable_tuple(term_id) for term_id in term_ids],
        )
        if insert_result:
            transaction.commit()
            return True
        else:
            transaction.rollback()
            return False


def poll_merged_enrollment_term_job_queue():
    result = rds.fetch(
        f"""UPDATE {_rds_schema()}.merged_enrollment_term_job_queue
        SET status='started', instance_id=%s
        WHERE id = (
            SELECT id
            FROM {_rds_schema()}.merged_enrollment_term_job_queue
            WHERE status = 'created'
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, master_job_id, term_id
        """,
        params=(_instance_id(),),
        log_query=False,
    )
    if result and result[0]:
        return result[0]


def get_merged_enrollment_term_job_status(master_job_id):
    return rds.fetch(
        f"""SELECT *
        FROM {_rds_schema()}.merged_enrollment_term_job_queue
        WHERE master_job_id=%s
        ORDER BY term_id
        """,
        params=(master_job_id,),
        log_query=False,
    )


def update_merged_enrollment_term_job_status(job_id, status, details):
    if details:
        details = details[:4096]
    sql = f"""UPDATE {_rds_schema()}.merged_enrollment_term_job_queue
             SET status=%s, updated_at=current_timestamp, details=%s
             WHERE id=%s"""
    return rds.execute(
        sql,
        params=(status, details, job_id),
    )


def _instance_id():
    return os.environ.get('EC2_INSTANCE_ID')


def _rds_schema():
    return app.config['RDS_SCHEMA_METADATA']
