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

import os
import tempfile
from flask import current_app as app
from nessie import db, std_commit
from nessie.externals import s3
from nessie.externals.asc_athletes_api import get_asc_feed
from nessie.jobs.background_job import BackgroundJob, get_s3_asc_daily_path
from nessie.lib import asc
from nessie.lib.db import get_psycopg_cursor
from nessie.models.json_cache import JsonCache
from nessie.models.student import Student
import psycopg2
import psycopg2.sql


class ImportAscAthletes(BackgroundJob):

    def run(self):
        app.logger.info(f'ASC import: Fetch team and student athlete data from ASC API')
        api_results = get_asc_feed()
        if 'error' in api_results:
            app.logger.error('ASC import: Error from external API: {}'.format(api_results['error']))
            status = False
        elif not api_results:
            app.logger.error('ASC import: API returned zero students')
            status = False
        else:
            last_sync_date = _get_last_sync_date()
            sync_date = api_results[0]['SyncDate']
            if sync_date != api_results[-1]['SyncDate']:
                app.logger.error(f'ASC import: SyncDate conflict in ASC API: {api_results[0]} vs. {api_results[-1]}')
                status = False
            elif last_sync_date == sync_date:
                app.logger.warning(f"""
                    ASC import: Current SyncDate {sync_date} matches last SyncDate {last_sync_date}.
                    Existing cache will not be overwritten
                """)
                status = True
            else:
                _stash_feed(api_results)
                status = {
                    'last_sync_date': last_sync_date,
                    'this_sync_date': sync_date,
                    'warnings': [],
                    'change_counts': {
                        'new_students': 0,
                        'deleted_students': 0,
                        'activated_students': 0,
                        'inactivated_students': 0,
                        'changed_students': 0,
                        'new_memberships': 0,
                        'deleted_memberships': 0,
                        'new_team_groups': 0,
                        'changed_team_groups': 0,
                    },
                    'api_results_count': len(api_results),
                }
                if last_sync_date == sync_date:
                    last_feed = asc.get_cached_feed(sync_date)
                    app.logger.warning(f'ASC import: Current and previous feeds have the same sync date: {sync_date}')
                    old_only, new_only = _compare_rows(last_feed, api_results)
                    app.logger.warning(f"""
                        ASC import: Previous feed differences: {old_only}; current feed differences: {new_only}.
                        Overwriting previous feed in cache.""")
                    _stash_feed(api_results)
                safety = _safety_check_asc_api(api_results)
                if not safety['safe']:
                    error = safety['message']
                    app.logger.error(f'ASC import error: {error}')
                    status = False
                else:
                    change_counts = asc.merge_student_athletes(api_results)
                    app.logger.info(f'ASC import: change_counts={change_counts}')
                    status['change_counts'].update(change_counts)
                    asc.confirm_sync(sync_date)
                    _export_rds_to_s3()
                    app.logger.info(f'ASC import: Successfully completed import job: {str(status)}')
        return status


def _export_rds_to_s3():
    schema = {
        'athletics': 'group_code, group_name, team_code, team_name',
        'students': 'sid, in_intensive_cohort, is_active_asc, status_asc',
        'student_athletes': 'group_code, sid',
    }
    tmp_dir = f'{tempfile.mkdtemp()}'
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir, 0o777)

    def results_file_path(_table_name):
        return f'{tmp_dir}/{_table_name}.json'
    # Dump contents of nessie db (RDS) to JSONs in tmp dir
    for table_name, columns in schema.items():
        sql = f'COPY (SELECT ROW_TO_JSON(row) FROM (SELECT {columns} FROM {table_name}) row) TO STDOUT'
        with open(results_file_path(table_name), 'w+') as query_results_file:
            db_uri = app.config.get('SQLALCHEMY_DATABASE_URI')
            with get_psycopg_cursor(operation='write', dsn=db_uri) as cursor:
                cursor.copy_expert(sql=psycopg2.sql.SQL(sql), file=query_results_file)
    # Upload newly created JSON to S3
    s3_path = get_s3_asc_daily_path()
    for table_name, columns in schema.items():
        file_path = results_file_path(table_name)
        s3.upload_data(open(file_path, 'r').read(), f'{s3_path}/{table_name}/{table_name}.json')


def _safety_check_asc_api(feed):
    # Does the new feed have less than half the number of active athletes?
    nbr_current_active = Student.query.filter(Student.is_active_asc.is_(True)).count()
    nbr_active_in_feed = len(set(r['SID'] for r in feed if r['ActiveYN'] == 'Yes'))
    if nbr_current_active > 4 and (nbr_current_active / 2.0 > nbr_active_in_feed):
        remove_count = nbr_current_active - nbr_active_in_feed
        return {
            'safe': False,
            'message': f'Import feed would remove {remove_count} out of {nbr_current_active} active athletes',
        }
    else:
        return {'safe': True}


def _compare_rows(old_rows, new_rows):
    old_rows = _sorted_imports(old_rows)
    new_rows = _sorted_imports(new_rows)
    old_only = {r['SID'] + '/' + r['SportCode']: dict(r) for r in old_rows if r not in new_rows}
    new_only = {r['SID'] + '/' + r['SportCode']: dict(r) for r in new_rows if r not in old_rows}
    for k in (old_only.keys() & new_only.keys()):
        app.logger.warning(f'Changed old: {old_only[k]}\n  to new: {new_only[k]}')
    for k in (old_only.keys() - new_only.keys()):
        app.logger.warning(f'Removed: {old_only[k]}')
    for k in (new_only.keys() - old_only.keys()):
        app.logger.warning(f'Added: {new_only[k]}')
    return old_only, new_only


def _sorted_imports(import_rows):
    used_fields = [
        'SID',
        'IntensiveYN',
        'SportCode',
        'SportCodeCore',
        'Sport',
        'SportCore',
        'ActiveYN',
        'SportStatus',
    ]
    rows = [{k: v for k, v in r.items() if k in used_fields} for r in import_rows]
    rows = sorted(rows, key=lambda row: (row['SID'], row['SportCode']))
    return rows


def _stash_feed(rows):
    sync_date = rows[0]['SyncDate']
    key = asc.feed_key(sync_date)
    if JsonCache.query.filter_by(key=key).first():
        db.session.query(JsonCache).filter(JsonCache.key == key).delete()
    db.session.add(JsonCache(key=key, json=rows))
    std_commit()


def _get_last_sync_date():
    item = JsonCache.query.filter_by(key=asc.LAST_SYNC_DATE_KEY).first()
    return item and item.json
