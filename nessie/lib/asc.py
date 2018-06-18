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
from nessie import db, std_commit
from nessie.models.athletics import Athletics
from nessie.models.json_cache import JsonCache
from nessie.models.student import Student


"""A utility module collecting logic specific to the Athletic Study Center."""


LAST_SYNC_DATE_KEY = 'asc_athletes_last_sync'


SPORT_TRANSLATIONS = {
    'MBB': 'BAM',
    'MBK': 'BBM',
    'WBK': 'BBW',
    'MCR': 'CRM',
    'WCR': 'CRW',
    'MFB': 'FBM',
    'WFH': 'FHW',
    'MGO': 'GOM',
    'WGO': 'GOW',
    'MGY': 'GYM',
    'WGY': 'GYW',
    'WLC': 'LCW',
    'MRU': 'RGM',
    'WSF': 'SBW',
    'MSC': 'SCM',
    'WSC': 'SCW',
    'MSW': 'SDM',
    'WSW': 'SDW',
    # 'Beach Volleyball' vs. 'Sand Volleyball'.
    'WBV': 'SVW',
    'MTE': 'TNM',
    'WTE': 'TNW',
    # ASC's subsets of Track do not directly match the Athlete API's subsets. In ASC's initial data transfer,
    # all track athletes were mapped to 'TO*', 'Outdoor Track & Field'.
    'MTR': 'TOM',
    'WTR': 'TOW',
    'WVB': 'VBW',
    'MWP': 'WPM',
    'WWP': 'WPW',
}


# There are multiple groups within these teams, and the remainder group (for team members who don't fit any
# of the defined squads or specialties) is misleadingly named as if it identifies the entire team.
AMBIGUOUS_GROUP_CODES = [
    'MFB',
    'MSW',
    'MTR',
    'WSW',
    'WTR',
]


def merge_student_athletes(asc_feed, delete_students=True):
    status = {}
    # We want to remove any existing students who are absent from the new TSV.
    # We also want to remove any existing team memberships which have gone away.
    # So long as all data fits in memory, the simplest approach is to collect the current states and compare.
    (imported_team_groups, imported_students) = _parse_all_rows(asc_feed)
    status.update(_merge_athletics_import(imported_team_groups))
    status.update(_merge_students_import(imported_students, delete_students))
    return status


def confirm_sync(sync_date):
    if JsonCache.query.filter_by(key=LAST_SYNC_DATE_KEY).first():
        db.session.query(JsonCache).filter(JsonCache.key == LAST_SYNC_DATE_KEY).delete()
    db.session.add(JsonCache(key=LAST_SYNC_DATE_KEY, json=sync_date))
    std_commit()


def get_cached_feed(date):
    item = JsonCache.query.filter_by(key=feed_key(date)).first()
    return item and item.json


def feed_key(sync_date):
    return f'asc_athletes_{sync_date}'


def _parse_all_rows(rows):
    imported_team_groups = {}
    imported_students = {}
    for r in rows:
        if r['AcadYr'] == app.config['ASC_THIS_ACAD_YR'] and r['SportCode']:
            in_intensive_cohort = r.get('IntensiveYN', 'No') == 'Yes'
            is_active_asc = r.get('ActiveYN', 'No') == 'Yes'
            status_asc = r.get('SportStatus', '')
            asc_code = r['SportCodeCore']
            if asc_code in SPORT_TRANSLATIONS:
                sid = r['SID']
                group_code = r['SportCode']
                if sid in imported_students:
                    student = imported_students[sid]
                    if student['in_intensive_cohort'] is not in_intensive_cohort:
                        app.logger.error(f'Unexpected conflict in import rows for SID {sid}')
                    # Any active team membership means the student is an active athlete,
                    # even if they happen not to be an active member of a different team.
                    # Until BOAC-460 is resolved, the app will discard inactive memberships of an
                    # otherwise active student.
                    if not student['is_active_asc'] and is_active_asc:
                        athletics = student['athletics']
                        app.logger.warning(f'Will discard inactive memberships {athletics} for active SID {sid}')
                        student['athletics'] = []
                        student['is_active_asc'] = is_active_asc
                        student['status_asc'] = status_asc
                    elif student['is_active_asc'] and not is_active_asc:
                        app.logger.warning(f'Will discard inactive memberships {group_code} for SID {sid}')
                        continue
                else:
                    student = {
                        'sid': sid,
                        'in_intensive_cohort': in_intensive_cohort,
                        'is_active_asc': is_active_asc,
                        'status_asc': status_asc,
                        'athletics': [],
                    }
                    imported_students[sid] = student
                student['athletics'].append(group_code)
                if group_code not in imported_team_groups:
                    imported_team_groups[group_code] = {
                        'group_code': group_code,
                        'group_name': _unambiguous_group_name(r['Sport'], group_code),
                        'team_code': SPORT_TRANSLATIONS[asc_code],
                        'team_name': r['SportCore'],
                    }
            else:
                sid = r['SID']
                app.logger.error(f'Unmapped asc_code {asc_code} has ActiveYN for sid {sid}')
    return imported_team_groups, imported_students


def _merge_athletics_import(imported_team_groups):
    status = {
        'new_team_groups': 0,
        'changed_team_groups': 0,
    }
    for team_group in imported_team_groups.values():
        group_code = team_group['group_code']
        existing_group = Athletics.query.filter(Athletics.group_code == group_code).first()
        if not existing_group:
            app.logger.info(f'Adding new Athletics row: {team_group}')
            status['new_team_groups'] += 1
            athletics_row = Athletics(
                group_code=group_code,
                group_name=team_group['group_name'],
                team_code=team_group['team_code'],
                team_name=team_group['team_name'],
            )
            db.session.add(athletics_row)
            std_commit()
        elif (existing_group.group_name != team_group['group_name']) or (
                existing_group.team_code != team_group['team_code']) or (
                existing_group.team_name != team_group['team_name']):
            app.logger.warning(f'Modifying Athletics row: {team_group} from {existing_group}')
            status['changed_team_groups'] += 1
            existing_group.group_name = team_group['group_name']
            existing_group.team_code = team_group['team_code']
            existing_group.team_name = team_group['team_name']
            db.session.merge(existing_group)
            std_commit()
    return status


def _merge_students_import(imported_students, delete_students):
    status = {
        'new_students': 0,
        'deleted_students': 0,
        'activated_students': 0,
        'inactivated_students': 0,
        'changed_students': 0,
        'new_memberships': 0,
        'deleted_memberships': 0,
    }
    existing_students = {row['sid']: row for row in Student.get_all('sid')}
    remaining_sids = set(existing_students.keys())
    for student_import in imported_students.values():
        sid = student_import['sid']
        in_intensive_cohort = student_import['in_intensive_cohort']
        is_active_asc = student_import['is_active_asc']
        status_asc = student_import['status_asc']
        team_group_codes = set(student_import['athletics'])
        student_data = existing_students.get(sid, None)
        if not student_data:
            app.logger.info(f'Adding new Student row: {student_import}')
            status['new_students'] += 1
            student_row = Student(
                sid=sid,
                first_name='',
                last_name='',
                in_intensive_cohort=in_intensive_cohort,
                is_active_asc=is_active_asc,
                status_asc=status_asc,
            )
            db.session.add(student_row)
            std_commit()
            _merge_memberships(sid, set([]), team_group_codes, status)
        else:
            remaining_sids.remove(sid)
            if (
                    student_data['inIntensiveCohort'] is not in_intensive_cohort
            ) or (
                    student_data['isActiveAsc'] is not is_active_asc

            ) or (
                    student_data['statusAsc'] != status_asc
            ):
                app.logger.warning(f'Modifying Student row to {student_import} from {student_data}')
                if student_data['isActiveAsc'] is not is_active_asc:
                    if is_active_asc:
                        status['activated_students'] += 1
                    else:
                        status['inactivated_students'] += 1
                else:
                    status['changed_students'] += 1
                student_row = Student.find_by_sid(sid)
                student_row.in_intensive_cohort = in_intensive_cohort
                student_row.is_active_asc = is_active_asc
                student_row.status_asc = status_asc
                db.session.merge(student_row)
                std_commit()
            existing_group_codes = {mem['groupCode'] for mem in student_data.get('athletics', [])}
            if team_group_codes != existing_group_codes:
                _merge_memberships(sid, existing_group_codes, team_group_codes, status)
    if delete_students:
        for sid in remaining_sids:
            app.logger.warning(f'Deleting Student SID {sid}')
            status['deleted_students'] += 1
            Student.delete_student(sid)
    else:
        app.logger.warning(f'Will not delete unspecified Students: {remaining_sids}')
    return status


def _merge_memberships(sid, old_group_codes, new_group_codes, status):
    student_row = Student.find_by_sid(sid)
    for removed_group_code in (old_group_codes - new_group_codes):
        team_group = Athletics.query.filter(Athletics.group_code == removed_group_code).first()
        app.logger.warning(f'Removing SID {sid} from team group {team_group}')
        status['deleted_memberships'] += 1
        team_group.athletes.remove(student_row)
    for added_group_code in (new_group_codes - old_group_codes):
        team_group = Athletics.query.filter(Athletics.group_code == added_group_code).first()
        app.logger.info(f'Adding SID {sid} to team group {team_group}')
        status['new_memberships'] += 1
        team_group.athletes.append(student_row)
    std_commit()
    return status


def _unambiguous_group_name(asc_group_name, group_code):
    if group_code in AMBIGUOUS_GROUP_CODES:
        return f'{asc_group_name} - Other'
    else:
        return asc_group_name
