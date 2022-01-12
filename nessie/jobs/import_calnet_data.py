"""
Copyright ©2021. The Regents of the University of California (Regents). All Rights Reserved.

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

import json

from flask import current_app as app
from nessie.externals import calnet
from nessie.externals import s3
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.queries import get_active_student_ids, get_all_instructor_uids
from nessie.lib.util import get_s3_calnet_daily_path


class ImportCalNetData(BackgroundJob):

    def run(self, advisee_csids=None, instructor_uids=None):
        if not advisee_csids:
            advisee_csids = [row['sid'] for row in get_active_student_ids()]
        if not instructor_uids:
            instructor_uids = [row['instructor_uid'] for row in get_all_instructor_uids()]
        _put_advisee_data_to_s3(advisee_csids)
        _put_instructor_data_to_s3(instructor_uids)
        return True


def _put_advisee_data_to_s3(sids):
    app.logger.info(f'Starting CalNet import job for {len(sids)} advisees...')
    all_attributes = calnet.client(app).search_csids(sids)
    if len(sids) != len(all_attributes):
        ldap_sids = [person['csid'] for person in all_attributes]
        missing = set(sids) - set(ldap_sids)
        app.logger.warning(f'Looked for {len(sids)} advisee SIDs but only found {len(all_attributes)} : missing {missing}')

    serialized_data = ''
    for index, a in enumerate(all_attributes):
        sid = a['csid']
        affiliations = a['affiliations']
        first_name, last_name = calnet.split_sortable_name(a)
        # JsonSerDe in Redshift schema creation requires one and only one JSON record per line in text file in S3.
        serialized_data += json.dumps({
            'affiliations': ','.join(affiliations) if isinstance(affiliations, list) else affiliations,
            'campus_email': a['campus_email'],
            'email': a['email'],
            'first_name': first_name,
            'last_name': last_name,
            'ldap_uid': a['uid'],
            'sid': sid,
        }) + '\n'
    s3.upload_data(serialized_data, f'{get_s3_calnet_daily_path()}/advisees/advisees.json')
    app.logger.info(f'Uploaded data for {len(all_attributes)} advisees')


def _put_instructor_data_to_s3(uids):
    app.logger.info(f'Starting CalNet import job for {len(uids)} instructors...')
    all_attributes = calnet.client(app).search_uids(uids)
    if len(uids) != len(all_attributes):
        ldap_uids = [person['uid'] for person in all_attributes]
        missing = set(uids) - set(ldap_uids)
        app.logger.warning(f'Looked for {len(uids)} instructor UIDs but only found {len(all_attributes)} : missing {missing}')

    serialized_data = ''
    for index, a in enumerate(all_attributes):
        uid = a['uid']
        affiliations = a['affiliations']
        first_name, last_name = calnet.split_sortable_name(a)
        serialized_data += json.dumps({
            'affiliations': ','.join(affiliations) if isinstance(affiliations, list) else affiliations,
            'campus_email': a['campus_email'],
            'dept_code': calnet.get_dept_code(a),
            'email': a['email'],
            'first_name': first_name,
            'last_name': last_name,
            'ldap_uid': uid,
            'csid': a['csid'],
            'title': a['title'],
        }) + '\n'
    s3.upload_data(serialized_data, f'{get_s3_calnet_daily_path()}/instructors/instructors.json')
    app.logger.info(f'Uploaded data for {len(all_attributes)} instructors')
