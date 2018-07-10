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

import json
from flask import current_app as app
from nessie.externals import calnet
from nessie.externals import s3
from nessie.jobs.background_job import BackgroundJob, get_s3_calnet_daily_path
from nessie.lib.queries import get_all_student_ids


class ImportCalNetData(BackgroundJob):

    def run(self, csids=None):
        if not csids:
            csids = [row['sid'] for row in get_all_student_ids()]
        app.logger.info(f'Starting CalNet import job for {len(csids)} students...')
        _put_calnet_data_to_s3(csids)
        app.logger.info('CalNet import: done')
        return True


def _put_calnet_data_to_s3(sids):
    all_attributes = calnet.client(app).search_csids(sids)
    if len(sids) != len(all_attributes):
        ldap_sids = [l['csid'] for l in all_attributes]
        missing = set(sids) - set(ldap_sids)
        app.logger.warning(f'Looked for {len(sids)} SIDs but only found {len(all_attributes)} : missing {missing}')

    serialized_data = ''
    total_count = len(all_attributes)
    for index, a in enumerate(all_attributes):
        sid = a['csid']
        app.logger.info(f'CalNet import: Fetch attributes of student {sid} ({index + 1} of {total_count})')
        affiliations = a['affiliations']
        first_name, last_name = _split_sortable_name(a)
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
    s3.upload_data(serialized_data, f'{get_s3_calnet_daily_path()}/persons.json')


def _split_sortable_name(a):
    if 'sortable_name' not in a:
        name_split = []
    elif isinstance(a['sortable_name'], list):
        name_split = a['sortable_name'][0].split(',')
    else:
        name_split = a['sortable_name'].split(',')
    full_name = [name.strip() for name in reversed(name_split)]
    first_name = full_name[0] if len(full_name) else ''
    last_name = full_name[1] if len(full_name) > 1 else ''
    return first_name, last_name
