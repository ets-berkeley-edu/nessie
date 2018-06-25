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
from nessie.models.student import Student


class ImportCalNetData(BackgroundJob):

    def run(self):
        students = Student.query.all()
        if len(students):
            app.logger.info(f'CalNet import: Fetch data of {len(students)} students')
            _put_calnet_data_to_s3([s.sid for s in students])
            app.logger.info('CalNet import: done')
            status = True
        else:
            app.logger.error('ASC import: API returned zero students')
            status = False
        return status


def _put_calnet_data_to_s3(sids):
    all_attributes = calnet.client(app).search_csids(sids)
    if len(sids) != len(all_attributes):
        ldap_sids = [l['csid'] for l in all_attributes]
        missing = set(sids) - set(ldap_sids)
        app.logger.warning(f'Looked for {len(sids)} SIDs but only found {len(all_attributes)} : missing {missing}')

    students = []
    for a in all_attributes:
        sortable_name = a['sortable_name'] if 'sortable_name' in a else ''
        name_split = sortable_name.split(',')
        full_name = [name.strip() for name in reversed(name_split)]
        students.append({
            'sid': a['csid'],
            'uid': a['uid'],
            'first_name': full_name[0] if len(full_name) else '',
            'last_name': full_name[1] if len(full_name) > 1 else '',
        })
    s3.upload_data(json.dumps(students), f'{get_s3_calnet_daily_path()}/calnet.json')
