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

from datetime import datetime

import dateutil.parser
from flask import current_app as app, request
from nessie.api.auth_helper import auth_required
from nessie.lib import metadata
from nessie.lib.http import tolerant_jsonify


@app.route('/api/metadata/background_job_status', methods=['POST'])
@auth_required
def background_job_status():
    iso_date = request.args.get('date')
    date = dateutil.parser.parse(iso_date) if iso_date else datetime.today()
    rows = metadata.background_job_status_by_date(created_date=date) or []
    rows.sort(key=lambda row: row.get('created_at'))

    def to_api_json(row):
        return {
            'id': row['job_id'],
            'status': row['status'],
            'instanceId': row['instance_id'],
            'details': row['details'],
            'started': row['created_at'].strftime('%c'),
            'finished': row['updated_at'].strftime('%c'),
        }
    return tolerant_jsonify([to_api_json(row) for row in rows])


@app.route('/api/metadata/failures_from_last_sync', methods=['POST'])
@auth_required
def failures_from_last_sync():
    result = metadata.get_failures_from_last_sync()

    def to_api_json(result):
        return {
            'jobId': result['job_id'],
            'failures': result['failures'],
        }
    return tolerant_jsonify(to_api_json(result))
