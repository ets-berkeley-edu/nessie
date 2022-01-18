"""
Copyright ©2022. The Regents of the University of California (Regents). All Rights Reserved.

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
from random import randint

import dateutil.parser
from flask import current_app as app, request
from nessie.api.auth_helper import auth_required
from nessie.lib import http, metadata
from nessie.lib.http import tolerant_jsonify


@app.route('/api/admin/runnable_jobs')
@auth_required
def console_available_jobs():
    job_api_endpoints = []
    for rule in app.url_map.iter_rules():
        if isinstance(rule.rule, str) and rule.rule.startswith('/api/job/'):
            job_api_endpoints.append({
                'name': rule.endpoint.replace('_', ' ').capitalize(),
                'path': rule.rule,
                'required': list(rule.arguments),
                'methods': list(rule.methods),
            })
    job_api_endpoints.sort(key=lambda row: row.get('name'))
    return tolerant_jsonify(job_api_endpoints)


@app.route('/api/admin/background_job_status', methods=['POST'])
@auth_required
def background_job_status():
    iso_date = request.args.get('date')
    date = dateutil.parser.parse(iso_date) if iso_date else datetime.today()
    rows = metadata.background_job_status_by_date(created_date=date) or []
    rows.sort(key=lambda row: row.get('created_at'))

    def to_api_json(row):
        created_at = row['created_at']
        updated_at = row['updated_at']
        return {
            'id': row['job_id'],
            'status': row['status'],
            'instanceId': row['instance_id'],
            'details': row['details'],
            'started': created_at.isoformat(),
            'finished': None if updated_at == created_at else updated_at.isoformat(),
        }
    return tolerant_jsonify([to_api_json(row) for row in rows])


@app.route('/api/admin/xkcd')
@auth_required
def xkcd():
    try:
        url = http.build_url(f'https://xkcd.com/{randint(1, 2427)}/info.0.json')
        json = http.request(url).json()
    except Exception:
        json = {
            'alt': '40% of OpenBSD installs lead to shark attacks.',
            'img': 'https://imgs.xkcd.com/comics/success.png',
        }
    return tolerant_jsonify(json)
