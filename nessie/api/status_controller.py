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
from nessie import __version__ as version
from nessie import db
from nessie.externals import redshift
from nessie.lib.berkeley import current_term_id, current_term_name, future_term_id
from nessie.lib.http import tolerant_jsonify

PUBLIC_CONFIGS = [
    'CURRENT_TERM',
    'EARLIEST_LEGACY_TERM',
    'EARLIEST_TERM',
    'EB_ENVIRONMENT',
    'EMAIL_SYSTEM_ERRORS_TO',
    'FEATURE_FLAG_ENTERPRISE_DATA_LAKE',
    'FUTURE_TERM',
    'JOB_SCHEDULING_ENABLED',
    'NESSIE_ENV',
    'WORKER_QUEUE_ENABLED',
    'WORKER_THREADS',
]


@app.route('/api/config')
def app_config():
    def _to_api_key(key):
        chunks = key.split('_')
        return f"{chunks[0].lower()}{''.join(chunk.title() for chunk in chunks[1:])}"

    return tolerant_jsonify(
        {
            **dict((_to_api_key(key), app.config[key] if key in app.config else None) for key in PUBLIC_CONFIGS),
            **{
                'currentEnrollmentTerm': current_term_name(),
                'currentEnrollmentTermId': int(current_term_id()),
                'futureTermId': int(future_term_id()),
            },
        },
    )


@app.route('/api/ping')
def app_status():
    def db_status():
        try:
            db.session.execute('SELECT 1')
            return True
        except Exception:
            app.logger.exception('Failed to connect to RDS database')
            return False

    redshift_row = redshift.fetch('SELECT 1', silent=True)
    resp = {
        'app': True,
        'rds': db_status(),
        'redshift': redshift_row is not None,
    }
    return tolerant_jsonify(resp)


@app.route('/api/version')
def app_version():
    v = {
        'version': version,
    }
    build_stats = load_json('config/build-summary.json')
    if build_stats:
        v.update(build_stats)
    else:
        v.update({
            'build': None,
        })
    return tolerant_jsonify(v)


def load_json(relative_path):
    try:
        file = open(app.config['BASE_DIR'] + '/' + relative_path)
        return json.load(file)
    except (FileNotFoundError, KeyError, TypeError):
        return None
