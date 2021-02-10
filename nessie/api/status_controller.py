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
from nessie.lib import berkeley
from nessie.lib.http import tolerant_jsonify


@app.route('/api/config')
def app_config():
    current_term_name = berkeley.current_term_name()
    current_term_id = berkeley.current_term_id()
    future_term_id = berkeley.future_term_id()
    return tolerant_jsonify({
        'currentEnrollmentTerm': current_term_name,
        'currentEnrollmentTermId': int(current_term_id),
        'ebEnvironment': app.config['EB_ENVIRONMENT'] if 'EB_ENVIRONMENT' in app.config else None,
        'featureFlagEnterpriseDataLake': app.config['FEATURE_FLAG_ENTERPRISE_DATA_LAKE'],
        'futureTermId': int(future_term_id),
        'nessieEnv': app.config['NESSIE_ENV'],
    })


@app.route('/api/ping')
def app_status():
    def db_status():
        try:
            db.session.execute('SELECT 1')
            return True
        except Exception:
            app.logger.exception('Failed to connect to RDS database')
            return False

    redshift_row = redshift.fetch('SELECT 1')
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
