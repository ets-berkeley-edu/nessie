"""
Copyright ©2024. The Regents of the University of California (Regents). All Rights Reserved.

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

from flask import current_app as app, request
from nessie.api.auth_helper import auth_required
from nessie.api.errors import BadRequestError
from nessie.externals import rds
from nessie.lib.http import tolerant_jsonify


@auth_required
@app.route('/api/8ball/schedules', methods=['GET'])
def magic_eight_ball_schedules():
    results = rds.fetch('SELECT * FROM magic_eight_ball.schedules ORDER BY release, name')
    return tolerant_jsonify(results)


@auth_required
@app.route('/api/8ball/schedules', methods=['POST'])
def create_eight_ball_schedules():
    try:
        props = request.get_json(force=True)
    except Exception as e:
        raise BadRequestError(str(e))
    params = {}
    if props:
        for p in ('name', 'design', 'development', 'qa', 'release'):
            if p not in props:
                raise BadRequestError(f'{p} required')
            params[p] = props[p]
        rds.execute(
            """INSERT INTO magic_eight_ball.schedules
               (name, design, development, qa, release)
               VALUES
               (%(name)s, %(design)s, %(development)s, %(qa)s, %(release)s)""",
            params=params,
        )
        results = {'created': True}
    return tolerant_jsonify(results)


@auth_required
@app.route('/api/8ball/schedules/<schedule_id>', methods=['POST', 'DELETE'])
def update_eight_ball_schedules(schedule_id):
    schedule_id = int(schedule_id)
    schedule_count = rds.fetch('SELECT COUNT(*) FROM magic_eight_ball.schedules WHERE id = %(id)s', params={'id': schedule_id})
    if not schedule_count:
        raise BadRequestError(f'No schedule found for id: {schedule_id}')
    if request.method == 'DELETE':
        app.logger.warn(f'About to delete schedule for id: {schedule_id}')
        deletion_result = rds.execute('DELETE FROM magic_eight_ball.schedules WHERE id = %(id)s', params={'id': schedule_id})
        results = {'deleted': deletion_result}
    else:
        try:
            props = request.get_json(force=True)
        except Exception as e:
            raise BadRequestError(str(e))
        params = {'id': schedule_id}
        if props:
            for p in ('name', 'design', 'development', 'qa', 'release'):
                if p not in props:
                    raise BadRequestError(f'{p} required')
                params[p] = props[p]
            rds.execute(
                """UPDATE magic_eight_ball.schedules SET
                   name = %(name)s, design = %(design)s, development = %(development)s, qa = %(qa)s, release = %(release)s
                   WHERE id = %(id)s""",
                params=params,
            )
            updated = rds.fetch('SELECT * FROM magic_eight_ball.schedules WHERE id = %(id)s', params={'id': schedule_id})
            results = updated[0]
    return tolerant_jsonify(results)
