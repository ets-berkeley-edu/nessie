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

from flask import jsonify, make_response, request
from flask_login import LoginManager
from nessie.models.user import find_by_uid


def register_routes(app):
    """Register app routes."""
    # Register authentication before routes are registered.
    login_manager = LoginManager()

    login_manager.init_app(app)
    login_manager.user_loader(find_by_uid)

    # Register API routes.
    import nessie.api.admin_controller
    import nessie.api.analyze_edl_controller
    import nessie.api.user_controller
    import nessie.api.job_controller
    import nessie.api.schedule_controller
    import nessie.api.status_controller

    # Register error handlers.
    import nessie.api.error_handlers

    @app.login_manager.unauthorized_handler
    def unauthorized_handler():
        return jsonify(success=False, data={'login_required': True}, message='Unauthorized'), 401

    # Unmatched API routes return a 404.
    @app.route('/api/<path:path>')
    def handle_unmatched_api_route(**kwargs):
        app.logger.error('The requested resource could not be found.')
        raise nessie.api.errors.ResourceNotFoundError('The requested resource could not be found.')

    # Non-API routes are handled by the front end.
    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def front_end_route(**kwargs):
        return make_response(open(app.config['INDEX_HTML']).read())

    @app.after_request
    def after_api_request(response):
        if app.config['NESSIE_ENV'] == 'development':
            # In development the response can be shared with requesting code from any local origin.
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
            response.headers['Access-Control-Allow-Origin'] = app.config['VUE_LOCALHOST_BASE_URL']
            response.headers['Access-Control-Allow-Credentials'] = 'true'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, DELETE'
        if request.full_path.startswith('/api'):
            log_message = ' '.join([
                request.remote_addr,
                request.method,
                request.full_path,
                response.status,
            ])
            if response.status_code >= 500:
                app.logger.error(log_message)
            elif response.status_code >= 400:
                app.logger.warning(log_message)
            elif not request.full_path.startswith('/api/ping'):
                # Do not litter the logs with 'pings'
                app.logger.debug(log_message)
        return response
