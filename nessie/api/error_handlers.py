"""
Copyright ©2025. The Regents of the University of California (Regents). All Rights Reserved.

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
from werkzeug.exceptions import HTTPException

import nessie.api.errors
from nessie.lib.http import tolerant_jsonify


@app.errorhandler(nessie.api.errors.BadRequestError)
def handle_bad_request(error):
    return error.to_json(), error.status_code


@app.errorhandler(nessie.api.errors.UnauthorizedRequestError)
def handle_unauthorized(error):
    return error.to_json(), error.status_code


@app.errorhandler(nessie.api.errors.ResourceNotFoundError)
def handle_resource_not_found(error):
    return error.to_json(), error.status_code


@app.errorhandler(nessie.api.errors.InternalServerError)
def handle_internal_server_error(error):
    return error.to_json(), error.status_code


@app.errorhandler(HTTPException)
def handle_http_exception(error):
    # Werkzeug/Flask exceptions (405 Method Not Allowed, 413 Payload Too Large, etc.) keep their own status.
    if error.code is None or error.code < 400:
        # Redirects, for example, are passed through untouched.
        return error
    if error.code >= 500:
        app.logger.exception(error)
    headers = {k: v for k, v in error.get_headers() if k.lower() != 'content-type'}
    return tolerant_jsonify({'message': error.description}), error.code, headers


@app.errorhandler(Exception)
def handle_unexpected_error(error):
    app.logger.exception(error)
    return tolerant_jsonify({'message': 'An unexpected server error occurred.'}), 500
