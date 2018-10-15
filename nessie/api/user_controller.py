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


from urllib.parse import urlencode, urlparse

import cas
from flask import current_app as app, flash, redirect, request, url_for
from flask_login import current_user, login_required, login_user, logout_user
from nessie.lib.http import add_param_to_url, tolerant_jsonify


@app.route('/api/user/cas_login_url', methods=['GET'])
def cas_login_url():
    return tolerant_jsonify({
        'casLoginURL': _cas_client(request.referrer).get_login_url(),
    })


@app.route('/api/user/cas_logout_url', methods=['GET'])
@login_required
def logout():
    referrer = urlparse(request.referrer)
    base_url = f'{referrer.scheme}://{referrer.netloc}'
    logout_user()
    return tolerant_jsonify({
        'casLogoutURL': _cas_client().get_logout_url(base_url),
    })


@app.route('/api/user/profile')
def my_profile():
    me = {'uid': current_user.get_id()} if current_user.is_authenticated else None
    return tolerant_jsonify(me)


@app.route('/cas/callback', methods=['GET', 'POST'])
def cas_login():
    logger = app.logger
    ticket = request.args['ticket']
    redirect_url = request.args.get('url')
    uid, attributes, proxy_granting_ticket = _cas_client(redirect_url).verify_ticket(ticket)
    logger.info(f'Logged into CAS as user {uid}')
    user = app.login_manager.user_callback(uid)
    if user is None:
        logger.error(f'User with UID {uid} was not found.')
        param = ('casLoginError', f'Sorry, no user found with UID {uid}.')
        redirect_url = add_param_to_url('/', param)
    else:
        login_user(user)
        flash('Logged in successfully.')
        if not redirect_url:
            redirect_url = '/'
    return redirect(redirect_url)


def _cas_client(target_url=None):
    cas_server = app.config['CAS_SERVER']
    # One (possible) advantage this has over "request.base_url" is that it embeds the configured SERVER_NAME.
    service_url = url_for('.cas_login', _external=True)
    if target_url:
        service_url = service_url + '?' + urlencode({'url': target_url})
    return cas.CASClientV3(server_url=cas_server, service_url=service_url)
