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


class TestCasAuth:
    """CAS login URL generation and redirects."""

    def test_cas_login_url(self, client):
        """Returns berkeley.edu URL of CAS login page."""
        response = client.get('/api/user/cas_login_url')
        assert response.status_code == 200
        assert 'berkeley.edu/cas/login' in response.json.get('casLoginURL')

    def test_cas_callback_with_invalid_ticket(self, client):
        """Fails if CAS can not verify the ticket."""
        response = client.get('/cas/callback?ticket=is_invalid')
        assert response.status_code == 302
        assert 'casLoginError' in response.location


class TestUserProfile:
    """User Profile API."""

    def test_not_authenticated(self, client):
        """Returns empty profile when user is not logged in."""
        response = client.get('/api/user/profile')
        assert response.status_code == 200
        assert not response.json
