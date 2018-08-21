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


from nessie.externals import sis_terms_api
from nessie.lib.mockingbird import MockResponse, register_mock


class TestSisTermsApi:
    """SIS terms API query."""

    def test_get_term(self, app):
        """Returns unwrapped data."""
        term = sis_terms_api.get_term('2172')
        assert len(term) == 4
        assert term[0]['academicCareer']['code'] == 'LAW'
        assert term[1]['academicCareer']['code'] == 'GRAD'
        assert term[2]['academicCareer']['code'] == 'UCBX'
        assert term[3]['academicCareer']['code'] == 'UGRD'
        assert term[3]['beginDate'] == '2017-01-10'
        assert term[3]['endDate'] == '2017-05-12'

    def test_server_error(self, app, caplog):
        """Logs unexpected server errors and returns None."""
        api_error = MockResponse(500, {}, '{"message": "Internal server error."}')
        with register_mock(sis_terms_api._get_term, api_error):
            term = sis_terms_api.get_term('2172')
            assert 'HTTP/1.1" 500' in caplog.text
            assert term is None
