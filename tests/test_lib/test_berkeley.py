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

from nessie.externals import rds
from nessie.lib import berkeley
from nessie.lib.util import resolve_sql_template
import pytest


@pytest.fixture
def current_term_index(app):
    current_term_name = app.config['CURRENT_TERM']
    future_term_name = app.config['FUTURE_TERM']
    s3_canvas_data_path_current_term = app.config['LOCH_S3_CANVAS_DATA_PATH_CURRENT_TERM']
    app.config['CURRENT_TERM'] = 'auto'
    app.config['FUTURE_TERM'] = 'auto'
    app.config['LOCH_S3_CANVAS_DATA_PATH_CURRENT_TERM'] = 'auto'
    berkeley.cache_thread.config_terms = None
    rds_schema = app.config['RDS_SCHEMA_SIS_TERMS']
    rds.execute(f'DROP SCHEMA {rds_schema} CASCADE')
    rds.execute(resolve_sql_template('create_rds_indexes.template.sql'))
    rds.execute(f"""INSERT INTO {rds_schema}.current_term_index
        (current_term_name, future_term_name)
        VALUES ('Spring 2018', 'Fall 2018')
    """)
    yield
    app.config['CURRENT_TERM'] = current_term_name
    app.config['FUTURE_TERM'] = future_term_name
    app.config['LOCH_S3_CANVAS_DATA_PATH_CURRENT_TERM'] = s3_canvas_data_path_current_term


class TestBerkeleySisTermIdForName:
    """Term name to SIS id translation."""

    def test_sis_term_id_for_name(self):
        """Handles well-formed term names."""
        assert berkeley.sis_term_id_for_name('Spring 2015') == '2152'
        assert berkeley.sis_term_id_for_name('Summer 2016') == '2165'
        assert berkeley.sis_term_id_for_name('Fall 2017') == '2178'
        assert berkeley.sis_term_id_for_name('Fall 1997') == '1978'
        assert berkeley.sis_term_id_for_name('Winter 1976') == '1760'

    def test_term_name_for_sis_id(self):
        assert berkeley.term_name_for_sis_id('2178') == 'Fall 2017'
        assert berkeley.term_name_for_sis_id('1978') == 'Fall 1997'
        assert berkeley.term_name_for_sis_id('1760') == 'Winter 1976'

    def test_unparseable_term_name(self):
        """Returns None for unparseable term names."""
        assert berkeley.sis_term_id_for_name('Autumn 2061') is None
        assert berkeley.sis_term_id_for_name('Default Term') is None

    def test_missing_term_name(self):
        """Returns None for missing term names."""
        assert berkeley.sis_term_id_for_name(None) is None


class TestBerkeleyDegreeProgramUrl:

    def test_major_with_known_link(self):
        assert berkeley.degree_program_url_for_major('English BA') == \
            'http://guide.berkeley.edu/undergraduate/degree-programs/english/'
        assert berkeley.degree_program_url_for_major('Peace & Conflict Studies BA') == \
            'http://guide.berkeley.edu/undergraduate/degree-programs/peace-conflict-studies/'
        assert berkeley.degree_program_url_for_major('History BA') == \
            'http://guide.berkeley.edu/undergraduate/degree-programs/history/'
        assert berkeley.degree_program_url_for_major('History of Art BA') == \
            'http://guide.berkeley.edu/undergraduate/degree-programs/art-history/'

    def test_major_without_a_link(self):
        assert berkeley.degree_program_url_for_major('English for Billiards Players MS') is None
        assert berkeley.degree_program_url_for_major('Altaic Language BA') is None
        assert berkeley.degree_program_url_for_major('Entomology BS') is None


class TestBerkeley:

    def test_term_id_lists(self, app):
        all_term_ids = set(berkeley.reverse_term_ids(include_future_terms=True, include_legacy_terms=True))
        canvas_integrated_term_ids = set(berkeley.reverse_term_ids())
        future_term_ids = set(berkeley.future_term_ids())
        legacy_term_ids = set(berkeley.legacy_term_ids())
        assert canvas_integrated_term_ids < all_term_ids
        assert berkeley.sis_term_id_for_name(app.config['EARLIEST_LEGACY_TERM']) in all_term_ids
        assert berkeley.sis_term_id_for_name(app.config['EARLIEST_TERM']) in all_term_ids
        assert berkeley.sis_term_id_for_name(app.config['CURRENT_TERM']) in all_term_ids
        assert berkeley.sis_term_id_for_name(app.config['FUTURE_TERM']) in all_term_ids

        assert berkeley.current_term_id() in canvas_integrated_term_ids
        assert berkeley.earliest_term_id() in canvas_integrated_term_ids

        assert future_term_ids.isdisjoint(canvas_integrated_term_ids)
        assert future_term_ids < all_term_ids
        assert berkeley.future_term_id() in future_term_ids

        assert legacy_term_ids.isdisjoint(canvas_integrated_term_ids)
        assert legacy_term_ids < all_term_ids
        assert berkeley.earliest_legacy_term_id() in berkeley.legacy_term_ids()

    def test_auto_terms(self, app, current_term_index):
        all_term_ids = set(berkeley.reverse_term_ids(include_future_terms=True, include_legacy_terms=True))
        canvas_integrated_term_ids = set(berkeley.reverse_term_ids())
        assert canvas_integrated_term_ids < all_term_ids
        assert berkeley.current_term_id() == '2182'
        assert berkeley.future_term_id() == '2188'
        assert berkeley.s3_canvas_data_path_current_term() == 'canvas-data/term/spring-2018'
