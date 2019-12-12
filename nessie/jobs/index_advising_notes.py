"""
Copyright ©2020. The Regents of the University of California (Regents). All Rights Reserved.

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
from nessie.externals import calnet, rds, redshift
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.util import resolve_sql_template

"""Logic for advising note author names index job."""


class IndexAdvisingNotes(BackgroundJob):

    def run(self):
        app.logger.info(f'Starting advising note index job...')
        app.logger.info(f'Executing SQL...')
        self.create_advising_note_authors()
        self.import_note_authors()
        self.index_advising_notes()

        return 'Advising note index job completed.'

    def create_advising_note_authors(self):
        resolved_ddl = resolve_sql_template('create_advising_note_authors.template.sql')
        if rds.execute(resolved_ddl):
            app.logger.info('Created advising note authors.')
        else:
            raise BackgroundJobError('Failed to create advising note authors.')

    def import_note_authors(self):
        notes_schema = app.config['RDS_SCHEMA_ADVISING_NOTES']
        asc_schema = app.config['RDS_SCHEMA_ASC']
        e_i_schema = app.config['RDS_SCHEMA_E_I']
        sis_notes_schema = app.config['RDS_SCHEMA_SIS_ADVISING_NOTES']
        advisor_schema_redshift = app.config['REDSHIFT_SCHEMA_ADVISOR_INTERNAL']

        advisor_sids_from_sis_notes = set(
            [r['advisor_sid'] for r in rds.fetch(f'SELECT DISTINCT advisor_sid FROM {sis_notes_schema}.advising_notes')],
        )
        advisor_sids_from_advisors = set(
            [r['sid'] for r in redshift.fetch(f'SELECT DISTINCT sid FROM {advisor_schema_redshift}.advisor_departments')],
        )
        advisor_sids = list(advisor_sids_from_sis_notes | advisor_sids_from_advisors)

        advisor_uids_from_asc_notes = set(
            [r['advisor_uid'] for r in rds.fetch(f'SELECT DISTINCT advisor_uid FROM {asc_schema}.advising_notes')],
        )
        advisor_uids_from_e_i_notes = set(
            [r['advisor_uid'] for r in rds.fetch(f'SELECT DISTINCT advisor_uid FROM {e_i_schema}.advising_notes')],
        )
        advisor_uids = list(advisor_uids_from_asc_notes | advisor_uids_from_e_i_notes)

        advisor_attributes = calnet.client(app).search_csids(advisor_sids) + calnet.client(app).search_uids(advisor_uids)
        if not advisor_attributes:
            raise BackgroundJobError('Failed to fetch note author attributes.')

        unique_advisor_attributes = list({adv['uid']: adv for adv in advisor_attributes}.values())

        with rds.transaction() as transaction:
            insertable_rows = []
            for entry in unique_advisor_attributes:
                first_name, last_name = calnet.split_sortable_name(entry)
                insertable_rows.append(tuple((entry.get('uid'), entry.get('csid'), first_name, last_name)))

            result = transaction.insert_bulk(
                f'INSERT INTO {notes_schema}.advising_note_authors (uid, sid, first_name, last_name) VALUES %s',
                insertable_rows,
            )
            if result:
                transaction.commit()
                app.logger.info('Imported advising note author attributes.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Failed to import advising note author attributes.')

    def index_advising_notes(self):
        resolved_ddl = resolve_sql_template('index_advising_notes.template.sql')
        if rds.execute(resolved_ddl):
            app.logger.info('Indexed advising notes.')
        else:
            raise BackgroundJobError('Failed to index advising notes.')
