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

from nessie.externals import calnet, rds
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.jobs.index_advising_notes import import_note_authors, index_advising_notes
from nessie.lib.queries import get_advisor_sids
from nessie.lib.util import resolve_sql_template

"""Logic for BOA App RDS Data Advising Notes author names index job."""


class IndexDailyAdvisingNotes(BackgroundJob):

    def run(self):
        app.logger.info('Starting Daily Advising Notes author names index job...')
        app.logger.info('Executing SQL...')
        self.import_note_authors()
        self.index_advising_notes()

        return 'Daily Advising Notes author names index job completed.'

    def import_note_authors(self):
        notes_schema = app.config['RDS_SCHEMA_ADVISING_NOTES']

        advisor_attributes = self._advisor_attributes_by_uid()
        if not advisor_attributes:
            raise BackgroundJobError('Failed to fetch note author attributes.')

        unique_advisor_attributes = list({adv['uid']: adv for adv in advisor_attributes}.values())

        with rds.transaction() as transaction:
            insertable_rows = []
            for entry in unique_advisor_attributes:
                first_name, last_name = calnet.split_sortable_name(entry)
                insertable_rows.append(tuple((entry.get('uid'), entry.get('csid'), first_name, last_name, entry.get('campus_email'))))  # noqa: C409

            result = transaction.insert_bulk(
                f'INSERT INTO {notes_schema}.advising_note_authors (uid, sid, first_name, last_name, campus_email) VALUES %s',
                insertable_rows,
            )
            if result:
                transaction.commit()
                app.logger.info('Imported advising note author attributes.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Failed to import advising note author attributes.')

    def index_advising_notes(self):
        resolved_ddl = resolve_sql_template('index_daily_advising_notes.template.sql')
        if rds.execute(resolved_ddl):
            app.logger.info('Indexed advising notes.')
        else:
            raise BackgroundJobError('Failed to index advising notes.')


    def _advisor_attributes_by_uid(self):
        bard_schema = app.config['RDS_SCHEMA_BARD']
        advisor_uids_from_bard_notes = set(
            [r['advisor_uid'] for r in rds.fetch(f'SELECT DISTINCT advisor_uid FROM {bard_schema}.advising_notes')],
        )
        advisor_uids = list(advisor_uids_from_bard_notes)
        return calnet.client(app).search_uids(advisor_uids)
