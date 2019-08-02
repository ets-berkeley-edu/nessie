"""
Copyright ©2019. The Regents of the University of California (Regents). All Rights Reserved.

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
from nessie.externals import rds
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError

"""Logic for advising note author names index job."""


class IndexAdvisingNoteAuthors(BackgroundJob):

    def run(self):
        app.logger.info(f'Starting Advising Note author names index job...')
        app.logger.info(f'Executing SQL...')
        self.index_author_names()

        return 'Advising Note author names index job completed.'

    def index_author_names(self):
        asc_schema = app.config['RDS_SCHEMA_ASC']
        e_i_schema = app.config['RDS_SCHEMA_E_I']
        sis_schema = app.config['RDS_SCHEMA_SIS_ADVISING_NOTES']

        with rds.transaction() as transaction:
            if not transaction.execute(f'TRUNCATE {sis_schema}.advising_note_author_names'):
                transaction.rollback()
                raise BackgroundJobError('Failed to truncate advising note author name index.')

            sql = f"""INSERT INTO {sis_schema}.advising_note_author_names (
                SELECT DISTINCT uid, unnest(string_to_array(
                    regexp_replace(upper(first_name), '[^\w ]', '', 'g'),
                    ' '
                )) AS name FROM {sis_schema}.advising_note_authors
                UNION
                SELECT DISTINCT uid, unnest(string_to_array(
                    regexp_replace(upper(last_name), '[^\w ]', '', 'g'),
                    ' '
                )) AS name FROM {sis_schema}.advising_note_authors
                UNION
                SELECT DISTINCT advisor_uid, unnest(string_to_array(
                    regexp_replace(upper(advisor_first_name), '[^\w ]', '', 'g'),
                    ' '
                )) AS name FROM {asc_schema}.advising_notes WHERE advisor_uid IS NOT NULL
                UNION
                SELECT DISTINCT advisor_uid, unnest(string_to_array(
                    regexp_replace(upper(advisor_last_name), '[^\w ]', '', 'g'),
                    ' '
                )) AS name FROM {asc_schema}.advising_notes WHERE advisor_uid IS NOT NULL
                UNION
                SELECT DISTINCT advisor_uid, unnest(string_to_array(
                    regexp_replace(upper(advisor_first_name), '[^\w ]', '', 'g'),
                    ' '
                )) AS name FROM {e_i_schema}.advising_notes WHERE advisor_uid IS NOT NULL
                UNION
                SELECT DISTINCT advisor_uid, unnest(string_to_array(
                    regexp_replace(upper(advisor_last_name), '[^\w ]', '', 'g'),
                    ' '
                )) AS name FROM {e_i_schema}.advising_notes WHERE advisor_uid IS NOT NULL
                );"""
            if transaction.execute(sql):
                transaction.commit()
                app.logger.info('Indexed advising note author names.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Failed to index advising note author names.')
