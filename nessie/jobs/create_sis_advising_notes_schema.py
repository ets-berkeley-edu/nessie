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
from nessie.externals import rds, redshift
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError, verify_external_schema
from nessie.lib.util import legacy_note_datetime_to_utc, resolve_sql_template

"""Logic for SIS Advising Notes schema creation job."""


class CreateSisAdvisingNotesSchema(BackgroundJob):

    def run(self):
        app.logger.info(f'Starting SIS Advising Notes schema creation job...')
        app.logger.info(f'Executing SQL...')
        external_schema = app.config['REDSHIFT_SCHEMA_SIS_ADVISING_NOTES']
        redshift.drop_external_schema(external_schema)
        resolved_ddl = resolve_sql_template('create_sis_advising_notes_schema.template.sql')
        if redshift.execute_ddl_script(resolved_ddl):
            verify_external_schema(external_schema, resolved_ddl)
        else:
            raise BackgroundJobError(f'SIS Advising Notes schema creation job failed.')

        app.logger.info(f'Redshift schema created. Creating RDS indexes...')

        rds_index_ddl = resolve_sql_template('index_sis_advising_notes.template.sql')
        if rds.execute(rds_index_ddl):
            notes = rds.fetch(f'SELECT n.id, n.created_at, n.updated_at FROM {self._rds_schema_sis_advising_notes}.advising_notes n')
            self._legacy_notes_to_utc(notes)
            return 'SIS Advising Notes schema creation job completed.'
        else:
            raise BackgroundJobError(f'SIS Advising Notes schema creation job failed.')

    @classmethod
    def _legacy_notes_to_utc(cls, notes):
        if len(notes):
            with rds.transaction() as transaction:
                def _utc_str(dt):
                    utc_date = legacy_note_datetime_to_utc(dt)
                    return str(utc_date)

                for note in notes:
                    note_id = note.get('id')
                    created_at = note.get('created_at')
                    updated_at = note.get('updated_at')
                    sql = f"""UPDATE {cls._rds_schema_sis_advising_notes}.advising_notes
                              SET
                                  created_at = '{_utc_str(created_at)}',
                                  updated_at = '{_utc_str(updated_at)}'
                              WHERE id = '{note_id}'
                    """
                    transaction.execute(sql)
                    transaction.commit()

    @classmethod
    def _rds_schema_sis_advising_notes(cls):
        return app.config['RDS_SCHEMA_SIS_ADVISING_NOTES']
