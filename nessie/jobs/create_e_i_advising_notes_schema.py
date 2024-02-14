"""
Copyright ©2024. The Regents of the University of California (Regents). All Rights Reserved.

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
from nessie.externals import rds, redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError, verify_external_schema
from nessie.lib.util import resolve_sql_template

"""Logic for E&I Advising Notes schema creation job."""


class CreateEIAdvisingNotesSchema(BackgroundJob):

    def run(self):
        app.logger.info('Starting E&I Advising Notes schema creation job...')
        self.create_schema()
        app.logger.info('Redshift schema created. Creating RDS indexes...')
        self.create_indexes()
        return 'E&I Advising Notes schema creation job completed.'

    def create_schema(self):
        base_s3_key = app.config['LOCH_S3_E_I_DATA_PATH']
        external_schema = app.config['REDSHIFT_SCHEMA_E_I_ADVISING_NOTES']
        redshift.drop_external_schema(external_schema)
        # Flatten E&I-sourced JSON files into two schema-friendly JSON files.
        notes = []
        topics = []
        for key in s3.get_keys_with_prefix(base_s3_key):
            if key.endswith('.json') and 'aggregated_' not in key:
                notes_json = s3.get_object_json(key)
                if notes_json and 'notes' in notes_json:
                    notes += notes_json['notes']
                    for note in notes_json['notes']:
                        topics += _extract_topics(note)

        if s3.upload_json(obj=notes, s3_key=f'{base_s3_key}/aggregated_notes/data.json') \
                and s3.upload_json(obj=topics, s3_key=f'{base_s3_key}/aggregated_topics/data.json'):
            # Create schema
            app.logger.info('Executing SQL...')
            resolved_ddl = resolve_sql_template('create_e_i_advising_notes_schema.template.sql')
            if redshift.execute_ddl_script(resolved_ddl):
                verify_external_schema(external_schema, resolved_ddl)
            else:
                raise BackgroundJobError('E&I Advising Notes schema creation job failed.')
        else:
            raise BackgroundJobError('Failed to upload aggregated E&I advising notes and topics.')

    def create_indexes(self):
        resolved_ddl = resolve_sql_template('index_e_i_advising_notes.template.sql')
        if rds.execute(resolved_ddl):
            app.logger.info('Created E&I Advising Notes RDS indexes.')
        else:
            raise BackgroundJobError('E&I Advising Notes schema creation job failed to create indexes.')


def _extract_topics(note):
    topics = []
    note_id = note['id']
    sid = note['studentSid']
    for topic in (note['topics'] or []):
        topics.append({
            'id': f'{sid}-{note_id}',
            'e_i_id': note_id,
            'sid': sid,
            'topic': topic,
        })
    return topics
