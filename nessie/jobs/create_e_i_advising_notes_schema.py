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

import json

from flask import current_app as app
from nessie.externals import rds, redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError, verify_external_schema
from nessie.lib.util import resolve_sql_template

"""Logic for E&I Advising Notes schema creation job."""


class CreateEIAdvisingNotesSchema(BackgroundJob):

    def run(self):
        app.logger.info('Starting E&I Advising Notes schema creation job...')
        app.logger.info('Executing SQL...')
        self.create_schema()
        app.logger.info('Redshift schema created. Creating RDS indexes...')
        self.create_indexes()

        return 'E&I Advising Notes schema creation job completed.'

    def create_schema(self):
        external_schema = app.config['REDSHIFT_SCHEMA_E_I_ADVISING_NOTES']
        redshift.drop_external_schema(external_schema)
        # Merge all JSON files (sourced from E&I) into a single, schema-friendly JSON file.
        merged_json_filename = '_e_i_advising_notes.json'
        merged_json_s3_key = f"s3://{app.config['LOCH_S3_E_I_NOTES_PATH']}/{merged_json_filename}"

        # The file uploaded to S3 will have one note (JSON object) per line.
        data = ''
        for key in s3.get_keys_with_prefix(app.config['LOCH_S3_E_I_NOTES_PATH']):
            if key.endswith('.json') and key != merged_json_filename:
                notes_json = s3.get_object_json(key)
                if notes_json and 'notes' in notes_json:
                    notes = [json.dumps(note) for note in notes_json['notes']]
                    data += '\n'.join(notes)
        s3.upload_data(data=data, s3_key=merged_json_s3_key)

        # Create schema
        resolved_ddl = resolve_sql_template(
            'create_e_i_advising_notes_schema.template.sql',
            e_i_advising_notes_path=merged_json_s3_key,
        )
        # Clean up
        s3.delete_objects([merged_json_s3_key])

        if redshift.execute_ddl_script(resolved_ddl):
            verify_external_schema(external_schema, resolved_ddl)
        else:
            raise BackgroundJobError('E&I Advising Notes schema creation job failed.')

    def create_indexes(self):
        resolved_ddl = resolve_sql_template('index_e_i_advising_notes.template.sql')
        if rds.execute(resolved_ddl):
            app.logger.info('Created E&I Advising Notes RDS indexes.')
        else:
            raise BackgroundJobError('E&I Advising Notes schema creation job failed to create indexes.')
