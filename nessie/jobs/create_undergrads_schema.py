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
from nessie.externals import rds, redshift
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError, verify_external_schema
from nessie.lib.util import resolve_sql_template
import psycopg2


"""Logic for Active Undergraduates schema creation job."""


external_schema = app.config['REDSHIFT_SCHEMA_UNDERGRADS_EXTERNAL']
internal_schema = app.config['REDSHIFT_SCHEMA_UNDERGRADS']
internal_schema_identifier = psycopg2.sql.Identifier(internal_schema)
rds_schema = app.config['RDS_SCHEMA_UNDERGRADS']


class CreateUndergradsSchema(BackgroundJob):

    def run(self):
        app.logger.info(f'Starting Undergrads schema creation job...')
        redshift.drop_external_schema(external_schema)
        resolved_ddl = resolve_sql_template('create_undergrads_schema.template.sql')
        if redshift.execute_ddl_script(resolved_ddl):
            app.logger.info(f'Undergrads external schema created.')
            verify_external_schema(external_schema, resolved_ddl)
        else:
            raise BackgroundJobError(f'Undergrads external schema creation failed.')
        undergrads_rows = redshift.fetch(f'SELECT * FROM {external_schema}.students ORDER by sid')

        with rds.transaction() as transaction:
            if self.refresh_rds_indexes(undergrads_rows, transaction):
                transaction.commit()
                app.logger.info('Refreshed RDS indexes.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Error refreshing RDS indexes.')

        return 'Undergrads internal schema created.'

    def refresh_rds_indexes(self, undergrads_rows, transaction):
        if len(undergrads_rows):
            result = transaction.execute(f'TRUNCATE {rds_schema}.students')
            if not result:
                return False
            columns = [
                'sid', 'acadprog_code', 'acadprog_descr',
                'acadplan_code', 'acadplan_descr',
                'acadplan_type_code', 'acadplan_ownedby_code',
            ]
            result = transaction.insert_bulk(
                f'INSERT INTO {rds_schema}.students ({", ".join(columns)}) VALUES %s',
                [tuple([r[c] for c in columns]) for r in undergrads_rows],
            )
            if not result:
                return False
        return True
