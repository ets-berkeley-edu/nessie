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

from datetime import datetime, timedelta

from flask import current_app as app
from nessie.externals import rds, redshift
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError, verify_external_schema
from nessie.lib.berkeley import next_term_id, term_name_for_sis_id
from nessie.lib.util import resolve_sql_template
import pytz

"""Logic for SIS Terms schema creation job."""

external_schema = app.config['REDSHIFT_SCHEMA_SIS_TERMS']
rds_schema = app.config['RDS_SCHEMA_SIS_TERMS']


class CreateSisTermsSchema(BackgroundJob):
    def run(self):
        app.logger.info(f'Starting SIS terms schema creation job...')
        self.create_schema()
        self.refresh_sis_term_definitions()
        self.refresh_current_term_index()
        return 'SIS terms schema creation job completed.'

    def create_schema(self):
        app.logger.info(f'Executing SQL...')
        redshift.drop_external_schema(external_schema)
        resolved_ddl = resolve_sql_template('create_sis_terms_schema.template.sql')
        if redshift.execute_ddl_script(resolved_ddl):
            verify_external_schema(external_schema, resolved_ddl)
        else:
            raise BackgroundJobError(f'SIS terms schema creation job failed.')

    def refresh_sis_term_definitions(self):
        rows = redshift.fetch(f'SELECT * FROM {external_schema}.term_definitions')
        if len(rows):
            with rds.transaction() as transaction:
                if self.refresh_rds(rows, transaction):
                    transaction.commit()
                    app.logger.info('Refreshed RDS indexes.')
                else:
                    transaction.rollback()
                    raise BackgroundJobError('Error refreshing RDS term definitions.')

    def refresh_rds(self, rows, transaction):
        result = transaction.execute(f'TRUNCATE {rds_schema}.term_definitions')
        if not result:
            return False
        columns = ['term_id', 'term_name', 'term_begins', 'term_ends']
        result = transaction.insert_bulk(
            f'INSERT INTO {rds_schema}.term_definitions ({", ".join(columns)}) VALUES %s',
            [tuple([r[c] for c in columns]) for r in rows],
        )
        if not result:
            return False
        return True

    def refresh_current_term_index(self):
        today = datetime.now(pytz.utc).astimezone(pytz.timezone(app.config['TIMEZONE'])).date()
        current_term = self.get_sis_current_term(today)

        if current_term:
            current_term_id = current_term['term_id']

            # If today is one month or less before the end of the current term, or if the current term is summer,
            # include the next term.
            if current_term_id[3] == '5' or (current_term['term_ends'] - timedelta(weeks=4)) < today:
                future_term_id = next_term_id(current_term['term_id'])
                # ... and if the upcoming term is Summer, include the next Fall term as well.
                if future_term_id[3] == '5':
                    future_term_id = next_term_id(future_term_id)
            else:
                future_term_id = current_term_id

            with rds.transaction() as transaction:
                transaction.execute(f'TRUNCATE {rds_schema}.current_term_index')
                columns = ['current_term_name', 'future_term_name']
                values = tuple([current_term['term_name'], term_name_for_sis_id(future_term_id)])
                if transaction.execute(f'INSERT INTO {rds_schema}.current_term_index ({", ".join(columns)}) VALUES {values} '):
                    transaction.commit()
                else:
                    transaction.rollback()
                    raise BackgroundJobError('Error refreshing RDS current term index.')

    def get_sis_current_term(self, for_date):
        sql = f"SELECT * FROM {rds_schema}.term_definitions WHERE term_ends > '{for_date}' ORDER BY term_id ASC LIMIT 1"
        rows = rds.fetch(sql)
        return rows and rows[0]
