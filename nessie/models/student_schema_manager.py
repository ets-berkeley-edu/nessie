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

from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.jobs.background_job import BackgroundJobError
from nessie.lib.queries import student_schema
from nessie.lib.util import get_s3_sis_api_daily_path, resolve_sql_template_string
import psycopg2.sql

"""Higher-level logic for staged student schema in Redshift."""


def staging_schema():
    return f'{student_schema()}_staging'


def refresh_all_from_staging(tables):
    with redshift.transaction() as transaction:
        for table in tables:
            refresh_from_staging(table, None, None, transaction)
        if not transaction.commit():
            raise BackgroundJobError(f'Final transaction commit failed for {student_schema()}.')


def refresh_from_staging(table, term_id, sids, transaction, truncate_staging=True):
    # If our job is restricted to a particular term id or set of sids, then drop rows from the destination table
    # matching those restrictions. If there are no restrictions, the entire destination table can be truncated.
    refresh_conditions = []
    refresh_params = []
    if term_id:
        refresh_conditions.append('term_id = %s')
        refresh_params.append(term_id)
    if sids:
        refresh_conditions.append('sid = ANY(%s)')
        refresh_params.append(sids)

    def _success():
        app.logger.info(f'Populated {student_schema()}.{table} from staging schema.')

    def _rollback():
        transaction.rollback()
        raise BackgroundJobError(f'Failed to populate table {student_schema()}.{table} from staging schema.')

    if not refresh_conditions:
        transaction.execute(
            'TRUNCATE {schema}.{table}',
            schema=psycopg2.sql.Identifier(student_schema()),
            table=psycopg2.sql.Identifier(table),
        )
        app.logger.info(f'Truncated destination table {student_schema()}.{table}.')

        _success() if transaction.execute(
            'INSERT INTO {schema}.{table} (SELECT * FROM {staging_schema}.{table})',
            schema=psycopg2.sql.Identifier(student_schema()),
            staging_schema=psycopg2.sql.Identifier(staging_schema()),
            table=psycopg2.sql.Identifier(table),
        ) else _rollback()

    else:
        delete_sql = 'DELETE FROM {schema}.{table} WHERE ' + ' AND '.join(refresh_conditions)
        transaction.execute(
            delete_sql,
            schema=psycopg2.sql.Identifier(student_schema()),
            table=psycopg2.sql.Identifier(table),
            params=tuple(refresh_params),
        )
        app.logger.info(f"""
            Deleted existing rows from destination table {student_schema()}.{table} '
            term_id={term_id or 'all'}, {len(sids) if sids else 'all'} sids).
        """)
        insert_sql = 'INSERT INTO {schema}.{table} (SELECT * FROM {staging_schema}.{table} WHERE ' + ' AND '.join(refresh_conditions) + ')'

        _success() if transaction.execute(
            insert_sql,
            schema=psycopg2.sql.Identifier(student_schema()),
            staging_schema=psycopg2.sql.Identifier(staging_schema()),
            table=psycopg2.sql.Identifier(table),
            params=tuple(refresh_params),
        ) else _rollback()

    # The staging table can now be truncated, unless we're running a job distributed between workers.
    if truncate_staging:
        transaction.execute(
            'TRUNCATE {schema}.{table}',
            schema=psycopg2.sql.Identifier(staging_schema()),
            table=psycopg2.sql.Identifier(table),
        )
        app.logger.info(f'Truncated staging table {staging_schema()}.{table}.')


def truncate_staging_table(table):
    redshift.execute(
        'TRUNCATE {schema}.{table}',
        schema=psycopg2.sql.Identifier(staging_schema()),
        table=psycopg2.sql.Identifier(table),
    )


def upload_file_to_staging(table, term_file, row_count, term_id):
    tsv_filename = f'staging_{table}_{term_id}.tsv' if term_id else f'staging_{table}.tsv'
    s3_key = f'{get_s3_sis_api_daily_path()}/{tsv_filename}'
    app.logger.info(f'Will stash {row_count} feeds in S3: {s3_key}')
    # Be kind; rewind
    term_file.seek(0)
    if not s3.upload_data(term_file, s3_key):
        raise BackgroundJobError(f'Failed upload {row_count} records to s3:{s3_key}. Aborting job.')

    app.logger.info('Will copy S3 feeds into Redshift...')
    query = resolve_sql_template_string(
        """
        COPY {staging_schema}.{table}
            FROM '{loch_s3_sis_api_data_path}/{tsv_filename}'
            IAM_ROLE '{redshift_iam_role}'
            DELIMITER '\\t';
        """,
        staging_schema=staging_schema(),
        table=table,
        tsv_filename=tsv_filename,
    )
    if not redshift.execute(query):
        raise BackgroundJobError('Error on Redshift copy: aborting job.')


def verify_table(table):
    result = redshift.fetch(
        'SELECT COUNT(*) FROM {schema}.{table}',
        schema=psycopg2.sql.Identifier(staging_schema()),
        table=psycopg2.sql.Identifier(table),
    )
    if result and result[0] and result[0]['count']:
        count = result[0]['count']
        app.logger.info(f'Verified population of staging table {table} ({count} rows).')
    else:
        raise BackgroundJobError(f'Failed to verify population of staging table {table}: aborting job.')


def write_file_to_staging(table, term_file, row_count, term_id=None):
    upload_file_to_staging(table, term_file, row_count, term_id)
    verify_table(table)
    return True
