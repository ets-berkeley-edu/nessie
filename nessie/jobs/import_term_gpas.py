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
from nessie.externals import rds, redshift, s3, sis_student_api
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.queries import get_all_student_ids
from nessie.lib.util import encoded_tsv_row, get_s3_sis_api_daily_path, resolve_sql_template_string, split_tsv_row

"""Logic for term GPA import job."""


class ImportTermGpas(BackgroundJob):

    rds_schema = app.config['RDS_SCHEMA_STUDENT']
    redshift_schema = app.config['REDSHIFT_SCHEMA_STUDENT']

    def run(self, csids=None):
        if not csids:
            csids = [row['sid'] for row in get_all_student_ids()]

        app.logger.info(f'Starting term GPA import job for {len(csids)} students...')

        rows = []
        success_count = 0
        no_registrations_count = 0
        failure_count = 0
        index = 1
        for csid in csids:
            app.logger.info(f'Fetching term GPAs for SID {csid}, ({index} of {len(csids)})')
            feed = sis_student_api.get_term_gpas(csid)
            if feed:
                success_count += 1
                for term_id, term_data in feed.items():
                    rows.append(encoded_tsv_row([csid, term_id, (term_data.get('gpa') or '0'), (term_data.get('unitsTakenForGpa') or '0')]))
            elif feed == {}:
                app.logger.info(f'No past UGRD registrations found for SID {csid}.')
                no_registrations_count += 1
            else:
                failure_count += 1
                app.logger.error(f'Term GPA import failed for SID {csid}.')
            index += 1

        if (success_count == 0) and (failure_count > 0):
            raise BackgroundJobError('Failed to import term GPAs: aborting job.')

        s3_key = f'{get_s3_sis_api_daily_path()}/term_gpas.tsv'
        app.logger.info(f'Will stash {success_count} feeds in S3: {s3_key}')
        if not s3.upload_tsv_rows(rows, s3_key):
            raise BackgroundJobError('Error on S3 upload: aborting job.')

        app.logger.info('Will copy S3 feeds into Redshift...')
        if not redshift.execute(f'TRUNCATE {self.redshift_schema}_staging.student_term_gpas'):
            raise BackgroundJobError('Error truncating old staging rows: aborting job.')
        if not redshift.copy_tsv_from_s3(f'{self.redshift_schema}_staging.student_term_gpas', s3_key):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')
        staging_to_destination_query = resolve_sql_template_string("""
            DELETE FROM {redshift_schema_student}.student_term_gpas
                WHERE sid IN
                (SELECT sid FROM {redshift_schema_student}_staging.student_term_gpas);
            INSERT INTO {redshift_schema_student}.student_term_gpas
                (SELECT * FROM {redshift_schema_student}_staging.student_term_gpas);
            TRUNCATE TABLE {redshift_schema_student}_staging.student_term_gpas;
            """)
        if not redshift.execute(staging_to_destination_query):
            raise BackgroundJobError('Error inserting staging entries into destination: aborting job.')

        with rds.transaction() as transaction:
            if self.refresh_rds_indexes(csids, rows, transaction):
                transaction.commit()
                app.logger.info('Refreshed RDS indexes.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Failed to refresh RDS indexes.')

        return (
            f'Term GPA import completed: {success_count} succeeded, '
            f'{no_registrations_count} returned no registrations, {failure_count} failed.'
        )

    def refresh_rds_indexes(self, csids, rows, transaction):
        sql = f'DELETE FROM {self.rds_schema}.student_term_gpas WHERE sid = ANY(%s)'
        params = (csids,)
        if not transaction.execute(sql, params):
            return False
        if not transaction.insert_bulk(
            f"""INSERT INTO {self.rds_schema}.student_term_gpas
                (sid, term_id, gpa, units_taken_for_gpa) VALUES %s""",
            [split_tsv_row(r) for r in rows],
        ):
            return False

        return True
