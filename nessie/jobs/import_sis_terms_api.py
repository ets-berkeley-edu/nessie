"""
Copyright ©2018. The Regents of the University of California (Regents). All Rights Reserved.

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


"""Logic for SIS terms API import job."""

from flask import current_app as app
from nessie.externals import rds, redshift, s3, sis_terms_api
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.berkeley import reverse_term_ids
from nessie.lib.util import get_s3_sis_api_daily_path, resolve_sql_template_string, split_tsv_row


class ImportSisTermsApi(BackgroundJob):

    destination_schema = app.config['REDSHIFT_SCHEMA_SIS_INTERNAL']

    def run(self, term_ids=None):
        if not term_ids:
            term_ids = reverse_term_ids()
        app.logger.info(f'Starting SIS terms API import job for {len(term_ids)} terms...')

        rows = []
        success_count = 0
        failure_count = 0
        index = 1
        for term_id in term_ids:
            app.logger.info(f'Fetching SIS terms API for term id {term_id} ({index} of {len(term_ids)})')
            feed = sis_terms_api.get_term(term_id)
            if feed:
                success_count += 1
                for academic_career_term in feed:
                    for session in academic_career_term.get('sessions', []):
                        rows.append(
                            '\t'.join([
                                academic_career_term.get('id', ''),
                                academic_career_term.get('name', ''),
                                academic_career_term.get('academicCareer', {}).get('code', ''),
                                academic_career_term.get('beginDate', ''),
                                academic_career_term.get('endDate', ''),
                                session.get('id', ''),
                                session.get('name', ''),
                                session.get('beginDate', ''),
                                session.get('endDate', ''),
                            ]),
                        )
            else:
                failure_count += 1
                app.logger.error(f'SIS terms API import failed for term id {term_id}.')
            index += 1

        s3_key = f'{get_s3_sis_api_daily_path()}/terms.tsv'
        app.logger.info(f'Will stash {len(rows)} rows from {success_count} feeds in S3: {s3_key}')
        if not s3.upload_data('\n'.join(rows), s3_key):
            app.logger.error('Error on S3 upload: aborting job.')
            return False

        app.logger.info('Will copy S3 feeds into Redshift...')
        with redshift.transaction() as transaction:
            if self.update_redshift(term_ids, transaction):
                transaction.commit()
                app.logger.info('Updated Redshift.')
            else:
                transaction.rollback()
                app.logger.error('Failed to update Redshift.')
                return False

        with rds.transaction() as transaction:
            if self.update_rds(rows, term_ids, transaction):
                transaction.commit()
                app.logger.info('Updated RDS.')
            else:
                transaction.rollback()
                app.logger.error('Failed to update RDS.')
                return False

        return f'SIS terms API import job completed: {success_count} succeeded, {failure_count} failed.'

    def update_redshift(self, term_ids, transaction):
        if not transaction.execute(
            f'DELETE FROM {self.destination_schema}.sis_terms WHERE term_id = ANY(%s)',
            params=(term_ids,),
        ):
            return False
        template = """COPY {redshift_schema_sis_internal}.sis_terms
                      FROM '{loch_s3_sis_api_data_path}/terms.tsv'
                      IAM_ROLE '{redshift_iam_role}'
                      DELIMITER '\\t';"""
        if not transaction.execute(resolve_sql_template_string(template)):
            return False
        return True

    def update_rds(self, rows, term_ids, transaction):
        if not transaction.execute(
            f'DELETE FROM {self.destination_schema}.sis_terms WHERE term_id = ANY(%s)',
            params=(term_ids,),
        ):
            return False
        if not transaction.insert_bulk(
            f"""INSERT INTO {self.destination_schema}.sis_terms
                (term_id ,term_name , academic_career, term_begins, term_ends, session_id, session_name, session_begins, session_ends)
                VALUES %s""",
            [tuple(split_tsv_row(r)) for r in rows],
        ):
            return False
        return True
