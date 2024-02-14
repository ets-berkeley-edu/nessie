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

from urllib.parse import urlparse

from flask import current_app as app
from nessie.externals import boac, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.util import get_s3_boa_api_daily_path


"""Logic for BOAC metadata export and cache refresh kickoff."""


class RefreshBoacCache(BackgroundJob):

    def run(self):
        app.logger.info('Starting BOA notes metadata export...')
        for boa_credentials in app.config['BOAC_REFRESHERS']:
            skip_notes_metadata_export = boa_credentials.get('SKIP_NOTES_METADATA_EXPORT', False)
            if not skip_notes_metadata_export:
                with boac.export_notes_metadata(boa_credentials) as export:
                    hostname = urlparse(boa_credentials['API_BASE_URL']).hostname.split('.')[0]
                    s3_key = f'{get_s3_boa_api_daily_path()}/{hostname}/advising_notes_metadata.csv'
                    app.logger.info(f"Will upload BOA notes metadata to S3: url={boa_credentials['API_BASE_URL']}, key={s3_key}")
                    s3.upload_from_response(export, s3_key)
                app.logger.info('Notes metadata export complete.')

        app.logger.info('Starting BOAC refresh kickoffs...')
        if boac.kickoff_refresh():
            app.logger.info('BOAC refresh kickoffs completed.')
            return True
        else:
            raise BackgroundJobError('BOAC refresh kickoffs returned an error.')
