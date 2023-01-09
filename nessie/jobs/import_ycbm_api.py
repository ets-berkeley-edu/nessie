"""
Copyright ©2022. The Regents of the University of California (Regents). All Rights Reserved.

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
import json

from flask import current_app as app
from nessie.externals import s3
from nessie.externals import ycbm_api
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.util import get_s3_ycbm_daily_path, hashed_datestamp, localize_datetime, utc_now


class ImportYcbmApi(BackgroundJob):

    def run(self):
        localized_date = localize_datetime(datetime.now()).date()
        start_date = localized_date - timedelta(app.config['YCBM_FETCH_DAYS_BEHIND'])
        end_date = localized_date + timedelta(app.config['YCBM_FETCH_DAYS_AHEAD'])
        fetch_date = start_date
        while fetch_date <= end_date:
            _put_booking_data_to_s3(fetch_date)
            fetch_date = fetch_date + timedelta(1)
        app.logger.info(f"Finished bookings import from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        return True


def _put_booking_data_to_s3(date):
    datestamp = date.strftime('%Y-%m-%d')
    app.logger.info(f'Starting YCBM bookings import for {datestamp}...')

    bookings = ycbm_api.get_bookings_for_date(date)
    if not bookings or not len(bookings):
        app.logger.info(f'No bookings found for {datestamp}')
        return

    imported_at = utc_now().strftime('%Y-%m-%dT%H:%M:%SZ')
    serialized_data = ''
    for b in bookings:
        b['importedAt'] = imported_at
        # Make JsonSerDe schema creation easier in Redshift: transform arrays to dicts, and output one JSON record per line in text file in S3.
        answers_dict = {}
        for a in b.get('answers', []):
            if 'code' in a and 'string' in a:
                # 4096 bytes is Redshift's maxiumum for a CHAR column.
                if a['string'] and len(a['string']) > 4000:
                    answer_string = a['string'][0:4000] + '...'
                else:
                    answer_string = a['string']
                answers_dict[a['code'].lower()] = answer_string
        b['answers'] = answers_dict
        serialized_data += json.dumps(b) + '\n'
    # Upload one copy to the daily path, which we keep for a few days in S3 in case something goes wrong and we need to
    # recover an earlier run.
    s3.upload_data(serialized_data, f'{get_s3_ycbm_daily_path()}/bookings/{datestamp}/bookings.json')
    # Upload one copy to the archive path which we expect to keep as our permanent record.
    s3.upload_data(serialized_data, f"{app.config['LOCH_S3_YCBM_DATA_PATH']}/archive/{hashed_datestamp(date)}/bookings/bookings.json")
    app.logger.info(f'Uploaded data for {len(bookings)} bookings on {datestamp}')
