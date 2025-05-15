"""
Copyright ©2025. The Regents of the University of California (Regents). All Rights Reserved.

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

import pytz
from flask import current_app as app
from nessie.externals import s3
from nessie.externals import calendly_api
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.util import get_s3_calendly_daily_path, hashed_datestamp, localize_datetime, utc_now


class ImportCalendlyApi(BackgroundJob):

    def run(self):
        localized_date = localize_datetime(datetime.now()).date()
        start_date = localized_date - timedelta(app.config['CALENDLY_FETCH_DAYS_BEHIND'])
        end_date = localized_date + timedelta(app.config['CALENDLY_FETCH_DAYS_AHEAD'])
        min_start_time_utc = start_date
        max_start_time_utc = min_start_time_utc + timedelta(1)
        while min_start_time_utc <= end_date:
            self._put_calendly_events_to_s3(min_start_time_utc, max_start_time_utc)
            min_start_time_utc = max_start_time_utc + timedelta(milliseconds=1)
            max_start_time_utc = min_start_time_utc + timedelta(days=1)
        app.logger.info(f"Finished Calendly events import from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        return True

    @staticmethod
    def _put_calendly_events_to_s3(min_start_time, max_start_time):
        min_start_time = _to_iso_format(min_start_time)
        max_start_time = _to_iso_format(max_start_time)
        timeframe_description = f'min_start_time={min_start_time} and max_start_time={max_start_time}'
        app.logger.info(f'Fetching Calendly events where {timeframe_description}')
        events = calendly_api.get_events(
            min_start_time=min_start_time,
            max_start_time=max_start_time,
        )
        if not len(events):
            app.logger.info(f'No Calendly events found where {timeframe_description}')
            return

        imported_at = utc_now().strftime('%Y-%m-%dT%H:%M:%SZ')
        serialized_data = ''
        for e in events:
            # Make JsonSerDe schema creation easier in Redshift: transform arrays to dicts,
            # and output one JSON record per line in text file in S3.
            serialized_data += json.dumps({'importedAt': imported_at, **e}) + '\n'
        # Upload one copy to the daily path. We keep it there for a few days in S3 in case something goes wrong.
        # We may need to recover an earlier run.
        s3.upload_data(
            data=serialized_data,
            s3_key=f'{get_s3_calendly_daily_path()}/events/{min_start_time}/events.json',
        )
        # Upload one copy to the archive path which we expect to keep as our permanent record.
        s3_path_prefix = app.config['LOCH_S3_CALENDLY_DATA_PATH']
        s3.upload_data(
            data=serialized_data,
            s3_key=f'{s3_path_prefix}/archive/{hashed_datestamp(min_start_time)}/events/events.json',
        )
        app.logger.info(f'Uploaded data for {len(events)} Calendly events on {min_start_time}')


def _to_iso_format(value):
    return value.astimezone(pytz.utc).isoformat()
