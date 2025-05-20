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

from datetime import datetime, timedelta, timezone
import json

from flask import current_app as app
from nessie.externals import s3
from nessie.externals import calendly_api
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.util import get_s3_calendly_daily_path, hashed_datestamp, localize_datetime, utc_now


class ImportCalendlyApi(BackgroundJob):

    def run(self):
        # Today's date
        localized_date = localize_datetime(datetime.now()).date()
        # In the API call, we request scheduled-events of an N day span, per config values.
        start_date = localized_date - timedelta(app.config['CALENDLY_FETCH_DAYS_BEHIND'])
        calendly_min_event_start = start_date
        calendly_max_event_start = calendly_min_event_start + timedelta(1)
        # Continue to fetch until we reach 'end_date'
        end_date = localized_date + timedelta(app.config['CALENDLY_FETCH_DAYS_AHEAD'])
        total_event_count = 0
        while calendly_min_event_start <= end_date:
            total_event_count += self._put_calendly_events_to_s3(
                calendly_min_event_start,
                calendly_max_event_start,
            )
            # Increment
            calendly_min_event_start = calendly_max_event_start + timedelta(days=1)
            calendly_max_event_start = calendly_min_event_start + timedelta(days=2)
        app.logger.info(f"Finished Calendly events import from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        return (
            f'Calendly API imported {total_event_count} events where start_date={start_date} and end_date={end_date}'
        )

    @staticmethod
    def _put_calendly_events_to_s3(calendly_min_event_start, calendly_max_event_start):
        def _datetime_combine(date, min_or_max):
            return datetime.combine(date, min_or_max.time()).replace(tzinfo=timezone.utc)

        app.logger.info(f'Fetching Calendly events between {calendly_min_event_start} and {calendly_max_event_start}')
        events = calendly_api.get_scheduled_events(
            min_start_time=_datetime_combine(calendly_min_event_start, datetime.min),
            max_start_time=_datetime_combine(calendly_max_event_start, datetime.max),
        )
        if len(events):
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
                s3_key=f'{get_s3_calendly_daily_path()}/events/{calendly_min_event_start}/events.json',
            )
            # Upload one copy to the archive path which we expect to keep as our permanent record.
            s3_path_prefix = app.config['LOCH_S3_CALENDLY_DATA_PATH']
            s3.upload_data(
                data=serialized_data,
                s3_key=f'{s3_path_prefix}/archive/{hashed_datestamp(calendly_min_event_start)}/events/events.json',
            )
            app.logger.info(f'Uploaded data for {len(events)} Calendly events on {calendly_min_event_start}')
        else:
            app.logger.info(f'No Calendly events between {calendly_min_event_start} and {calendly_max_event_start}')
        return len(events)
