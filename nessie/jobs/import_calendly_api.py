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

        def _datetime_per_delta(timedelta_with_now, time):
            date = localized_date + timedelta_with_now
            return datetime.combine(date, time).replace(tzinfo=timezone.utc)
        # Fetch scheduled-events of N day span, per configs.
        min_event_start = _datetime_per_delta(-timedelta(app.config['CALENDLY_FETCH_DAYS_BEHIND']), datetime.min.time())
        max_event_start = _datetime_per_delta(timedelta(app.config['CALENDLY_FETCH_DAYS_AHEAD']), datetime.max.time())
        total_event_count = self._put_calendly_events_to_s3(
            min_event_start=min_event_start,
            max_event_start=max_event_start,
        )
        job_summary = f"""
            Imported {total_event_count} Calendly events.
            These events had start-time between {min_event_start} and {max_event_start}.
        """
        app.logger.info(job_summary)
        return job_summary

    @staticmethod
    def _put_calendly_events_to_s3(min_event_start, max_event_start):
        app.logger.info(f'Fetching Calendly events between {min_event_start} and {max_event_start}')
        # 'min_event_start' and 'max_event_start' refer to the start-time of Calendly events.
        events = calendly_api.get_scheduled_events(
            min_start_time=min_event_start,
            max_start_time=max_event_start,
        )
        if len(events):
            imported_at = utc_now().strftime('%Y-%m-%dT%H:%M:%SZ')
            serialized_data = ''
            for e in events:
                # JsonSerDe schema creation in Redshift via JSON records in S3.
                serialized_data += json.dumps({'imported_at': imported_at, **e}) + '\n'
            s3_directory = f"{min_event_start.strftime('%Y-%m-%d')}_to_{max_event_start.strftime('%Y-%m-%d')}"
            s3.upload_data(
                data=serialized_data,
                s3_key=f'{get_s3_calendly_daily_path()}/events/{s3_directory}/events.json',
            )
            # Upload a copy, to keep as our permanent record, to the archive path.
            s3_path_prefix = app.config['LOCH_S3_CALENDLY_DATA_PATH']
            s3.upload_data(
                data=serialized_data,
                s3_key=f'{s3_path_prefix}/archive/{hashed_datestamp(min_event_start)}/events/events.json',
            )
        return len(events)
