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
from nessie.externals.calendly_api import CALENDLY_API_DATE_FORMAT
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
        number_of_events_with_sid = 0
        if len(events):
            serialized_data, number_of_events_with_sid = _serialize_calendly_events(events)
            if serialized_data:
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
        return number_of_events_with_sid


def _serialize_calendly_events(events):
    number_of_events_with_sid = 0
    serialized_data = ''
    imported_at = utc_now().strftime(CALENDLY_API_DATE_FORMAT)

    events_by_uuid = {_extract_event_uuid(event['uri']): event for event in events}
    for uuid, event in events_by_uuid.items():
        event['uuid'] = uuid
        # First, initialize attributes of the meeting host.
        for key in ('host_email', 'host_name', 'host_uri'):
            event[key] = None
        # Next, extract host info, if present.
        if len(event.get('event_memberships', [])):
            event_membership = event['event_memberships'][0]
            event['host_email'] = event_membership['user_email']
            event['host_name'] = event_membership['user_name']
            event['host_uri'] = event_membership['user']
        # Extract student info
        includes_invitee_with_sid = False
        for event_invitee in calendly_api.get_event_invitees(uuid):
            student = {
                **{k: event_invitee[k] for k in ['email', 'name'] if k in event_invitee},
                'questions_and_answers': None,
                'sid': None,
            }
            questions_and_answers = []
            for question_and_answer in event_invitee.get('questions_and_answers', []):
                question = question_and_answer.get("question")
                answer = question_and_answer.get("answer")
                if question and answer:
                    if 'Student Identification Number (SID)' in question:
                        student['sid'] = answer
                        includes_invitee_with_sid = True
                    else:
                        questions_and_answers.append({'question': question, 'answer': answer})
            student['questions_and_answers'] = json.dumps(questions_and_answers)
            if includes_invitee_with_sid:
                event['student'] = student
                break
        if event['host_email'] and includes_invitee_with_sid:
            # Remove noisy Zoom info.
            if event.get('location', {}).get('type', None) == 'zoom':
                del event['location']
            # Remove other extraneous elements
            for key in ('calendar_event', 'event_type', 'invitees_counter', 'event_guests', 'event_memberships'):
                del event[key]
            # We want to capture canceled events and the related metadata.
            cancellation = event.get('cancellation', {})
            event['canceled_at'] = cancellation.get('created_at', None)
            event['canceled_by'] = cancellation.get('canceled_by', None)
            event['cancellation_reason'] = cancellation.get('reason', None)
            # JsonSerDe schema creation in Redshift via JSON records in S3.
            serialized_data += json.dumps({'imported_at': imported_at, **event}) + '\n'
            number_of_events_with_sid += 1
    return serialized_data, number_of_events_with_sid


def _extract_event_uuid(event_uri):
    return event_uri.split('/')[-1]
