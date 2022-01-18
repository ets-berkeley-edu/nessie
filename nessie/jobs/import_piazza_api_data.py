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

import json
from re import split

from flask import current_app as app
from nessie.externals import s3
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.metadata import update_background_job_status
from nessie.lib.util import get_s3_piazza_data_path
import requests

"""Piazza API import."""


class ImportPiazzaApiData(BackgroundJob):

    def run(self, archive=None):
        frequency, datestamp, archive, s3_path = get_s3_piazza_data_path(archive)
        return self.process_archives(frequency, datestamp, self.job_id)

    def process_archives(self, frequency, datestamp, job_id):
        s3_key = app.config['LOCH_S3_PIAZZA_DATA_PATH']
        self.sid = app.config['PIAZZA_API_SID']  # this is Piazza school ID for Berkeley
        self.session_id = app.config['PIAZZA_API_SESSIONID']  # a 'random string' but we still are getting it from config
        self.headers = {
            'Content-Type': 'application/json',
            'CSRF-Token': self.session_id,
        }
        try:
            list_of_archives = self.get_list_of_archives(self.headers)
            archives_to_process = self.select_archives_by_type_and_date(list_of_archives, frequency, datestamp)
            if not archives_to_process:
                app.logger.debug(f'{frequency}/{datestamp}: no archives found for these criteria')
                return f'{frequency}/{datestamp}: no archives found for these criteria'
            for file_number, archive_file in enumerate(archives_to_process):
                download_url = self.piazza_api('school.generate_url', self.headers, {'sid': self.sid, 'name': archive_file['name']})
                download_url = download_url.text
                download_url = json.loads(download_url)['result']
                app.logger.debug('Download URL: ' + download_url)
                piazza_file_name = archive_file['name']
                # piazza_file_name is like 'daily_2020-08-14.zip' or 'full_2020-08-14.zip'
                # in s3 it will end up in e.g. .../piazza-data/daily/2020/08/14/daily_2020-08-14.zip
                parts = '/'.join(split('[\._\-]', piazza_file_name)[0:4])
                s3_file = f'{s3_key}/{parts}/{piazza_file_name}.zip'

                def update_streaming_status(headers):
                    update_background_job_status(job_id, 'streaming', details=f"{s3_file}, size={headers.get('Content-Length')}")

                response = s3.upload_from_url(download_url, s3_file, on_stream_opened=update_streaming_status)
                if response and job_id:
                    destination_size = response.get('ContentLength')
                    update_background_job_status(job_id, 'stream complete', details=f'{s3_file}, stream complete, size={destination_size}')
        except Exception as e:
            # let the people upstairs know, they're in charge
            raise e
        return ', '.join(f"{a['name']}: {a['size']} bytes" for a in archives_to_process)

    def get_list_of_archives(self, headers):
        email = app.config['PIAZZA_API_USERNAME']  # email for piazza account with school export permission
        password = app.config['PIAZZA_API_PASSWORD']
        # login to Piazza
        response = self.piazza_api('user.login', headers, {'email': email, 'pass': password})
        # we need this cookie!
        piazza_session = response.cookies['piazza_session'].replace('"', '')
        headers['Cookie'] = 'session_id=' + self.session_id + ';piazza_session=' + piazza_session
        # ok, get the archives we need...
        all_available_archives = self.piazza_api('school.list_available_archives', headers, {'sid': self.sid})
        list_of_archives = json.loads(all_available_archives.text)['result']
        app.logger.debug('All available archives:')
        app.logger.debug(list_of_archives)
        """
        here's what list_of_archives looks like
        //
           size: total size of zip file in bytes
           name: name of file
           from: timestamp from what time archive starts
           to: timestamp to what time archive ends
           type: 'fill', 'monthly' or 'daily'
        //
        e.g.
           [
             {
               size: 9807649,
               name: 'full_2020-06-05',
               from: 0,
               to: 1591353234,
               type: 'full'
             }
           ]
        """
        return sorted(list_of_archives, reverse=True, key=lambda x: x['to'])

    def select_archives_by_type_and_date(self, list_of_archives, frequency, datestamp):
        archives = []
        for archive in list_of_archives:
            if frequency not in archive['name']:
                continue
            if datestamp == 'latest' or datestamp is None:
                archives.append(list_of_archives[0])
                app.logger.info(f'Latest {frequency} archive: {archives[0]["name"]}')
                break
            elif datestamp in archive['name']:
                archives.append(archive)
                app.logger.debug(f'Selected {frequency} archive: {archive["name"]}')
        return archives

    def piazza_api(self, method, headers, params):
        working_url = app.config['PIAZZA_API_URL']
        payload = {
            'method': method,
            'params': params,
        }
        response = requests.post(working_url, headers=headers, json=payload)
        return response
