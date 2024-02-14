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

from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.externals.asc_athletes_api import get_asc_feed
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.util import encoded_tsv_row, get_s3_asc_daily_path, resolve_sql_template_string

SPORT_TRANSLATIONS = {
    'MBB': 'BAM',
    'MBK': 'BBM',
    'WBK': 'BBW',
    'MCR': 'CRM',
    'WCR': 'CRW',
    'MFB': 'FBM',
    'WFH': 'FHW',
    'MGO': 'GOM',
    'WGO': 'GOW',
    'MGY': 'GYM',
    'WGY': 'GYW',
    'WLC': 'LCW',
    'MRU': 'RGM',
    'WSF': 'SBW',
    'MSC': 'SCM',
    'WSC': 'SCW',
    'MSW': 'SDM',
    'WSW': 'SDW',
    # 'Beach Volleyball' vs. 'Sand Volleyball'.
    'WBV': 'SVW',
    'MTE': 'TNM',
    'WTE': 'TNW',
    # ASC's subsets of Track do not directly match the Athlete API's subsets. In ASC's initial data transfer,
    # all track athletes were mapped to 'TO*', 'Outdoor Track & Field'.
    'MTR': 'TOM',
    'WTR': 'TOW',
    'WVB': 'VBW',
    'MWP': 'WPM',
    'WWP': 'WPW',
}


# There are multiple groups within these teams, and the remainder group (for team members who don't fit any
# of the defined squads or specialties) is misleadingly named as if it identifies the entire team.
AMBIGUOUS_GROUP_CODES = [
    'MFB',
    'MSW',
    'MTR',
    'WSW',
    'WTR',
]


class ImportAscAthletes(BackgroundJob):

    def run(self):
        app.logger.info('ASC import: Fetch team and student athlete data from ASC API')
        api_results = get_asc_feed()
        if 'error' in api_results:
            raise BackgroundJobError('ASC import: Error from external API: {}'.format(api_results['error']))
        elif not api_results:
            raise BackgroundJobError('ASC import: API returned zero students')
        sync_date = api_results[0]['SyncDate']
        if sync_date != api_results[-1]['SyncDate']:
            raise BackgroundJobError(f'ASC import: SyncDate conflict in ASC API: {api_results[0]} vs. {api_results[-1]}')
        rows = []
        for r in api_results:
            if r['AcadYr'] == app.config['ASC_THIS_ACAD_YR'] and r['SportCode']:
                asc_code = r['SportCodeCore']
                if asc_code in SPORT_TRANSLATIONS:
                    group_code = r['SportCode']
                    data = [
                        r['SID'],
                        str(r.get('ActiveYN', 'No') == 'Yes'),
                        str(r.get('IntensiveYN', 'No') == 'Yes'),
                        r.get('SportStatus', ''),
                        group_code,
                        _unambiguous_group_name(r['Sport'], group_code),
                        SPORT_TRANSLATIONS[asc_code],
                        r['SportCore'],
                    ]
                    rows.append(encoded_tsv_row(data))
                else:
                    sid = r['SID']
                    app.logger.error(f'ASC import: Unmapped asc_code {asc_code} has ActiveYN for sid={sid}')

        s3_key = f'{get_s3_asc_daily_path()}/asc_api_raw_response_{sync_date}.tsv'
        if not s3.upload_tsv_rows(rows, s3_key):
            raise BackgroundJobError('Error on S3 upload: aborting job.')

        app.logger.info('Copy data in S3 file to Redshift...')
        query = resolve_sql_template_string(
            """
            TRUNCATE {redshift_schema_asc}.students;
            COPY {redshift_schema_asc}.students
                FROM 's3://{s3_bucket}/{s3_key}'
                IAM_ROLE '{redshift_iam_role}'
                DELIMITER '\\t';
            """,
            s3_bucket=app.config['LOCH_S3_BUCKET'],
            s3_key=s3_key,
        )
        if not redshift.execute(query):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')

        status = {
            'this_sync_date': sync_date,
            'api_results_count': len(api_results),
        }
        app.logger.info(f'ASC import: Successfully completed import job: {str(status)}')
        return status


def _unambiguous_group_name(asc_group_name, group_code):
    return f'{asc_group_name} - Other' if group_code in AMBIGUOUS_GROUP_CODES else asc_group_name
