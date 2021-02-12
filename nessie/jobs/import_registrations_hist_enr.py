"""
Copyright ©2021. The Regents of the University of California (Regents). All Rights Reserved.

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
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
import json
from timeit import default_timer as timer

from flask import current_app as app
from nessie.externals import redshift, s3, sis_student_api
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.berkeley import career_code_to_name, feature_flag_edl, term_info_for_sis_term_id
from nessie.lib.queries import get_edl_student_registrations, get_non_advisees_without_registration_imports
from nessie.lib.util import encoded_tsv_row, get_s3_sis_api_daily_path, resolve_sql_template_string
import numpy as np

"""Imports historical student registration data."""


class ImportRegistrationsHistEnr(BackgroundJob):

    rds_schema = app.config['RDS_SCHEMA_STUDENT']
    redshift_schema = app.config['REDSHIFT_SCHEMA_EDL' if feature_flag_edl() else 'REDSHIFT_SCHEMA_STUDENT']
    max_threads = app.config['STUDENT_API_MAX_THREADS']

    def run(self, load_mode='batch'):
        new_sids = [row['sid'] for row in get_non_advisees_without_registration_imports()]
        load_all = feature_flag_edl() or load_mode == 'new'

        # The size of the non-advisee population makes it unlikely that a one-shot load of all these slow feeds will
        # finish successfully without interfering with other work. Therefore the default approach is to apply a strict
        # upper limit on the number of feeds loaded in any one job run, no matter how many SIDs remain to be processed.
        if load_all:
            sids = new_sids
        elif load_mode == 'batch':
            max_batch = app.config['HIST_ENR_REGISTRATIONS_IMPORT_BATCH_SIZE']
            if max_batch >= len(new_sids):
                sids = new_sids
            else:
                sids = new_sids[0:(max_batch)]

        app.logger.info(f'Starting import of historical registration data for {len(sids)} students...')
        redshift.execute('VACUUM; ANALYZE;')

        rows = {
            'term_gpas': [],
            'last_registrations': [],
        }
        successes, failures = self._query_edl(rows, sids) if feature_flag_edl() else self._query_student_api(rows, sids)
        if len(successes) > 0:
            for key in rows.keys():
                filename = f'{key}_edl' if feature_flag_edl() else f'{key}_api'
                s3_key = f'{get_s3_sis_api_daily_path()}/{filename}.tsv'
                app.logger.info(f'Will stash {len(successes)} feeds in S3: {s3_key}')
                if not s3.upload_tsv_rows(rows[key], s3_key):
                    raise BackgroundJobError('Error on S3 upload: aborting job.')
                app.logger.info('Will copy S3 feeds into Redshift...')
                if not redshift.execute(f'TRUNCATE {self.redshift_schema}_staging.hist_enr_{key}'):
                    raise BackgroundJobError('Error truncating old staging rows: aborting job.')
                if not redshift.copy_tsv_from_s3(f'{self.redshift_schema}_staging.hist_enr_{key}', s3_key):
                    raise BackgroundJobError('Error on Redshift copy: aborting job.')
                staging_to_destination_query = resolve_sql_template_string(
                    """
                    DELETE FROM {target_schema}.hist_enr_{table_key}
                        WHERE sid IN
                        (SELECT sid FROM {target_schema}_staging.hist_enr_{table_key});
                    INSERT INTO {target_schema}.hist_enr_{table_key}
                        (SELECT * FROM {target_schema}_staging.hist_enr_{table_key});
                    TRUNCATE TABLE {target_schema}_staging.hist_enr_{table_key};
                    """,
                    table_key=key,
                    target_schema=self.redshift_schema,
                )
                if not redshift.execute(staging_to_destination_query):
                    raise BackgroundJobError('Error inserting staging entries into destination: aborting job.')

        redshift.execute('VACUUM; ANALYZE;')
        app.logger.info(f'Finished import of historical registration data for {len(sids)} students.')
        return successes, failures

    def _query_edl(self, rows, sids):
        successes = []
        for edl_row in get_edl_student_registrations(sids):
            sid = edl_row['student_id']
            if sid not in successes:
                # Based on the SQL order_by above, the first result per SID will be 'last_registration'.
                successes.append(sid)
                rows['last_registrations'].append(
                    encoded_tsv_row([sid, _edl_registration_to_json(edl_row)]),
                )
            rows['term_gpas'].append(
                encoded_tsv_row(
                    [
                        sid,
                        edl_row['term_id'],
                        edl_row['current_term_gpa_nbr'] or '0',
                        edl_row.get('unitsTakenForGpa') or '0',  # TODO: Does EDL give us 'unitsTakenForGpa'?
                    ],
                ),
            )
        failures = list(np.setdiff1d(sids, successes))
        return successes, failures

    def _query_student_api(self, rows, sids):
        successes = []
        failures = []
        app_obj = app._get_current_object()
        start_loop = timer()
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            for result in executor.map(_async_get_feed, repeat(app_obj), sids):
                sid = result['sid']
                full_feed = result['feed']
                if full_feed:
                    successes.append(sid)
                    rows['last_registrations'].append(
                        encoded_tsv_row([sid, json.dumps(full_feed.get('last_registration', {}))]),
                    )
                    gpa_feed = full_feed.get('term_gpas', {})
                    if gpa_feed:
                        for term_id, term_data in gpa_feed.items():
                            row = [
                                sid,
                                term_id,
                                (term_data.get('gpa') or '0'),
                                (term_data.get('unitsTakenForGpa') or '0'),
                            ]
                            rows['term_gpas'].append(encoded_tsv_row(row))
                    else:
                        app.logger.info(f'No past UGRD registrations found for SID {sid}.')
                else:
                    failures.append(sid)
                    app.logger.error(f'Registration history import failed for SID {sid}.')
        app.logger.info(f'Wanted {len(sids)} students; got {len(successes)} in {timer() - start_loop} secs')
        return successes, failures


def _async_get_feed(app_obj, sid):
    with app_obj.app_context():
        app.logger.info(f'Fetching registration history for SID {sid}')
        feed = sis_student_api.get_term_gpas_registration_demog(sid, with_demog=False)
        result = {
            'sid': sid,
            'feed': feed,
        }
    return result


def _edl_registration_to_json(row):
    def _flag_to_bool(key):
        return (row[key] or '').upper() == 'Y'

    def _str(v):
        return v and str(v)
    term_id = row['term_id']
    season, year = term_info_for_sis_term_id(term_id)
    career_code = row['academic_career_cd']
    # TODO: From EDL query results, what do we do with 'total_cumulative_gpa_nbr'?
    # TODO: All 'None' entries below need investigation. Does EDL provide?
    return {
        'loadedAt': _str(row['edl_load_date']),
        'term': {
            'id': term_id,
            'name': f'{year} {season}',
            'category': {
                'code': None,
                'description': None,
            },
            'academicYear': year,
            'beginDate': None,
            'endDate': None,
        },
        'academicCareer': {
            'code': career_code,
            'description': career_code_to_name(career_code),
        },
        'eligibleToRegister': _flag_to_bool('eligible_to_enroll_flag'),
        'eligibilityStatus': {
            'code': row['registrn_eligibility_status_cd'],
            'description': row['eligibility_status_desc'],
        },
        'registered': _flag_to_bool('registered_flag'),
        'disabled': None,
        'athlete': None,
        'intendsToGraduate': _flag_to_bool('intends_to_graduate_flag'),
        'academicLevels': [
            {
                'type': {
                    'code': 'BOT',
                    'description': 'Beginning of Term',
                },
                'level': {
                    'code': row['academic_level_beginning_of_term_cd'],
                    'description': row['academic_level_beginning_of_term_desc'],
                },
            },
            {
                'type': {
                    'code': 'EOT',
                    'description': 'End of Term',
                },
                'level': {
                    'code': row['academic_level_end_of_term_cd'],
                    'description': row['academic_level_end_of_term_desc'],
                },
            },
        ],
        'academicStanding': {
            'standing': {
                'code': None,
                'description': None,
            },
            'status': {
                'code': None,
                'description': None,
            },
            'fromDate': None,
        },
        'termUnits': [
            {
                'type': {
                    'code': 'Total',
                    'description': 'Total Units',
                },
                'unitsCumulative': None,
                'unitsEnrolled': _str(row['units_term_enrolled']),
                'unitsIncomplete': None,
                'unitsMax': _str(row['units_term_enrollment_max']),
                'unitsMin': _str(row['units_term_enrollment_min']),
                'unitsOther': None,
                'unitsPassed': None,
                'unitsTaken': _str(row['total_units_completed_qty']),  # TODO: Is this right?
                'unitsTransferAccepted': None,
                'unitsTransferEarned': None,
                'unitsWaitlisted': None,
            },
            {
                'type': {
                    'code': 'For GPA',
                    'description': 'Units For GPA',
                },
                'unitsEnrolled': None,
                'unitsIncomplete': None,
                'unitsMax': None,
                'unitsMin': None,
                'unitsOther': None,
                'unitsPassed': _str(row['unt_passd_gpa']),
                'unitsTaken': _str(row['unt_taken_gpa']),
                'unitsTransferAccepted': None,
                'unitsTransferEarned': None,
                'unitsWaitlisted': None,
            },
            {
                'type': {
                    'code': 'Not For GPA',
                    'description': 'Units Not For GPA',
                },
                'unitsEnrolled': _str(row['tot_inprog_gpa']),
                'unitsIncomplete': None,
                'unitsMax': _str(row['max_nogpa_unit']),
                'unitsMin': None,
                'unitsOther': None,
                'unitsPassed': _str(row['unt_passd_nogpa']),
                'unitsTaken': _str(row['unt_taken_nogpa']),
                'unitsTransferAccepted': None,
                'unitsTransferEarned': None,
                'unitsWaitlisted': None,
            },
        ],
        'termGPA': {
            'type': {
                'code': 'TGPA',
                'description': 'Term GPA',
            },
            'average': _str(row['current_term_gpa']),
            'source': 'UCB',
        },
        'withdrawalCancel': {
            'date': _str(row['withdraw_date']),
            'reason': {
                'code': row['withdraw_reason'],
                'description': _withdraw_code_to_name(row['withdraw_reason']),
            },
            'type': {
                'code': row['withdraw_code'],
                'description': _withdraw_code_to_name(row['withdraw_code']),
            },
        },
    }


def _withdraw_code_to_name(code):
    mappings = {
        'CAN': 'CAN',
        'DNSH': 'DNSH',
        'DYSH': 'DYSH',
        'MEDA': 'MEDA',
        'MEDI': 'Medical',
        'NPAY': 'NPAY',
        'NWD': 'NWD',
        'OTHR': 'Other',
        'PARN': 'PARN',
        'PERS': 'Personal',
        'RETR': 'RETR',
        'RSCH': 'RSCH',
        'WDR': 'Withdrew',
    }
    return mappings.get(code) or code
