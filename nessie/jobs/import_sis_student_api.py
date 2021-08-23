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
from datetime import datetime, timedelta
from itertools import repeat
import json
from timeit import default_timer as timer

from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.externals.sis_student_api import get_v2_by_sids_list
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.berkeley import current_term_id
from nessie.lib.queries import get_all_student_ids, student_schema, student_schema_table
from nessie.lib.util import encoded_tsv_row, get_s3_sis_api_daily_path, resolve_sql_template_string

"""Logic for SIS student API import job."""


def async_get_feeds(app_obj, up_to_100_sids, as_of):
    with app_obj.app_context():
        feeds = get_v2_by_sids_list(up_to_100_sids, term_id=current_term_id(), with_registration=True, as_of=as_of)
        result = {
            'sids': up_to_100_sids,
            'feeds': feeds,
        }
    return result


class ImportSisStudentApi(BackgroundJob):

    max_threads = app.config['STUDENT_API_MAX_THREADS']

    def run(self, csids=None):
        if not csids:
            csids = [row['sid'] for row in get_all_student_ids()]
        app.logger.info(f'Starting SIS student API import job for {len(csids)} students...')

        rows, failure_count = self.load(csids)
        if (len(rows) == 0) and (failure_count > 0):
            raise BackgroundJobError('Failed to import SIS student API feeds: aborting job.')

        s3_key = f'{get_s3_sis_api_daily_path()}/profiles.tsv'
        app.logger.info(f'Will stash {len(rows)} feeds in S3: {s3_key}')
        if not s3.upload_tsv_rows(rows, s3_key):
            raise BackgroundJobError('Error on S3 upload: aborting job.')

        app.logger.info('Will copy S3 feeds into Redshift...')

        sis_profiles_table = student_schema_table('sis_profiles')
        if not redshift.execute(f'TRUNCATE {student_schema()}_staging.{sis_profiles_table}'):
            raise BackgroundJobError('Error truncating old staging rows: aborting job.')
        if not redshift.copy_tsv_from_s3(f'{student_schema()}_staging.{sis_profiles_table}', s3_key):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')

        staging_to_destination_query = resolve_sql_template_string(
            """
            DELETE FROM {redshift_schema}.{sis_profiles_table} WHERE sid IN
                (SELECT sid FROM {redshift_schema}_staging.{sis_profiles_table});
            INSERT INTO {redshift_schema}.{sis_profiles_table}
                (SELECT * FROM {redshift_schema}_staging.{sis_profiles_table});
            TRUNCATE {redshift_schema}_staging.{sis_profiles_table};
            """,
            redshift_schema=student_schema(),
            sis_profiles_table=sis_profiles_table,
        )
        if not redshift.execute(staging_to_destination_query):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')

        return f'SIS student API import job completed: {len(rows)} succeeded, {failure_count} failed.'

    def load(self, all_sids):
        return self._load_from_edl(all_sids) if app.config['FEATURE_FLAG_EDL_STUDENT_PROFILES'] else self._load_from_student_api(all_sids)

    def _load_from_student_api(self, all_sids):
        # Students API will not return 'unitsTransferEarned' and 'unitsTransferAccepted' data
        # for incoming transfer students unless we request an 'as-of-date' in their enrolled term.
        near_future = (datetime.now() + timedelta(days=60)).strftime('%Y-%m-%d')

        chunked_sids = [all_sids[i:i + 100] for i in range(0, len(all_sids), 100)]
        rows = []
        failure_count = 0
        app_obj = app._get_current_object()
        start_loop = timer()
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            for result in executor.map(async_get_feeds, repeat(app_obj), chunked_sids, repeat(near_future)):
                remaining_sids = set(result['sids'])
                feeds = result['feeds']
                if feeds:
                    for feed in feeds:
                        sid = next(_id['id'] for _id in feed['identifiers'] if _id['type'] == 'student-id')
                        remaining_sids.discard(sid)
                        rows.append(encoded_tsv_row([sid, json.dumps(feed)]))
                if remaining_sids:
                    failure_count += len(remaining_sids)
                    app.logger.error(f'SIS student API import failed for SIDs {remaining_sids}.')
        app.logger.info(f'Wanted {len(all_sids)} students; got {len(rows)} in {timer() - start_loop} secs')
        return rows, failure_count

    def _load_from_edl(self, all_sids):
        return self._mock_edl_rows(all_sids), 0

    def _mock_edl_rows(self, all_sids):
        results = []
        for sid in all_sids:
            results.append({
                'identifiers': [
                    {
                        'type': 'student-id',
                        'id': sid,
                        'disclose': True,
                    },
                    {
                        'type': 'campus-uid',
                        'id': '********',
                        'disclose': True,
                    },
                    {
                        'type': 'Social Security Number',
                        'id': '***-**-****',
                        'primary': True,
                        'disclose': False,
                    },
                    {
                        'type': 'CalNet ID',
                        'id': '********',
                        'disclose': False,
                        'fromDate': '2020-04-22',
                    },
                    {
                        'type': 'HCM System',
                        'id': '********',
                        'disclose': False,
                        'fromDate': '2019-08-26',
                    },
                ],
                'names': [
                    {
                        'type': {
                            'code': 'PRF',
                            'description': 'Preferred',
                        },
                        'familyName': '********',
                        'givenName': '********',
                        'middleName': '********',
                        'formattedName': '********',
                        'preferred': True,
                        'disclose': True,
                        'uiControl': {
                            'code': 'U',
                            'description': 'Edit - No Delete',
                        },
                        'fromDate': '2019-05-12',
                    },
                    {
                        'type': {
                            'code': 'PRI',
                            'description': 'Primary',
                        },
                        'familyName': '********',
                        'givenName': '********',
                        'formattedName': '********',
                        'preferred': False,
                        'disclose': True,
                        'uiControl': {
                            'code': 'D',
                            'description': 'Display Only',
                        },
                        'fromDate': '2019-07-24',
                    },
                ],
                'affiliations': [
                    {
                        'type': {
                            'code': 'UNDERGRAD',
                            'description': 'Undergraduate Student',
                            'formalDescription': 'An individual with an Undergraduate-based Career/Program/Plan.',
                        },
                        'detail': 'Active',
                        'status': {
                            'code': 'ACT',
                            'description': 'Active',
                            'formalDescription': 'Active',
                        },
                        'fromDate': '2019-06-04',
                    },
                ],
                'addresses': [
                    {
                        'type': {
                            'code': 'HOME',
                            'description': 'Home',
                        },
                        'address1': '********',
                        'address2': '********',
                        'city': '********',
                        'stateCode': 'CA',
                        'stateName': 'California',
                        'postalCode': '********',
                        'countryCode': 'USA',
                        'countryName': 'United States',
                        'formattedAddress': '********',
                        'disclose': True,
                        'uiControl': {
                            'code': 'U',
                            'description': 'Edit - No Delete',
                        },
                        'fromDate': '2019-07-24',
                    },
                    {
                        'type': {
                            'code': 'LOCL',
                            'description': 'Local',
                        },
                        'address1': '********',
                        'city': '********',
                        'county': '********',
                        'stateCode': 'CA',
                        'stateName': 'California',
                        'postalCode': '********',
                        'countryCode': 'USA',
                        'countryName': 'United States',
                        'formattedAddress': '********',
                        'disclose': True,
                        'uiControl': {
                            'code': 'U',
                            'description': 'Edit - No Delete',
                        },
                        'fromDate': '2020-09-13',
                    },
                ],
                'phones': [
                    {
                        'type': {
                            'code': 'CELL',
                            'description': 'Mobile',
                        },
                        'number': '********',
                        'primary': True,
                        'disclose': True,
                        'uiControl': {
                            'code': 'F',
                            'description': 'Full Edit',
                        },
                    },
                    {
                        'type': {
                            'code': 'HOME',
                            'description': 'Home/Permanent',
                        },
                        'number': '********',
                        'primary': False,
                        'disclose': True,
                        'uiControl': {
                            'code': 'U',
                            'description': 'Edit - No Delete',
                        },
                    },
                ],
                'emails': [
                    {
                        'type': {
                            'code': 'CAMP',
                            'description': 'Campus',
                        },
                        'emailAddress': '********',
                        'primary': True,
                        'disclose': True,
                        'uiControl': {
                            'code': 'D',
                            'description': 'Display Only',
                        },
                    },
                    {
                        'type': {
                            'code': 'OTHR',
                            'description': 'Other',
                        },
                        'emailAddress': '********',
                        'primary': False,
                        'disclose': True,
                        'uiControl': {
                            'code': 'F',
                            'description': 'Full Edit',
                        },
                    },
                    {
                        'type': {
                            'code': 'PERS',
                            'description': 'Personal',
                        },
                        'emailAddress': '********',
                        'primary': False,
                        'disclose': True,
                        'uiControl': {
                            'code': 'F',
                            'description': 'Full Edit',
                        },
                    },
                ],
                'confidential': False,
                'academicStatuses': [
                    {
                        'studentCareer': {
                            'academicCareer': {
                                'code': 'UGRD',
                                'description': 'Undergrad',
                                'formalDescription': 'Undergraduate',
                            },
                            'matriculation': {
                                'term': {
                                    'id': '2198',
                                    'name': '2019 Fall',
                                    'category': {
                                        'code': 'R',
                                        'description': 'Regular Term',
                                    },
                                    'academicYear': '2020',
                                    'beginDate': '2019-08-21',
                                    'endDate': '2019-12-20',
                                },
                                'type': {
                                    'code': 'TRN',
                                    'description': 'Transfer',
                                    'formalDescription': 'Transfer Student',
                                },
                                'homeLocation': {
                                    'code': '041',
                                    'description': '********',
                                },
                            },
                            'fromDate': '2019-06-04',
                        },
                        'studentPlans': [
                            {
                                'academicPlan': {
                                    'plan': {
                                        'code': '25345U',
                                        'description': 'English BA',
                                        'formalDescription': 'English',
                                    },
                                    'type': {
                                        'code': 'MAJ',
                                        'description': 'Major - Regular Acad/Prfnl',
                                        'formalDescription': 'Major - Regular Acad/Prfnl',
                                    },
                                    'cipCode': '23.0101',
                                    'targetDegree': {
                                        'type': {
                                            'code': 'AB',
                                            'description': 'Bachelor of Arts',
                                            'formalDescription': 'Bachelor of Arts',
                                        },
                                    },
                                    'ownedBy': [
                                        {
                                            'organization': {
                                                'code': 'ENGLISH',
                                                'description': 'English',
                                                'formalDescription': 'English',
                                            },
                                            'percentage': 100.0,
                                        },
                                    ],
                                    'academicProgram': {
                                        'program': {
                                            'code': 'UCLS',
                                            'description': 'UG L&S',
                                            'formalDescription': 'Undergrad Letters & Science',
                                        },
                                        'academicGroup': {
                                            'code': 'CLS',
                                            'description': 'L&S',
                                            'formalDescription': 'College of Letters and Science',
                                        },
                                        'academicCareer': {
                                            'code': 'UGRD',
                                            'description': 'Undergrad',
                                            'formalDescription': 'Undergraduate',
                                        },
                                    },
                                },
                                'statusInPlan': {
                                    'status': {
                                        'code': 'AC',
                                        'description': 'Active',
                                        'formalDescription': 'Active in Program',
                                    },
                                    'action': {
                                        'code': 'DATA',
                                        'description': 'Data Chg',
                                        'formalDescription': 'Data Change',
                                    },
                                    'reason': {
                                        'code': 'GSCH',
                                        'description': 'Graduation Status Change',
                                        'formalDescription': 'Graduation Status Change',
                                    },
                                },
                                'primary': True,
                                'expectedGraduationTerm': {
                                    'id': '2212',
                                    'name': '2021 Spring',
                                    'category': {
                                        'code': 'R',
                                        'description': 'Regular Term',
                                    },
                                    'academicYear': '2021',
                                    'beginDate': '2021-01-12',
                                    'endDate': '2021-05-14',
                                },
                                'degreeCheckoutStatus': {
                                    'code': 'EG',
                                    'description': 'Eligible',
                                    'formalDescription': 'Eligible for Graduation',
                                },
                                'fromDate': '2019-10-23',
                                'toDate': '2021-05-14',
                            },
                            {
                                'academicPlan': {
                                    'plan': {
                                        'code': '25429U',
                                        'description': 'History BA',
                                        'formalDescription': 'History',
                                    },
                                    'type': {
                                        'code': 'MAJ',
                                        'description': 'Major - Regular Acad/Prfnl',
                                        'formalDescription': 'Major - Regular Acad/Prfnl',
                                    },
                                    'cipCode': '54.0101',
                                    'targetDegree': {
                                        'type': {
                                            'code': 'AB',
                                            'description': 'Bachelor of Arts',
                                            'formalDescription': 'Bachelor of Arts',
                                        },
                                    },
                                    'ownedBy': [
                                        {
                                            'organization': {
                                                'code': 'HISTORY',
                                                'description': 'History',
                                                'formalDescription': 'History',
                                            },
                                            'percentage': 100.0,
                                        },
                                    ],
                                    'academicProgram': {
                                        'program': {
                                            'code': 'UCLS',
                                            'description': 'UG L&S',
                                            'formalDescription': 'Undergrad Letters & Science',
                                        },
                                        'academicGroup': {
                                            'code': 'CLS',
                                            'description': 'L&S',
                                            'formalDescription': 'College of Letters and Science',
                                        },
                                        'academicCareer': {
                                            'code': 'UGRD',
                                            'description': 'Undergrad',
                                            'formalDescription': 'Undergraduate',
                                        },
                                    },
                                },
                                'statusInPlan': {
                                    'status': {
                                        'code': 'AC',
                                        'description': 'Active',
                                        'formalDescription': 'Active in Program',
                                    },
                                    'action': {
                                        'code': 'DATA',
                                        'description': 'Data Chg',
                                        'formalDescription': 'Data Change',
                                    },
                                    'reason': {
                                        'code': 'GSCH',
                                        'description': 'Graduation Status Change',
                                        'formalDescription': 'Graduation Status Change',
                                    },
                                },
                                'primary': False,
                                'expectedGraduationTerm': {
                                    'id': '2212',
                                    'name': '2021 Spring',
                                    'category': {
                                        'code': 'R',
                                        'description': 'Regular Term',
                                    },
                                    'academicYear': '2021',
                                    'beginDate': '2021-01-12',
                                    'endDate': '2021-05-14',
                                },
                                'degreeCheckoutStatus': {
                                    'code': 'EG',
                                    'description': 'Eligible',
                                    'formalDescription': 'Eligible for Graduation',
                                },
                                'fromDate': '2020-09-24',
                                'toDate': '2021-05-14',
                            },
                        ],
                        'termsInAttendance': 8,
                        'cumulativeGPA': {
                            'type': {
                                'code': 'CGPA',
                                'description': 'Cumulative GPA',
                            },
                            'average': 0.000,
                            'source': 'UCB',
                        },
                        'cumulativeUnits': [
                            {
                                'type': {
                                    'code': 'Total',
                                    'description': 'Total Units',
                                },
                                'unitsEnrolled': 0,
                                'unitsTaken': 0.0,
                                'unitsPassed': 0.0,
                                'unitsTransferEarned': 0,
                                'unitsTransferAccepted': 0.0,
                                'unitsCumulative': 0.0,
                            },
                            {
                                'type': {
                                    'code': 'For GPA',
                                    'description': 'Units For GPA',
                                },
                                'unitsTaken': 0,
                                'unitsPassed': 0,
                            },
                            {
                                'type': {
                                    'code': 'Not For GPA',
                                    'description': 'Units Not For GPA',
                                },
                                'unitsEnrolled': 0,
                                'unitsTaken': 0,
                                'unitsPassed': 0,
                                'unitsTransferEarned': 0,
                                'unitsTransferAccepted': 0.0,
                            },
                        ],
                    },
                ],
                'registrations': [
                    {
                        'term': {
                            'id': '2212',
                            'name': '2021 Spring',
                            'category': {
                                'code': 'R',
                                'description': 'Regular Term',
                            },
                            'academicYear': '2021',
                            'beginDate': '2021-01-12',
                            'endDate': '2021-05-14',
                        },
                        'academicCareer': {
                            'code': 'UGRD',
                            'description': 'Undergraduate',
                        },
                        'eligibleToRegister': True,
                        'eligibilityStatus': {
                            'code': 'C',
                            'description': 'Continuing',
                        },
                        'registered': True,
                        'disabled': True,
                        'athlete': False,
                        'intendsToGraduate': False,
                        'academicLevels': [
                            {
                                'type': {
                                    'code': 'BOT',
                                    'description': 'Beginning of Term',
                                },
                                'level': {
                                    'code': '40',
                                    'description': 'Senior',
                                },
                            },
                            {
                                'type': {
                                    'code': 'EOT',
                                    'description': 'End of Term',
                                },
                                'level': {
                                    'code': '40',
                                    'description': 'Senior',
                                },
                            },
                        ],
                        'termUnits': [
                            {
                                'type': {
                                    'code': 'Total',
                                    'description': 'Total Units',
                                },
                                'unitsMin': 0,
                                'unitsMax': 0.0,
                                'unitsEnrolled': 0,
                            },
                            {
                                'type': {
                                    'code': 'For GPA',
                                    'description': 'Units For GPA',
                                },
                            },
                            {
                                'type': {
                                    'code': 'Not For GPA',
                                    'description': 'Units Not For GPA',
                                },
                                'unitsMax': 0.0,
                                'unitsEnrolled': 0,
                            },
                        ],
                        'termGPA': {
                            'type': {
                                'code': 'TGPA',
                                'description': 'Term GPA',
                            },
                            'average': 0,
                            'source': 'UCB',
                        },
                    },
                ],
                'degrees': [
                    {
                        'id': '1',
                        'academicDegree': {
                            'type': {
                                'code': 'IGETC',
                                'description': 'Intersegmental General Ed',
                            },
                        },
                        'status': {
                            'code': 'C',
                            'description': 'Complete',
                        },
                        'statusDate': '2019-07-02',
                    },
                    {
                        'id': '1',
                        'academicDegree': {
                            'type': {
                                'code': 'ASSC',
                                'description': 'Ext Associate Degree or Equiv',
                            },
                        },
                        'status': {
                            'code': 'I',
                            'description': 'In Progress',
                        },
                        'statusDate': '2019-05-15',
                    },
                ],
            })
        return results
