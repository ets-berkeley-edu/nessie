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
from decimal import Decimal
from itertools import groupby, islice, repeat
import json
from operator import itemgetter
from tempfile import TemporaryFile
from threading import current_thread

from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.berkeley import career_code_to_name, term_info_for_sis_term_id
from nessie.lib.queries import get_edl_degrees, get_edl_demographics, get_edl_holds, get_edl_plans, get_edl_profile_terms,\
    get_edl_profiles, get_edl_registrations
from nessie.lib.util import get_s3_edl_daily_path, resolve_sql_template, write_to_tsv_file
from nessie.merged.student_demographics import GENDER_CODE_MAP, merge_from_details, UNDERREPRESENTED_GROUPS

"""Logic for EDL SIS schema creation job."""


class CreateEdlSchema(BackgroundJob):

    external_schema = app.config['REDSHIFT_SCHEMA_EDL_EXTERNAL']
    internal_schema = app.config['REDSHIFT_SCHEMA_EDL']

    batch_size = app.config['EDL_SCHEMA_BATCH_SIZE']
    max_threads = app.config['EDL_SCHEMA_MAX_THREADS']

    def run(self):
        app.logger.info('Starting EDL schema creation job...')
        self.create_schema()
        self.generate_feeds()
        return 'EDL schema creation job completed.'

    def create_schema(self):
        app.logger.info('Executing SQL...')
        template_sql = 'create_edl_schema.template.sql'
        resolved_ddl = resolve_sql_template(template_sql)
        if not redshift.execute_ddl_script(resolved_ddl):
            raise BackgroundJobError('EDL SIS schema creation job failed.')
        # Create staging schema
        resolved_ddl_staging = resolve_sql_template(
            template_sql,
            redshift_schema_edl=f'{self.internal_schema}_staging',
        )
        if redshift.execute_ddl_script(resolved_ddl_staging):
            app.logger.info(f"Schema '{self.internal_schema}_staging' found or created.")
        else:
            raise BackgroundJobError(f'{self.internal_schema} schema creation failed.')

        app.logger.info('Redshift EDL schema created.')

    def generate_feeds(self):
        if app.config['FEATURE_FLAG_EDL_STUDENT_PROFILES']:
            self.generate_sis_profile_feeds()
        if app.config['FEATURE_FLAG_EDL_DEGREE_PROGRESS']:
            self.generate_degree_progress_feeds()
        if app.config['FEATURE_FLAG_EDL_DEMOGRAPHICS']:
            self.generate_demographics_feeds()
        if app.config['FEATURE_FLAG_EDL_REGISTRATIONS']:
            self.generate_registration_feeds()

    def generate_sis_profile_feeds(self):
        app.logger.info('Staging SIS profile feeds...')

        profile_results = groupby(get_edl_profiles(), lambda r: r['sid'])

        supplemental_queries = {
            'degrees': get_edl_degrees,
            'holds': get_edl_holds,
            'plans': get_edl_plans,
            'profile_terms': get_edl_profile_terms,
        }
        supplemental_query_results = {k: groupby(query(), lambda r: r['sid']) for k, query in supplemental_queries.items()}

        def _grouped_profile_results():
            sid_tracker = {k: '' for k in supplemental_query_results.keys()}
            rows_tracker = {}

            for sid, profile_rows in profile_results:
                grouped_results = {
                    'sid': sid,
                    'profile': list(profile_rows),
                }
                for k in supplemental_query_results.keys():
                    while sid_tracker[k] < sid:
                        sid_tracker[k], rows_tracker[k] = next(supplemental_query_results[k])
                    if sid_tracker[k] == sid:
                        grouped_results[k] = list(rows_tracker[k])
                yield grouped_results

        chunked_profiles = []
        while True:
            chunk = [r for r in islice(_grouped_profile_results(), self.batch_size)]
            if not chunk:
                break
            chunked_profiles.append(chunk)

        self._process_concurrent(chunked_profiles, _process_profile_feeds, 'student_profiles')

    def generate_degree_progress_feeds(self):
        app.logger.info('Staging degree progress feeds...')
        rows = redshift.fetch(f'SELECT * FROM {self.internal_schema}.student_degree_progress_index ORDER by sid')
        with TemporaryFile() as feeds:
            for sid, rows_for_student in groupby(rows, itemgetter('sid')):
                rows_for_student = list(rows_for_student)
                report_date = rows_for_student[0].get('report_date')
                feed = {
                    'reportDate': report_date.strftime('%Y-%m-%d'),
                    'requirements': {
                        row.get('requirement'): {
                            'name': row.get('requirement_desc'), 'status': row.get('status'),
                        } for row in rows_for_student
                    },
                }
                write_to_tsv_file(feeds, [sid, json.dumps(feed)])
            self._upload_file_to_staging('student_degree_progress', feeds)

    def generate_demographics_feeds(self):
        app.logger.info('Staging demographics feeds...')
        demographics_results = get_edl_demographics()

        chunked_demographics = []
        demographics_by_sid_iterator = iter(groupby(demographics_results, lambda r: r['sid']))
        while True:
            chunk = {sid: list(demographics_for_sid) for sid, demographics_for_sid in islice(demographics_by_sid_iterator, self.batch_size)}
            if not chunk:
                break
            chunked_demographics.append(chunk)

        self._process_concurrent(chunked_demographics, _process_demographics_feeds, 'student_demographics')

    def generate_registration_feeds(self):
        app.logger.info('Staging registration feeds...')
        registration_results = get_edl_registrations()

        chunked_registrations = []
        registrations_by_sid_iterator = iter(groupby(registration_results, lambda r: r['sid']))
        while True:
            chunk = {sid: list(registrations_for_sid) for sid, registrations_for_sid in islice(registrations_by_sid_iterator, self.batch_size)}
            if not chunk:
                break
            chunked_registrations.append(chunk)

        self._process_concurrent(chunked_registrations, _process_registration_feeds, 'student_last_registrations')

    def _process_concurrent(self, chunks, processor_method, filename):
        tempfiles = []
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            app_obj = app._get_current_object()
            for result in executor.map(processor_method, repeat(app_obj), chunks):
                tempfiles.append(result)

        with TemporaryFile() as all_feeds:
            for t in tempfiles:
                t.seek(0)
                for line in t:
                    all_feeds.write(line)
                t.close()
            self._upload_file_to_staging(filename, all_feeds)

    def _upload_file_to_staging(self, table, _file):
        tsv_filename = f'staging_{table}.tsv'
        s3_key = f'{get_s3_edl_daily_path()}/{tsv_filename}'

        app.logger.info(f'Will stash {table} feeds in S3: {s3_key}')
        if not s3.upload_file(_file, s3_key):
            raise BackgroundJobError('Error on S3 upload: aborting job.')

        app.logger.info('Will copy S3 feeds into Redshift...')
        if not redshift.copy_tsv_from_s3(f'{self.internal_schema}.{table}', s3_key):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')


def _process_demographics_feeds(app_arg, chunk):
    def _simplified_ethnicities(ethnic_map):
        simpler_list = []
        for group in ethnic_map.keys():
            merge_from_details(simpler_list, group, ethnic_map[group])
        if not simpler_list:
            simpler_list.append('Not Specified')
        return sorted(simpler_list)

    with app_arg.app_context():
        app_arg.logger.debug(f'{current_thread().name} will process demographics feeds chunk ({len(chunk)} records)')
        feeds = TemporaryFile()
        for sid, rows in chunk.items():
            gender = None
            visa = None
            nationalities = set()
            ethnic_map = {}
            for r in rows:
                # TODO: Prefer gender identity once available (NS-1073)
                gender = r['gender']
                if r['visa_type']:
                    visa = {'status': r['visa_status'], 'type': r['visa_type']}
                if r['citizenship_country']:
                    nationalities.add(r['citizenship_country'])
                if r['ethnic_group']:
                    if r['ethnic_group'] not in ethnic_map:
                        ethnic_map[r['ethnic_group']] = set()
                    ethnic_map[r['ethnic_group']].add(r['ethnicity'])
            feed = {
                'gender': GENDER_CODE_MAP[gender],
                'ethnicities': _simplified_ethnicities(ethnic_map),
                'nationalities': sorted(nationalities),
                'underrepresented': not UNDERREPRESENTED_GROUPS.isdisjoint(ethnic_map.keys()),
                'visa': visa,
            }
            write_to_tsv_file(feeds, [sid, json.dumps(feed)])
        app_arg.logger.debug(f'{current_thread().name} wrote all feeds, returning TSV tempfile')
        return feeds


def _process_profile_feeds(app_arg, chunk):
    with app_arg.app_context():
        app_arg.logger.debug(f'{current_thread().name} will process profile feeds chunk ({len(chunk)} records)')
        feeds = TemporaryFile()

        for feed_components in chunk:
            sid = feed_components.get('sid')

            # We may see results from multiple academic careers. We prefer a UGRD career if present; otherwise we look
            # for a non-Law career with the most recent entering term.
            plans = feed_components.get('plans', [])
            career_code = None
            career_admit_term = ''
            for plan_row in feed_components.get('plans', []):
                if plan_row['academic_career_cd'] == 'UGRD':
                    career_code = 'UGRD'
                    break
                elif plan_row['academic_career_cd'] in {'UCBX', 'GRAD'} and plan_row['current_admit_term'] > career_admit_term:
                    career_code = plan_row['academic_career_cd']
                    career_admit_term = plan_row['current_admit_term']

            feed = {
                'identifiers': [
                    {
                        'id': sid,
                        'type': 'student-id',
                    },
                ],
            }

            _merge_profile(feed, feed_components.get('profile'))
            _merge_holds(feed, feed_components.get('holds'))
            _merge_academic_status(feed, feed_components.get('profile_terms'), career_code)
            _merge_plans(feed, plans, career_code)
            _merge_degrees(feed, feed_components.get('degrees'), career_code)

            write_to_tsv_file(feeds, [sid, json.dumps(feed)])

        app_arg.logger.debug(f'{current_thread().name} wrote all feeds, returning TSV tempfile')
        return feeds


def _merge_profile(feed, profile_rows):
    if not profile_rows or not len(profile_rows):
        return
    r = profile_rows[0]

    feed['emails'] = []
    if r['campus_email_address_nm']:
        feed['emails'].append({'emailAddress': r['campus_email_address_nm'], 'type': {'code': 'CAMP'}})
    if r['preferred_email_address_nm']:
        feed['emails'].append({'emailAddress': r['campus_email_address_nm'], 'primary': True, 'type': {'code': 'OTHR'}})

    feed['names'] = []
    if r['person_preferred_display_nm']:
        feed['names'].append({'formattedName': r['person_preferred_display_nm'], 'type': {'code': 'PRF'}})
    if r['person_display_nm']:
        feed['names'].append({'formattedName': r['person_display_nm'], 'type': {'code': 'PRI'}})

    if r['phone']:
        feed['phones'] = [{'number': r['phone'], 'type': {'code': r['phone_type']}}]


def _merge_holds(feed, hold_rows):
    if not hold_rows or not len(hold_rows):
        return
    feed['holds'] = []
    for r in hold_rows:
        feed['holds'].append({
            'fromDate': str(r['service_indicator_start_dt'])[0:10],
            'reason': {
                'description': r['service_indicator_reason_desc'],
                'formalDescription': r['service_indicator_long_desc'],
            },
        })


def _merge_academic_status(feed, profile_term_rows, career_code):
    if not profile_term_rows or not career_code:
        return

    latest_career_row = None
    # This crude calculation of total GPA units ignores transfer units and therefore won't agree with the number from the SIS API.
    # We only use it as a check for zero value to distinguish null from zero GPA.
    total_units_for_gpa = 0
    for row in profile_term_rows:
        if row['academic_career_cd'] == career_code:
            total_units_for_gpa += float(row['term_berkeley_completed_gpa_units'] or 0)
            latest_career_row = row
    if not latest_career_row:
        return

    academic_status = {
        'studentCareer': {
            'academicCareer': {
                'code': career_code,
            },
        },
    }
    academic_status['cumulativeGPA'] = {
        'average': float(latest_career_row['total_cumulative_gpa_nbr'] or 0),
    }
    academic_status['cumulativeUnits'] = [
        {
            'type': {
                'code': 'Total',
            },
            'unitsCumulative': float(latest_career_row['total_units_completed_qty'] or 0),
        },
        {
            'type': {
                'code': 'For GPA',
            },
            'unitsTaken': float(total_units_for_gpa),
        },
    ]
    if latest_career_row['terms_in_attendance']:
        academic_status['termsInAttendance'] = int(latest_career_row['terms_in_attendance'])

    feed['academicStatuses'] = [academic_status]


def _merge_plans(feed, plan_rows, career_code):
    if not plan_rows or not career_code or not feed.get['academicStatuses']:
        return

    academic_status = feed['academicStatuses'][0]
    academic_status['studentPlans'] = []
    statuses = set()
    matriculation_term_cd = ''
    effective_date = ''
    transfer_student = False

    for row in plan_rows:
        if row['academic_career_cd'] == career_code:
            statuses.add(_simplified_status(row))
            academic_status['studentPlans'].append(_construct_plan_feed(row))

            if row['matriculation_term_cd'] and str(row['matriculation_term_cd']) > matriculation_term_cd:
                matriculation_term_cd = str(row['matriculation_term_cd'])
            if row['academic_program_effective_dt'] and str(row['academic_program_effective_dt']) > effective_date:
                effective_date = str(row['academic_program_effective_dt'])
            if row['transfer_student'] == 'Y':
                transfer_student = True

    matriculation = {}
    if matriculation_term_cd:
        season, year = term_info_for_sis_term_id(matriculation_term_cd)
        matriculation['term'] = {'name': f'{year} {season}'}
    if transfer_student:
        matriculation['type'] = {'code': 'TRN'}
    academic_status['studentCareer']['matriculation'] = matriculation

    if effective_date:
        academic_status['studentCareer']['toDate'] = effective_date[0:10]

    feed['affiliations'] = []
    for status in statuses:
        feed['affiliations'].append({'status': {'description': status}, 'type': {'code': career_code_to_name(career_code)}})


def _construct_plan_feed(row):
    plan_feed = {
        'academicPlan': {
            'academicProgram': {
                'program': {
                    'description': row['academic_program_shrt_nm'],
                    'formalDescription': row['academic_program_nm'],
                },
            },
            'plan': {
                'description': row['academic_plan_nm'],
            },
            'type': {
                'code': row['academic_plan_type_cd'],
            },
        },
        'statusInPlan': {
            'status': {
                'formalDescription': _simplified_status(row),
            },
        },
    }
    if row['degree_expected_year_term_cd'] and row['degree_expected_year_term_cd'].strip():
        plan_feed['expectedGraduationTerm'] = {
            'id': row['degree_expected_year_term_cd'],
        }
    if row['academic_subplan_nm']:
        plan_feed['academicSubplans'] = {
            'subplan': {
                'description': row['academic_subplan_nm'],
            },
        }
    return plan_feed


def _simplified_status(row):
    return 'Active' if row['academic_program_status_desc'] == 'Active in Program' else row['academic_program_status_desc']


def _merge_degrees(feed, degree_rows, career_code):
    if not degree_rows or not career_code:
        return
    feed['degrees'] = []

    for degree, degree_plans in groupby(degree_rows, lambda r: [r['degree_conferred_dt'], r['degree_desc']]):
        degree_plans = list(degree_plans)
        if degree_plans[0]['academic_career_cd'] != career_code:
            continue

        degree_feed = {
            'academicDegree': {
                'type': {
                    'description': degree_plans[0]['degree_desc'],
                },
            },
            'academicPlans': [],
            'dateAwarded': str(degree_plans[0]['degree_conferred_dt'])[0:10],
            'status': {
                'description': degree_plans[0]['academic_degree_status_desc'],
            },
        }

        for plan in degree_plans:
            plan_feed = {
                'plan': {
                    'description': plan['academic_plan_nm'],
                    'formalDescription': plan['academic_plan_transcr_desc'],
                },
                'targetDegree': {
                    'type': {
                        'description': plan['degree_desc'],
                    },
                },
                'type': {
                    'code': plan['academic_plan_type_cd'],
                },
            }
            if plan['academic_group_desc']:
                plan_feed['academicProgram'] = {
                    'academicGroup': {
                        'formalDescription': plan['academic_group_desc'],
                    },
                }
            degree_feed['academicPlans'].append(plan_feed)

        feed['degrees'].append(degree_feed)


def _process_registration_feeds(app_arg, chunk):
    def _str(v):
        return (v is not None) and (float(v) if isinstance(v, Decimal) else str(v))

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

    def _find_last_registration(rows):
        last_registration = None

        for row in rows:
            # We prefer the most recent completed registration. But if the only registration data
            # is for an in-progress or future term, use it as a fallback.
            is_pending = (row['term_enrolled_units'] and not row['term_berkeley_completed_total_units'])
            if is_pending and last_registration:
                continue

            # At present, terms spent as an Extension student are not included in Term GPAs (but see BOAC-2266).
            # However, if there are no other types of registration, the Extension term is used for academicCareer.
            if row['academic_career_cd'] == 'UCBX':
                if last_registration and last_registration['academic_career_cd'] != 'UCBX':
                    continue

            # The most recent registration will be at the end of the list.
            last_registration = row

        return last_registration

    def _generate_feed(row):
        feed = {
            'term': {
                'id': _str(row['term_id']),
            },
            'academicCareer': {
                'code': _str(row['academic_career_cd']),
            },
            'academicLevels': [
                {
                    'type': {
                        'code': 'BOT',
                        'description': 'Beginning of Term',
                    },
                    'level': {
                        'code': _str(row['academic_level_beginning_of_term_cd']),
                        'description': row['academic_level_beginning_of_term_desc'],
                    },
                },
                {
                    'type': {
                        'code': 'EOT',
                        'description': 'End of Term',
                    },
                    'level': {
                        'code': _str(row['academic_level_end_of_term_cd']),
                        'description': row['academic_level_end_of_term_desc'],
                    },
                },
            ],
            'termUnits': [
                {
                    'type': {
                        'code': 'Total',
                        'description': 'Total Units',
                    },
                    'unitsEnrolled': _str(row['term_enrolled_units']),
                    'unitsMax': _str(row['maximum_term_enrollment_units_limit']),
                    'unitsMin': _str(row['minimum_term_enrollment_units_limit']),
                    'unitsTaken': _str(row['term_berkeley_completed_total_units']),
                },
            ],
        }
        if row['withdraw_code'] != 'NWD':
            feed['withdrawalCancel'] = {
                'date': _str(row['withdraw_date']),
                'reason': {
                    'code': row['withdraw_reason'],
                    'description': _withdraw_code_to_name(row['withdraw_reason']),
                },
                'type': {
                    'code': _str(row['withdraw_code']),
                    'description': _withdraw_code_to_name(row['withdraw_code']),
                },
            }
        return feed

    with app_arg.app_context():
        app_arg.logger.debug(f'{current_thread().name} will process registration feeds chunk ({len(chunk)} records)')
        feeds = TemporaryFile()
        for sid, rows in chunk.items():
            last_registration = _find_last_registration(rows)
            if last_registration:
                feed = _generate_feed(last_registration)
                write_to_tsv_file(feeds, [sid, json.dumps(feed)])
        app_arg.logger.debug(f'{current_thread().name} wrote all feeds, returning TSV tempfile')
        return feeds
