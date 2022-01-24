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
from contextlib import contextmanager
from decimal import Decimal
from itertools import groupby, islice, repeat
import json
from operator import itemgetter
import pickle
from tempfile import TemporaryFile
from threading import current_thread

from flask import current_app as app
from nessie.externals import redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.berkeley import career_code_to_name, current_term_id, term_info_for_sis_term_id, term_name_for_sis_id
from nessie.lib.queries import stream_edl_degrees, stream_edl_demographics, stream_edl_holds, stream_edl_plans,\
    stream_edl_profile_terms, stream_edl_profiles, stream_edl_registrations
from nessie.lib.util import get_s3_edl_daily_path, resolve_sql_template, write_to_tsv_file
from nessie.merged.student_demographics import GENDER_CODE_MAP, merge_from_details, UNDERREPRESENTED_GROUPS

"""Logic for EDL SIS schema creation job."""


class CreateEdlSchema(BackgroundJob):

    external_schema = app.config['REDSHIFT_SCHEMA_EDL_EXTERNAL']
    internal_schema = app.config['REDSHIFT_SCHEMA_EDL']

    def run(self):
        app.logger.info('Starting EDL schema creation job...')
        self.create_schema()
        self.generate_feeds()
        return 'EDL schema creation job completed.'

    def create_schema(self):
        app.logger.info('Executing SQL...')
        template_sql = 'create_edl_schema.template.sql'
        resolved_ddl = resolve_sql_template(template_sql)
        if redshift.execute_ddl_script(resolved_ddl):
            app.logger.info(f'{self.internal_schema} Redshift schema created.')
        else:
            raise BackgroundJobError(f'{self.internal_schema} Redshift schema creation failed.')

    def generate_feeds(self):
        app.logger.info('Building profile feeds...')
        ProfileFeedBuilder().build()
        app.logger.info('Building demographics feeds...')
        DemographicsFeedBuilder().build()
        app.logger.info('Building registration feeds...')
        RegistrationsFeedBuilder().build()
        app.logger.info('Building degree progress feeds...')
        self.generate_degree_progress_feeds()

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
            _upload_file_to_staging('student_degree_progress', feeds)


class ConcurrentFeedBuilder(object):

    batch_size = app.config['EDL_SCHEMA_BATCH_SIZE']
    max_threads = app.config['EDL_SCHEMA_MAX_THREADS']

    # Subclasses implement.
    filename = None

    def build(self):
        source_files = []
        with self.fetch_source_feeds() as source_feed_generator:
            while True:
                results = False
                source_file = TemporaryFile()
                for source_feed in islice(source_feed_generator, self.batch_size):
                    pickle.dump(source_feed, source_file)
                    results = True
                if not results:
                    break
                source_files.append(source_file)

        target_files = []
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            app_obj = app._get_current_object()
            for result in executor.map(self.build_target_feeds, repeat(app_obj), source_files):
                target_files.append(result)

        with TemporaryFile() as all_feeds:
            for t in target_files:
                t.seek(0)
                for line in t:
                    all_feeds.write(line)
                t.close()
            _upload_file_to_staging(self.filename, all_feeds)

    # Subclasses implement.
    @contextmanager
    def fetch_source_feeds(self):
        yield

    # Subclasses implement.
    def build_target_feeds(self, app_arg, source_file):
        pass

    def get_pickled_feeds(self, f):
        f.seek(0)
        index = 0
        while True:
            try:
                feed = pickle.load(f)
                yield [feed['sid'], feed['feed'], index]
                index += 1
            except EOFError:
                break


class DemographicsFeedBuilder(ConcurrentFeedBuilder):

    filename = 'student_demographics'

    @contextmanager
    def fetch_source_feeds(self):
        stream = stream_edl_demographics()
        try:
            def _fetch_source_feeds():
                demographics_by_sid = iter(groupby(stream, lambda r: r['sid']))
                for sid, rows in demographics_by_sid:
                    yield {'sid': sid, 'feed': list(rows)}
            yield _fetch_source_feeds()
        finally:
            stream.close()

    def build_target_feeds(self, app_arg, source_file):
        with app_arg.app_context():
            app_arg.logger.debug(f'{current_thread().name} will process demographics feeds chunk')
            target_file = TemporaryFile()
            index = None

            for sid, rows, index in self.get_pickled_feeds(source_file):
                gender = None
                visa = None
                nationalities = set()
                ethnic_map = {}
                for r in rows:
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
                    'gender': GENDER_CODE_MAP.get(gender, None),
                    'ethnicities': self._simplified_ethnicities(ethnic_map),
                    'nationalities': sorted(nationalities),
                    'underrepresented': not UNDERREPRESENTED_GROUPS.isdisjoint(ethnic_map.keys()),
                    'visa': visa,
                }
                write_to_tsv_file(target_file, [sid, json.dumps(feed)])

            if index is None:
                app_arg.logger.warn(f'{current_thread().name} wrote no demographics feeds, returning empty tempfile')
            else:
                app_arg.logger.debug(f'{current_thread().name} wrote {index + 1} demographics feeds, returning TSV tempfile')
            return target_file

    @staticmethod
    def _simplified_ethnicities(ethnic_map):
        simpler_list = []
        for group in ethnic_map.keys():
            merge_from_details(simpler_list, group, ethnic_map[group])
        if not simpler_list:
            simpler_list.append('Not Specified')
        return sorted(simpler_list)


class ProfileFeedBuilder(ConcurrentFeedBuilder):

    filename = 'student_profiles'

    @contextmanager
    def fetch_source_feeds(self):
        profile_stream = stream_edl_profiles()
        supplemental_streams = {
            'degrees': stream_edl_degrees(),
            'holds': stream_edl_holds(),
            'plans': stream_edl_plans(),
            'profile_terms': stream_edl_profile_terms(),
        }

        try:
            profile_results = groupby(profile_stream, lambda r: r['sid'])
            supplemental_stream_results = {k: groupby(stream, lambda r: r['sid']) for k, stream in supplemental_streams.items()}

            sid_tracker = {k: '' for k in supplemental_stream_results.keys()}
            rows_tracker = {}

            def _fetch_source_feeds():
                # To avoid StopIteration errors, mark the ends of streams with a value alphabetically greater than any SID.
                stream_terminator = ('Z', [])

                for sid, profile_rows in profile_results:
                    grouped_results = {
                        'sid': sid,
                        'feed': {
                            'profile': list(profile_rows),
                        },
                    }
                    for k in supplemental_stream_results.keys():
                        while sid_tracker[k] < sid:
                            sid_tracker[k], rows_tracker[k] = next(supplemental_stream_results[k], stream_terminator)
                        if sid_tracker[k] == sid:
                            grouped_results['feed'][k] = list(rows_tracker[k])
                    yield grouped_results

            yield _fetch_source_feeds()

        finally:
            profile_stream.close()
            for stream in supplemental_streams.values():
                stream.close()

    def build_target_feeds(self, app_arg, source_file):
        with app_arg.app_context():
            app_arg.logger.debug(f'{current_thread().name} will process profile feeds chunk')
            target_file = TemporaryFile()
            index = None

            for sid, feed_components, index in self.get_pickled_feeds(source_file):
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

                self._merge_profile(feed, feed_components.get('profile'))
                self._merge_holds(feed, feed_components.get('holds'))
                self._merge_profile_terms(feed, feed_components.get('profile_terms'), career_code)
                self._merge_plans(feed, plans, career_code)
                self._merge_degrees(feed, feed_components.get('degrees'))

                write_to_tsv_file(target_file, [sid, json.dumps(feed)])

            if index is None:
                app_arg.logger.warn(f'{current_thread().name} wrote no profile feeds, returning empty tempfile')
            else:
                app_arg.logger.debug(f'{current_thread().name} wrote {index + 1} profile feeds, returning TSV tempfile')
            return target_file

    def _merge_profile(self, feed, profile_rows):
        if not profile_rows or not len(profile_rows):
            return
        r = profile_rows[0]

        feed['emails'] = []
        if r['campus_email_address_nm']:
            feed['emails'].append({'emailAddress': r['campus_email_address_nm'], 'type': {'code': 'CAMP'}})
        if r['preferred_email_address_nm']:
            feed['emails'].append({'emailAddress': r['preferred_email_address_nm'], 'primary': True, 'type': {'code': 'OTHR'}})

        preferred_name_parts = []
        primary_name_parts = []
        feed['names'] = []
        for col in ['person_preferred_first_nm', 'person_preferred_middle_nm', 'person_preferred_last_nm']:
            if r[col] and len(r[col]):
                preferred_name_parts.append(r[col])
        for col in ['person_first_nm', 'person_middle_nm', 'person_last_nm']:
            if r[col] and len(r[col]):
                primary_name_parts.append(r[col])
        if len(preferred_name_parts):
            feed['names'].append(
                {
                    'formattedName': ' '.join(preferred_name_parts),
                    'familyName': r['person_preferred_last_nm'],
                    'givenName': r['person_preferred_first_nm'],
                    'type': {'code': 'PRF'},
                },
            )
        if len(primary_name_parts):
            feed['names'].append(
                {
                    'formattedName': ' '.join(primary_name_parts),
                    'familyName': r['person_last_nm'],
                    'givenName': r['person_first_nm'],
                    'type': {'code': 'PRI'},
                },
            )

        if r['phone']:
            feed['phones'] = [{'number': r['phone'], 'type': {'code': r['phone_type']}}]

    def _merge_holds(self, feed, hold_rows):
        if not hold_rows or not len(hold_rows):
            return
        feed['holds'] = []
        for r in hold_rows:
            from_date = r['service_indicator_start_dt']
            feed['holds'].append({
                'fromDate': str(from_date)[0:10] if from_date else None,
                'reason': {
                    'description': r['service_indicator_reason_desc'],
                    'formalDescription': r['service_indicator_long_desc'],
                },
            })

    def _merge_profile_terms(self, feed, profile_term_rows, career_code):
        if not profile_term_rows or not career_code:
            return

        latest_academic_standing = None
        latest_career_row = None
        term_gpas = []
        # This crude calculation of total GPA units ignores transfer units and therefore won't agree with the number from the SIS API.
        # We only use it as a check for zero value to distinguish null from zero GPA.
        total_units_for_gpa = 0

        def _term_gpa(row):
            return {'termName': term_name_for_sis_id(row['term_id']), 'gpa': float(row['gpa'])}

        for row in profile_term_rows:
            if row['academic_career_cd'] == career_code:
                term_units_for_gpa = float(row['term_berkeley_completed_gpa_units'] or 0)
                total_units_for_gpa += term_units_for_gpa
                if term_units_for_gpa > 0:
                    term_gpas.append(_term_gpa(row))
                if row['acad_standing_status']:
                    latest_academic_standing = row
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
        if latest_academic_standing:
            feed['academicStanding'] = {
                'actionDate': str(latest_academic_standing['action_date']),
                'status': latest_academic_standing['acad_standing_status'],
                'termName': term_name_for_sis_id(latest_academic_standing['term_id']),
            }
        term_gpas.reverse()
        feed['termGpa'] = term_gpas

    def _merge_plans(self, feed, plan_rows, career_code):
        if not plan_rows or not career_code or not feed.get('academicStatuses'):
            return

        academic_status = feed['academicStatuses'][0]
        academic_status['studentPlans'] = []
        effective_date = ''
        ldap_affiliations = None
        matriculation_term_cd = None
        plans = set()
        statuses = set()
        transfer_student = False

        for row in plan_rows:
            if row['academic_career_cd'] == career_code:
                ldap_affiliations = row['ldap_affiliations']
                statuses.add(self._simplified_career_status(row))
                if row['academic_plan_nm'] not in plans:
                    plans.add(row['academic_plan_nm'])
                    academic_status['studentPlans'].append(self._construct_plan_feed(row))

                if not matriculation_term_cd:
                    matriculation_term_cd = self._get_matriculation_term(row)

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

        feed['calnet'] = {
            'affiliations': [a.strip() for a in ldap_affiliations.split(',')] if ldap_affiliations else [],
        }

        status = self._best_status(statuses)
        if status:
            code = career_code_to_name(career_code)
            feed['affiliations'] = [{'status': {'description': status}, 'type': {'code': code}}]

    def _get_matriculation_term(self, row):
        # Pick the first row we get through SQL ordering that does not correspond to a non-degree academic
        # program. If a summer term, treat as the next fall term.
        matriculation_term_cd = None
        if row['matriculation_term_cd'] and (row['academic_program_cd'] not in ['UNODG', 'GNODG', 'LNODG']):
            matriculation_term_cd = str(row['matriculation_term_cd'])
            if matriculation_term_cd[-1] == '5':
                matriculation_term_cd = matriculation_term_cd[0:3] + '8'
        return matriculation_term_cd

    def _construct_plan_feed(self, row):
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
                    'formalDescription': self._simplified_program_status(row),
                },
            },
        }
        if row['degree_expected_year_term_cd'] and row['degree_expected_year_term_cd'].strip():
            plan_feed['expectedGraduationTerm'] = {
                'id': row['degree_expected_year_term_cd'],
            }
        if row['academic_subplan_nm']:
            plan_feed['academicSubPlans'] = [
                {
                    'subPlan': {
                        'description': row['academic_subplan_nm'],
                    },
                },
            ]
        return plan_feed

    def _merge_degrees(self, feed, degree_rows):
        if not degree_rows:
            return

        feed['degrees'] = []
        for degree, degree_plans in groupby(degree_rows, lambda r: [r['degree_conferred_dt'], r['degree_desc']]):
            degree_plans = list(degree_plans)

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

    @staticmethod
    def _best_status(statuses):
        if 'Active' in statuses:
            return 'Active'
        elif 'Completed' in statuses:
            return 'Completed'
        elif 'Inactive' in statuses:
            return 'Inactive'

    @staticmethod
    def _simplified_career_status(row):
        simplifier = {
            'Active in Program': 'Active',
            'Cancelled': 'Inactive',
            'Completed Program': 'Completed',
            'Deceased': 'Inactive',
            'Discontinued': 'Inactive',
            'Dismissed': 'Inactive',
            'Leave of Absence': 'Inactive',
            'Suspended': 'Inactive',
        }
        return simplifier.get(row['academic_program_status_desc'], row['academic_program_status_desc'])

    @staticmethod
    def _simplified_program_status(row):
        return 'Active' if row['academic_program_status_desc'] == 'Active in Program' else row['academic_program_status_desc']


class RegistrationsFeedBuilder(ConcurrentFeedBuilder):

    filename = 'student_last_registrations'

    @contextmanager
    def fetch_source_feeds(self):
        stream = stream_edl_registrations()
        try:
            def _fetch_source_feeds():
                registrations_by_sid = iter(groupby(stream, lambda r: r['sid']))
                for sid, rows in registrations_by_sid:
                    yield {'sid': sid, 'feed': list(rows)}
            yield _fetch_source_feeds()
        finally:
            stream.close()

    def build_target_feeds(self, app_arg, source_file):
        with app_arg.app_context():
            app_arg.logger.debug(f'{current_thread().name} will process registration feeds chunk')
            target_file = TemporaryFile()
            index = None

            for sid, rows, index in self.get_pickled_feeds(source_file):
                last_registration = self._find_last_registration(rows)
                if last_registration:
                    feed = self._generate_feed(last_registration)
                    write_to_tsv_file(target_file, [sid, json.dumps(feed)])

            if index is None:
                app_arg.logger.warn(f'{current_thread().name} wrote no registration feeds, returning empty tempfile')
            else:
                app_arg.logger.debug(f'{current_thread().name} wrote {index + 1} registration feeds, returning TSV tempfile')
            return target_file

    def _generate_feed(self, row):
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
                    'description': self._withdraw_code_to_name(row['withdraw_reason']),
                },
                'type': {
                    'code': _str(row['withdraw_code']),
                    'description': self._withdraw_code_to_name(row['withdraw_code']),
                },
            }
        return feed

    @staticmethod
    def _find_last_registration(rows):
        last_registration = None

        for row in rows:
            # We prefer registration data from: 1) the current term; 2) failing that, the nearest past term; 3) failing that,
            # the nearest future term. Which is to say, skip future terms unless that's all we have.
            if (row['term_id'] > current_term_id()) and last_registration:
                continue

            # At present, terms spent as an Extension student are not included in Term GPAs (but see BOAC-2266).
            # However, if there are no other types of registration, the Extension term is used for academicCareer.
            if row['academic_career_cd'] == 'UCBX':
                if last_registration and last_registration['academic_career_cd'] != 'UCBX':
                    continue

            last_registration = row

        return last_registration

    @staticmethod
    def _withdraw_code_to_name(code):
        mappings = {
            'CAN': 'Cancelled',
            'DNSH': 'Deceased',
            'DYSH': 'Deceased',
            'MEDA': 'Medical - Approved',
            'MEDI': 'Medical - Self-Reported',
            'NPAY': 'Cancelled For Non-Payment',
            'OTHR': 'Other',
            'PARN': 'Parental Leave',
            'PERS': 'Personal',
            'RETR': 'Retroactive',
            'WDR': 'Withdrew',
        }
        return mappings.get(code) or code


def _str(v):
    return (v is not None) and (float(v) if isinstance(v, Decimal) else str(v))


def _upload_file_to_staging(table, _file):
    tsv_filename = f'staging_{table}.tsv'
    s3_key = f'{get_s3_edl_daily_path()}/{tsv_filename}'

    app.logger.info(f'Will stash {table} feeds in S3: {s3_key}')
    if not s3.upload_file(_file, s3_key):
        raise BackgroundJobError('Error on S3 upload: aborting job.')

    app.logger.info('Will copy S3 feeds into Redshift...')
    if not redshift.copy_tsv_from_s3(f"{app.config['REDSHIFT_SCHEMA_EDL']}.{table}", s3_key):
        raise BackgroundJobError('Error on Redshift copy: aborting job.')
