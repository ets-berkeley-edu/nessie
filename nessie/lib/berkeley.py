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

from decimal import Decimal
import re
import threading

from flask import current_app as app
from nessie.externals import rds
from nessie.externals.b_connected import BConnected

"""A utility module collecting logic specific to the Berkeley campus."""

# This is not a complete mapping:
#  - Not all SIS-defined academic plans map to a single Degree Programs page.
#  - Not all Degree Programs pages map to a single SIS-defined academic plan.
#  - An unknown number of obsolete academic plan descriptions are still active
#    from before the CS era.
ACADEMIC_PLAN_TO_DEGREE_PROGRAM_PAGE = {
    'African American Studies': 'african-american-studies',
    'American Studies': 'american-studies',
    'Anthropology': 'anthropology',
    'Applied Mathematics': 'applied-mathematics',
    'Architecture': 'architecture',
    'Art': 'art-practice',
    'Asian Am & Asian Diasp': 'asian-american-diaspora-studies',
    'Asian Studies': 'asian-studies-multi-area',
    'Astrophysics': 'astrophysics',
    r'BioE/MSE Joint Major': 'bioengineering-materials-science-engineering-joint-major',
    'Bioengineering': 'bioengineering',
    'Buddhist Studies': 'buddhism',
    'Business Administration': 'business-administration',
    'Celtic Studies': 'celtic-studies',
    r'Chem Eng/MSE Joint Major': 'chemical-engineering-materials-science-joint-major',
    r'Chem Eng/NE Joint Major': 'chemical-engineering-nuclear-joint-major',
    'Chemical Biology': 'chemical-biology',
    'Chemical Engineering': 'chemical-engineering',
    'Chemistry': 'chemistry',
    'Chicano Studies': 'chicano-latino-studies',
    'Chinese Language': 'chinese-language',
    'City & Regional Planning': 'city-planning',
    'Civil & Environmental Eng': 'environmental-engineering',
    'Civil Engineering': 'civil-engineering',
    'Classical Civilizations': 'classical-civilizations',
    'Classical Languages': 'classical-languages',
    'Cognitive Science': 'cognitive-science',
    'Comparative Literature': 'comparative-literature',
    'Computer Science': 'computer-science',
    'Conserv & Resource Stds': 'conservation-resource-studies',
    'Dance & Perf Studies': 'dance-performance-studies',
    'Demography': 'demography',
    'Development Studies': 'development-studies',
    'Dutch Studies': 'dutch-studies',
    'Earth & Planetary Science': 'earth-planetary-science',
    'Economics': 'economics',
    'Education': 'education',
    'Electrical Eng & Comp Sci': 'electrical-engineering-computer-sciences',
    'Energy & Resources': 'energy-resources',
    'Energy Engineering': 'energy-engineering',
    'Eng Math & Statistics': 'engineering-math-statistics',
    'Engineering Physics': 'engineering-physics',
    'English': 'english',
    'Environ Econ & Policy': 'environmental-economics-policy',
    'Environmental Eng Science': 'environmental-engineering-science',
    'Environmental Sciences': 'environmental-sciences',
    'Ethnic Studies': 'ethnic-studies',
    'Film': 'film',
    'Forestry & Natural Res': 'forestry-natural-resources',
    'French': 'french',
    'Gender & Womens Studies': 'gender-womens-studies',
    'Genetics & Plant Biology': 'genetics-plant-biology',
    'Geography': 'geography',
    'Geology': 'geology',
    'Geophysics': 'geophysics',
    'German': 'german',
    'Global Studies': 'global-studies',
    'Greek': 'greek',
    'Hispanic Lang': 'hispanic-languages-linguistics-bilingualism',
    'History of Art': 'art-history',
    'History': 'history',
    'Industrial Eng & Ops Rsch': 'industrial-engineering-operations-research',
    'Integrative Biology': 'integrative-biology',
    'Interdisciplinary Studies': 'interdisciplinary-studies',
    'Italian': 'italian-studies',
    'Japanese Language': 'japanese-language',
    'Jewish Studies': 'jewish-studies',
    'Journalism': 'journalism',
    'Landscape Architecture': 'landscape-architecture',
    'Latin American Studies': 'latin-american-studies',
    'Latin': 'latin',
    'Legal Studies': 'legal-studies',
    'Linguistics': 'linguistics',
    'Materials Science & Eng': 'materials-science-engineering',
    'Mathematics': 'mathematics',
    'MCB-Biochem & Mol Biol': 'molecular-cell-biology-biochemistry',
    'MCB-Cell & Dev Biology': 'molecular-cell-biology-developmental',
    'MCB-Neurobiology': 'molecular-cell-biology-neurobiology',
    r'ME/NE Joint Major': 'mechanical-engineering-nuclear',
    'Mechanical Engineering': 'mechanical-engineering',
    'Media Studies': 'media-studies',
    'Medieval Studies': 'medieval-studies',
    'Microbial Biology': 'microbial-biology',
    'Middle Eastern Studies': 'middle-eastern-studies',
    'Molecular Environ Biology': 'molecular-environmental-biology',
    r'MSE/ME Joint Major': 'materials-science-engineering-mechanical-joint-major',
    r'MSE/NE Joint Major': 'materials-science-engineering-nuclear-joint-major',
    'Music': 'music',
    'Native American Studies': 'native-american-studies',
    'Near Eastern Studies': 'near-eastern-civilizations',
    'Nuclear Engineering': 'nuclear-engineering',
    'Nut Sci-Physio & Metabol': 'nutritional-science',
    'Nutritional Sci-Toxicology': 'nutritional-science',
    'Nutritional Science': 'nutritional-science',
    'Peace & Conflict Studies': 'peace-conflict-studies',
    'Philosophy': 'philosophy',
    'Physics': 'physics',
    'Political Economy': 'political-economy',
    'Political Science': 'political-science',
    'Psychology': 'psychology',
    'Public Health': 'public-health',
    'Public Policy': 'public-policy',
    'Religious Studies': 'religious-studies',
    'Rhetoric': 'rhetoric',
    'Scandinavian': 'scandinavian',
    'Science & Math Education': 'science-math-education',
    'Slavic Lang & Lit': 'czech-polish-bosnian-croatian-serbian-language-literature',
    'Social Welfare': 'socialwelfare',
    'Society and Environment': 'society-environment',
    'Sociology': 'sociology',
    'South & SE Asian Studies': 'south-southeast-asian-studies',
    'Span-Spanish Lang & Lit': 'languages-literatures-cultures-spanish-speaking-world',
    'Spanish': 'languages-literatures-cultures-spanish-speaking-world',
    'Statistics': 'statistics',
    'Sustainable Environ Dsgn': 'sustainable-environmental-design',
    'Theater & Perf Studies': 'theater-performance-studies',
    'Urban Studies': 'urban-studies',
}


cache_thread = threading.local()


def canvas_terms():
    sis_term_ids = reverse_term_ids()
    return [term_name_for_sis_id(sid_id) for sid_id in sis_term_ids]


def career_code_to_name(code):
    return {
        'UGRD': 'UNDERGRAD',
        'UCBX': 'EXTENSION',
        'GRAD': 'GRADUATE',
    }.get(code)


def edl_demographics_to_json(row):
    # TODO: Extract demographics from query results
    return {
        'ethnicities': [
            {
                'detail': {
                    'code': 'EUROPEAN',
                    'description': 'European/European descent',
                },
                'group': {
                    'code': '1',
                    'description': 'White',
                },
                'hispanicLatino': False,
            },
            {
                'detail': {
                    'code': 'IPEDSWHT',
                    'description': 'White IPEDS',
                },
                'group': {
                    'code': '1',
                    'description': 'White',
                },
                'hispanicLatino': False,
            },
        ],
        'gender': {
            'discloseGenderIdentity': False,
            'discloseGenderOfRecord': True,
            'discloseSexAtBirth': False,
            'fromDate': '2019-08-21',
            'genderIdentity': {
                'code': 'TF',
                'description': 'Trans Female/Trans Woman',
            },
            'genderOfRecord': {
                'code': 'F',
                'description': 'Female',
            },
            'sexAtBirth': {
                'code': 'M',
                'description': 'Male',
            },
        },
        'residency': {
            'countryCode': 'USA',
            'financialAid': {
                'code': 'RES',
                'description': 'Resident',
            },
            'fromDate': '2016-07-14',
            'fromTerm': {
                'academicYear': '2017',
                'beginDate': '2016-08-17',
                'category': {
                    'code': 'R',
                    'description': 'Regular Term',
                },
                'endDate': '2016-12-16',
                'id': '2168',
                'name': '2016 Fall',
            },
            'official': {
                'code': 'RES',
                'description': 'Resident',
            },
            'source': {
                'code': 'Official',
            },
            'statementOfLegalResidenceStatus': {
                'code': 'Y',
                'description': 'Residency Determined',
            },
            'tuition': {
                'code': 'RES',
                'description': 'Resident',
            },
        },
        'usaCountry': {
            'citizenshipStatus': {
                'code': '1',
                'description': 'Native',
            },
            'militaryStatus': {},
            'passport': {},
            'visa': {},
        },
    }


def edl_registration_to_json(row):
    def _edl_career_code_to_name(code):
        return {
            'UGRD': 'Undergraduate',
            'UCBX': 'Extension',
            'GRAD': 'Graduate',
        }.get(code)

    term_id = row['term_id']
    season, year = term_info_for_sis_term_id(term_id)
    career_code = row['academic_career_cd']
    # TODO: From EDL query results, what do we do with 'total_cumulative_gpa_nbr'?
    # TODO: All 'None' entries below need investigation. Does EDL provide?
    return {
        'loadedAt': str(row['edl_load_date']),
        'term': {
            'id': term_id,
            'name': f'{year} {season}',
            'category': {
                'code': None,  # TODO: SIS API has it. Do we care?
                'description': None,
            },
            'academicYear': _str(row['acad_year']),
            'beginDate': row['enrl_on_trans_dt'].strftime('%Y-%m-%d'),
            'endDate': row['fully_graded_dt'].strftime('%Y-%m-%d'),
        },
        'academicCareer': {
            'code': _str(career_code),
            'description': _edl_career_code_to_name(career_code),
        },
        'eligibleToRegister': _flag_to_bool(row['eligible_to_enroll_flag']),
        'eligibilityStatus': {
            'code': _str(row['registrn_eligibility_status_cd']),  # TODO: SIS API and EDL have diff values.
            'description': row['eligibility_status_desc'],
        },
        'registered': _flag_to_bool(row['registered_flag']),
        'disabled': None,  # TODO: SIS API has it. Do we care?
        'athlete': None,  # TODO: SIS API has it. Do we care?
        'intendsToGraduate': _flag_to_bool(row['intends_to_graduate_flag']),
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
            # What to do with row['units_term_enrolled']?
            {
                'type': {
                    'code': 'Total',
                    'description': 'Total Units',
                },
                'unitsCumulative': None,
                'unitsEnrolled': None,
                'unitsIncomplete': None,
                'unitsMax': _str(row['units_term_enrollment_max']),
                'unitsMin': _str(row['units_term_enrollment_min']),
                'unitsOther': None,
                'unitsPassed': _str(row['unt_passd_fa']),
                'unitsTaken': _str(row['unt_taken_fa']),
                'unitsTest': _str(row['tot_test_credit']),
                'unitsTransferAccepted': None,
                'unitsTransferEarned': None,
                'unitsWaitlisted': None,
            },
            {
                'type': {
                    'code': 'For GPA',
                    'description': 'Units For GPA',
                },
                'unitsEnrolled': _str(row['tot_inprog_gpa']),
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
                'unitsEnrolled': _str(row['tot_inprog_nogpa']),
                'unitsIncomplete': None,
                'unitsMax': _str(row['max_nogpa_unit']),  # TODO: SIS API and EDL have diff values. Do we care?
                'unitsMin': None,
                'unitsOther': None,
                'unitsPassed': _str(row['unt_passd_nogpa']),
                'unitsTaken': _str(row['unt_taken_nogpa']),
                'unitsTest': None,  # TODO: SIS API has it. Do we care?
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
                'code': _str(row['withdraw_code']),
                'description': _withdraw_code_to_name(row['withdraw_code']),
            },
        },
    }


def feature_flag_edl():
    return app.config['FEATURE_FLAG_ENTERPRISE_DATA_LAKE']


def future_term_ids():
    return _collect_terms(start_term_id=future_term_id(), stop_term_id=current_term_id(), include_stop=False)


def legacy_term_ids():
    return _collect_terms(start_term_id=earliest_term_id(), stop_term_id=earliest_legacy_term_id(), include_start=False)


def next_term_id(term_id):
    if term_id[3] == '8':
        next_id = term_id[:1] + str(int(term_id[1:3]) + 1).zfill(2) + '2'
    elif term_id[3] == '5':
        next_id = term_id[:3] + '8'
    elif term_id[3] == '2':
        next_id = term_id[:3] + '5'
    return next_id


def previous_term_id(term_id):
    if term_id[3] == '8':
        previous = term_id[:3] + '5'
    elif term_id[3] == '5':
        previous = term_id[:3] + '2'
    elif term_id[3] == '2':
        year_code = str(int(term_id[:1]) - 1) + '99' if term_id[1:3] == '00' else term_id[:1] + str(int(term_id[1:3]) - 1).zfill(2)
        previous = year_code + '8'
    return previous


def reverse_term_ids(include_future_terms=False, include_legacy_terms=False):
    stop_term_id = sis_term_id_for_name(app.config['EARLIEST_LEGACY_TERM']) if include_legacy_terms \
        else sis_term_id_for_name(app.config['EARLIEST_TERM'])
    start_term_id = future_term_id() if include_future_terms else current_term_id()
    return _collect_terms(start_term_id, stop_term_id)


def send_system_error_email(message, subject=None):
    if subject is None:
        subject = f'{message[:50]}...' if len(message) > 50 else message
    config_value = app.config['EMAIL_SYSTEM_ERRORS_TO']
    email_addresses = config_value if isinstance(config_value, list) else [config_value]
    for email_address in email_addresses:
        BConnected().send(
            message=message,
            recipient={
                'email': email_address,
                'name': 'Nessie',
                'uid': '0',
            },
            subject_line=f'Alert: {subject}',
        )


def sis_term_id_for_name(term_name=None):
    if term_name:
        match = re.match(r'\A(Spring|Summer|Fall|Winter) (\d)[09](\d{2})\Z', term_name)
        if match:
            season_codes = {
                'Spring': '2',
                'Summer': '5',
                'Fall': '8',
                'Winter': '0',
            }
            return match.group(2) + match.group(3) + season_codes[match.group(1)]


def term_info_for_sis_term_id(sis_id=None):
    if sis_id:
        sis_id = str(sis_id)
        season_codes = {
            '2': 'Spring',
            '5': 'Summer',
            '8': 'Fall',
            '0': 'Winter',
        }
        year = f'19{sis_id[1:3]}' if sis_id.startswith('1') else f'20{sis_id[1:3]}'
        return season_codes[sis_id[3:4]], year


def term_name_for_sis_id(sis_id=None):
    if sis_id:
        season, year = term_info_for_sis_term_id(sis_id)
        return f'{season} {year}'


def degree_program_url_for_major(plan_description):
    matched = next(
        (k for k in ACADEMIC_PLAN_TO_DEGREE_PROGRAM_PAGE.keys() if re.match(r'^' + re.escape(k) + r' (BA|BS)', plan_description)),
        None,
    )
    if matched:
        return f'http://guide.berkeley.edu/undergraduate/degree-programs/{ACADEMIC_PLAN_TO_DEGREE_PROGRAM_PAGE[matched]}/'
    else:
        return None


def get_config_terms():
    config_terms = getattr(cache_thread, 'config_terms', None)
    if not config_terms:
        current_term_name = app.config['CURRENT_TERM']
        future_term_name = app.config['FUTURE_TERM']
        s3_canvas_data_path_current_term = app.config['LOCH_S3_CANVAS_DATA_PATH_CURRENT_TERM']
        if 'auto' in [current_term_name, future_term_name, s3_canvas_data_path_current_term]:
            default_terms = get_default_terms()
        if current_term_name == 'auto':
            current_term_name = default_terms['current_term_name']
        current_term_id = sis_term_id_for_name(current_term_name)
        if future_term_name == 'auto':
            future_term_name = default_terms['future_term_name']
        future_term_id = sis_term_id_for_name(future_term_name)
        if s3_canvas_data_path_current_term == 'auto':
            s3_canvas_data_path_current_term = default_terms['s3_canvas_data_path_current_term']
        config_terms = {
            'current_term_name': current_term_name,
            'current_term_id': current_term_id,
            'future_term_name': future_term_name,
            'future_term_id': future_term_id,
            's3_canvas_data_path_current_term': s3_canvas_data_path_current_term,
        }
        cache_thread.config_terms = config_terms
    return config_terms


def get_default_terms():
    current_term_index = get_current_term_index()
    canvas_data_current_term = '/term/' + current_term_index['current_term_name'].lower().replace(' ', '-')
    current_term_index['s3_canvas_data_path_current_term'] = app.config['LOCH_S3_CANVAS_DATA_PATH'] + canvas_data_current_term
    return current_term_index


def get_current_term_index():
    sis_rds_schema = app.config['RDS_SCHEMA_SIS_TERMS']
    sql = f'SELECT * FROM {sis_rds_schema}.current_term_index LIMIT 1'
    rows = rds.fetch(sql)
    return rows and rows[0]


def current_term_id():
    return get_config_terms()['current_term_id']


def current_term_name():
    return get_config_terms()['current_term_name']


def future_term_id():
    return get_config_terms()['future_term_id']


def s3_canvas_data_path_current_term():
    return get_config_terms()['s3_canvas_data_path_current_term']


def earliest_term_id():
    term_name = app.config['EARLIEST_TERM']
    return sis_term_id_for_name(term_name)


def earliest_legacy_term_id():
    term_name = app.config['EARLIEST_LEGACY_TERM']
    return sis_term_id_for_name(term_name)


def translate_grading_basis(code):
    bases = {
        'CNC': 'C/NC',
        'DPN': 'DPN',
        'EPN': 'EPN',
        'ESU': 'ESU',
        'GRD': 'Letter',
        'LAW': 'Law',
        'PNP': 'P/NP',
        'SUS': 'S/U',
    }
    return bases.get(code) or code


def _collect_terms(start_term_id, stop_term_id, include_start=True, include_stop=True):
    term_ids = []
    term_id = start_term_id if include_start else previous_term_id(start_term_id)
    stop_term_id = previous_term_id(stop_term_id) if include_stop else stop_term_id
    while True:
        if term_id <= stop_term_id:
            return term_ids
        term_ids.append(term_id)
        term_id = previous_term_id(term_id)


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


def _flag_to_bool(v):
    return v and v.upper() == 'Y'


def _str(v):
    return (v is not None) and (float(v) if isinstance(v, Decimal) else str(v))
