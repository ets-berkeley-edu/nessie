"""
Copyright ©2019. The Regents of the University of California (Regents). All Rights Reserved.

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

from collections import defaultdict

from nessie.lib import queries

UNDERREPRESENTED_GROUPS = {'Black/African American', 'Hispanic/Latino', 'American Indian/Alaska Native'}


def generate_demographics_data():
    intermediate_rows = queries.get_advisee_sis_demographics()
    parsed_rows = [parse_sis_demographic_data(row) for row in intermediate_rows]
    return parsed_rows


def parse_sis_demographic_data(student_row):
    sid = student_row['sid']
    return {
        'sid': sid,
        'feed': {
            'gender': simplified_gender(student_row),
            'underrepresented': underrepresented_minority(student_row),
            'ethnicities': simplified_ethnicities(student_row),
            'visa_types': student_row['visas'],
            'nationalities': student_row['countries'] and student_row['countries'].split(' + '),
        },
        'filtered_ethnicities': ethnicity_filter_values(student_row),
    }


def ethnicity_filter_values(row):
    """Return ethnicities found by search filters.

    Most notably, a search for White should find White-only ethnicities rather than multi-ethnic values.
    """
    values = simplified_ethnicities(row)
    if len(values) > 1 and 'White' in values:
        values.remove('White')
    return values


def simplified_ethnicities(row):
    """Reduce SIS's 66 possible ethnicities to more or less the CoE list."""
    simpler_list = []
    ethnicities = row['ethnicities']
    if not ethnicities:
        return ['Not Specified']
    ethnic_map = defaultdict(set)
    for group, detail in [eth.split(' : ') for eth in ethnicities.split(' + ')]:
        ethnic_map[group].add(detail)
    for group in ethnic_map.keys():
        merge_from_details(simpler_list, group, ethnic_map[group])
    if not simpler_list:
        simpler_list.append('Not Specified')
    return sorted(simpler_list)


def merge_from_details(simpler_list, group, details):
    direct_group_mapping = {
        'Black/African American': 'African-American / Black',
        'American Indian/Alaska Native': 'American Indian / Alaska Native',
        'Native Hawaiian/Oth Pac Island': 'Pacific Islander',
        'White': 'White',
    }
    subsets_mapping = {
        'Hispanic/Latino': [
            ['Mexican / Mexican-American / Chicano', {'Mexican/Mexican American/Chicano'}],
            ['Puerto Rican', {'Puerto Rican'}],
            ['Other Spanish-American / Latino', {'default'}],
        ],
        'Asian': [
            ['East Indian / Pakistani', {'Asian Indian', 'Pakistani'}],
            ['Chinese / Chinese-American', {'Chinese', 'Taiwanese'}],
            ['Filipino / Filipino-American', {'Filipino/Filipino American'}],
            ['Japanese / Japanese American', {'Japanese/Japanese American'}],
            ['Thai', {'Thai'}],
            ['Vietnamese', {'Vietnamese'}],
            ['Korean / Korean-American', {'Korean'}],
            ['Other Asian', {'default'}],
        ],
    }
    if group in direct_group_mapping:
        simpler_list.append(direct_group_mapping[group])
    elif group in subsets_mapping:
        group_set = set()
        for (label, matching) in subsets_mapping[group]:
            if not details.isdisjoint(matching):
                group_set.add(label)
            elif matching == {'default'} and not group_set:
                group_set.add(label)
        simpler_list += group_set


def simplified_gender(row):
    """Prefer gender_identity over gender_of_record. Do not display trans status."""
    gender_identity = row['gender_identity']
    if gender_identity:
        if gender_identity.startswith('Trans Female'):
            return 'Female'
        elif gender_identity.startswith('Trans Male'):
            return 'Male'
        else:
            # Other potential values include 'Genderqueer/Gender Non-Conform' and 'Different Identity'.
            return gender_identity
    else:
        # 'Female', 'Male', or 'Decline to State'.
        return row['gender_of_record']


def underrepresented_minority(row):
    """Check ethnic_group against the list of underrepresented groups.

    Official descriptions have some ambiguity, which is reflected in disagreements between our data sources.
    - Should 'Hispanic/Latino' students from South America be included, or should the flag be restricted to
      'Mexican/Mexican American/Chicano'?
    - Should 'African-American / Black' students with a visa from an African country be included?
    """
    ethnicities = row['ethnicities']
    if not ethnicities:
        return False
    groups = {eth.split(' : ')[0] for eth in ethnicities.split(' + ')}
    return not UNDERREPRESENTED_GROUPS.isdisjoint(groups)
