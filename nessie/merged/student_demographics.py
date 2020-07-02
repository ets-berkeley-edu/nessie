"""
Copyright ©2020. The Regents of the University of California (Regents). All Rights Reserved.

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

from nessie.lib.util import write_to_tsv_file

UNDERREPRESENTED_GROUPS = {'Black/African American', 'Hispanic/Latino', 'American Indian/Alaska Native'}


def add_demographics_rows(sid, feed, feed_files, feed_counts):
    parsed = parse_sis_demographics_api(feed)
    if parsed:
        filtered_ethnicities = parsed.pop('filtered_ethnicities', [])
        for ethn in filtered_ethnicities:
            feed_counts['ethnicities'] += write_to_tsv_file(feed_files['ethnicities'], [sid, ethn])

        feed_counts['demographics'] += write_to_tsv_file(
            feed_files['demographics'],
            [sid, parsed.get('gender'), parsed.get('underrepresented', False)],
        )
        visa = parsed.get('visa')
        if visa:
            feed_counts['visas'] += write_to_tsv_file(feed_files['visas'], [sid, visa.get('status'), visa.get('type')])
    return parsed


def refresh_rds_demographics(rds_schema, rds_dblink_to_redshift, redshift_schema, transaction):
    if not transaction.execute(f'TRUNCATE {rds_schema}.demographics'):
        return False
    sql = f"""INSERT INTO {rds_schema}.demographics (
            SELECT *
            FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
                SELECT sid, gender, minority
                FROM {redshift_schema}.demographics
              $REDSHIFT$)
            AS redshift_demographics (
                sid VARCHAR,
                gender VARCHAR,
                minority BOOLEAN
            ));"""
    if not transaction.execute(sql):
        return False
    if not transaction.execute(f'TRUNCATE {rds_schema}.ethnicities'):
        return False
    sql = f"""INSERT INTO {rds_schema}.ethnicities (
            SELECT *
            FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
                SELECT sid, ethnicity
                FROM {redshift_schema}.ethnicities
              $REDSHIFT$)
            AS redshift_ethnicities (
                sid VARCHAR,
                ethnicity VARCHAR
            ));"""
    if not transaction.execute(sql):
        return False
    if not transaction.execute(f'TRUNCATE {rds_schema}.visas'):
        return False
    sql = f"""INSERT INTO {rds_schema}.visas (
            SELECT *
            FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
                SELECT sid, visa_status, visa_type
                FROM {redshift_schema}.visas
              $REDSHIFT$)
            AS redshift_visas (
                sid VARCHAR,
                visa_status VARCHAR,
                visa_type VARCHAR
            ));"""
    if not transaction.execute(sql):
        return False
    return True


def parse_sis_demographics_api(feed):
    if not feed:
        return False
    return {
        'gender': simplified_gender(feed),
        'underrepresented': underrepresented_minority(feed),
        'ethnicities': simplified_ethnicities(feed),
        'visa': simplified_visa(feed),
        'nationalities': simplified_countries(feed),
        'filtered_ethnicities': ethnicity_filter_values(feed),
    }


def simplified_visa(feed):
    visa_status = feed.get('usaCountry', {}).get('visa', {}).get('status')
    if visa_status not in {'G', 'A'}:
        return None
    visa_type = feed['usaCountry']['visa'].get('type', {}).get('code')
    return {
        'status': visa_status,
        'type': visa_type,
    }


def simplified_countries(feed):
    return [c.get('description') for c in feed.get('foreignCountries', [])]


def ethnicity_filter_values(feed):
    """Return ethnicities found by search filters.

    Most notably, a search for White should find White-only ethnicities rather than multi-ethnic values.
    """
    values = simplified_ethnicities(feed)
    if len(values) > 1 and 'White' in values:
        values.remove('White')
    return values


def simplified_ethnicities(feed):
    """Reduce SIS's 66 possible ethnicities to more or less the CoE list."""
    simpler_list = []
    ethnicities = feed.get('ethnicities')
    if not ethnicities:
        return ['Not Specified']
    ethnic_map = defaultdict(set)
    for eth in ethnicities:
        group = eth.get('group', {}).get('description')
        detail = eth.get('detail', {}).get('description')
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


def simplified_gender(feed):
    """Prefer gender_identity over gender_of_record. Do not display trans status."""
    gender_data = feed.get('gender')
    if not gender_data:
        return
    gender_identity = gender_data.get('genderIdentity', {}).get('description')
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
        return gender_data.get('genderOfRecord', {}).get('description')


def underrepresented_minority(feed):
    """Check ethnic_group against the list of underrepresented groups.

    Official descriptions have some ambiguity, which is reflected in disagreements between our data sources.
    - Should 'Hispanic/Latino' students from South America be included, or should the flag be restricted to
      'Mexican/Mexican American/Chicano'?
    - Should 'African-American / Black' students with a visa from an African country be included?
    """
    ethnicities = feed.get('ethnicities')
    if not ethnicities:
        return False
    groups = {eth.get('group', {}).get('description') for eth in ethnicities}
    return not UNDERREPRESENTED_GROUPS.isdisjoint(groups)
