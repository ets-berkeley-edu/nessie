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

from nessie.lib.util import write_to_tsv_file

UNDERREPRESENTED_GROUPS = {'Black/African American', 'Hispanic/Latino', 'American Indian/Alaska Native'}

GENDER_CODE_MAP = {
    'F': 'Female',
    'M': 'Male',
    'Q': 'Genderqueer/Gender Non-Conform',
    'TF': 'Female',
    'TM': 'Male',
    'U': 'Decline to State',
    'X': 'Different Identity',
}


def add_demographics_rows(sid, feed, feed_files, feed_counts):
    if feed:
        filtered_ethnicities = filter_ethnicities(feed.get('ethnicities', []))
        for ethn in filtered_ethnicities:
            feed_counts['ethnicities'] += write_to_tsv_file(feed_files['ethnicities'], [sid, ethn])

        feed_counts['demographics'] += write_to_tsv_file(
            feed_files['demographics'],
            [sid, feed.get('gender'), feed.get('underrepresented', False)],
        )
        visa = feed.get('visa')
        if visa:
            feed_counts['visas'] += write_to_tsv_file(feed_files['visas'], [sid, visa.get('status'), visa.get('type')])
    return feed


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


def filter_ethnicities(ethnicities):
    """Return White-only ethnicities rather than multi-ethnic values under a search for 'White'."""
    if len(ethnicities) > 1 and 'White' in ethnicities:
        return [e for e in ethnicities if e != 'White']
    else:
        return ethnicities


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
            ['East Indian / Pakistani', {'Asian Indian', 'Pakistani', 'East Indian/Pakistani'}],
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
