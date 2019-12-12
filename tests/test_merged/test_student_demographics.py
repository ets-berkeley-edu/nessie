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

from nessie.externals.sis_student_api import get_term_gpas_registration_demog
from nessie.merged.student_demographics import add_demographics_rows, ethnicity_filter_values,\
    parse_sis_demographics_api, simplified_ethnicities, simplified_gender, underrepresented_minority


class TestStudentDemographics:

    def test_api_parsing(self, app):
        raw_feed = get_term_gpas_registration_demog('11667051').get('demographics')
        parsed = parse_sis_demographics_api(raw_feed)
        assert parsed['ethnicities'] == ['White']
        assert not parsed['underrepresented']
        assert parsed['gender'] == 'Female'
        assert not parsed['visa']
        assert not parsed['nationalities']
        assert parsed['filtered_ethnicities'] == ['White']

    def test_api_visa_parsing(self, app):
        raw_feed = get_term_gpas_registration_demog('1234567890').get('demographics')
        parsed = parse_sis_demographics_api(raw_feed)
        assert parsed['ethnicities'] == ['Korean / Korean-American', 'White']
        assert parsed['visa']['status'] == 'G'
        assert parsed['visa']['type'] == 'F1'
        assert parsed['nationalities'] == ['Korea, Republic of']
        assert parsed['filtered_ethnicities'] == ['Korean / Korean-American']

    def test_add_demographics_rows(self, app):
        rows_map = {
            'demographics': [],
            'ethnicities': [],
            'visas': [],
        }
        raw_feed = get_term_gpas_registration_demog('1234567890').get('demographics')
        parsed = add_demographics_rows('1234567890', raw_feed, rows_map)
        assert not parsed.get('filtered_ethnicities')
        assert parsed['ethnicities'] == ['Korean / Korean-American', 'White']
        assert rows_map['demographics'] == [b'1234567890\tFemale\tFalse']
        assert rows_map['ethnicities'] == [b'1234567890\tKorean / Korean-American']
        assert rows_map['visas'] == [b'1234567890\tG\tF1']

    def test_simplified_ethnicities(self, app):
        assert ['African-American / Black'] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'African'},
                    'group': {'description': 'Black/African American'},
                },
                {
                    'detail': {'description': 'Not Specified'},
                    'group': {'description': 'Not Specified'},
                },
                {
                    'detail': {'description': 'African American/Black'},
                    'group': {'description': 'Black/African American'},
                },
                {
                    'detail': {'description': 'Caribbean'},
                    'group': {'description': 'Black/African American'},
                },
                {
                    'detail': {'description': 'Other African American/Black'},
                    'group': {'description': 'Black/African American'},
                },
            ],
        })
        assert ['American Indian / Alaska Native'] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'American Indian/Alaskan Native'},
                    'group': {'description': 'American Indian/Alaska Native'},
                },
                {
                    'detail': {'description': 'Native American/Alaska Native IPEDS'},
                    'group': {'description': 'American Indian/Alaska Native'},
                },
            ],
        })
        assert ['Pacific Islander'] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'Fijian'},
                    'group': {'description': 'Native Hawaiian/Oth Pac Island'},
                },
                {
                    'detail': {'description': 'Native Hawaiian/Other Pacific Islander IPEDS'},
                    'group': {'description': 'Native Hawaiian/Oth Pac Island'},
                },
                {
                    'detail': {'description': 'Samoan'},
                    'group': {'description': 'Native Hawaiian/Oth Pac Island'},
                },
                {
                    'detail': {'description': 'Tongan'},
                    'group': {'description': 'Native Hawaiian/Oth Pac Island'},
                },
                {
                    'detail': {'description': 'Guamanian/Chamorro'},
                    'group': {'description': 'Native Hawaiian/Oth Pac Island'},
                },
            ],
        })
        assert ['Mexican / Mexican-American / Chicano'] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'Mexican/Mexican American/Chicano'},
                    'group': {'description': 'Hispanic/Latino'},
                },
                {
                    'detail': {'description': 'Other Hispanic, Latin American or Spanish Origin'},
                    'group': {'description': 'Hispanic/Latino'},
                },
                {
                    'detail': {'description': 'Hispanic/Latino IPEDS'},
                    'group': {'description': 'Hispanic/Latino'},
                },
            ],
        })
        assert ['Puerto Rican'] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'Other Hispanic, Latin American or Spanish Origin'},
                    'group': {'description': 'Hispanic/Latino'},
                },
                {
                    'detail': {'description': 'Puerto Rican'},
                    'group': {'description': 'Hispanic/Latino'},
                },
                {
                    'detail': {'description': 'Hispanic/Latino IPEDS'},
                    'group': {'description': 'Hispanic/Latino'},
                },
            ],
        })
        assert ['Other Spanish-American / Latino'] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'Other Hispanic, Latin American or Spanish Origin'},
                    'group': {'description': 'Hispanic/Latino'},
                },
                {
                    'detail': {'description': 'Hispanic/Latino IPEDS'},
                    'group': {'description': 'Hispanic/Latino'},
                },
            ],
        })
        assert ['Chinese / Chinese-American'] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'Chinese'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Asian IPEDS'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Taiwanese'},
                    'group': {'description': 'Asian'},
                },
            ],
        })
        assert ['East Indian / Pakistani'] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'Asian Indian'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Pakistani'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Asian IPEDS'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Other Asian (not including Middle Eastern)'},
                    'group': {'description': 'Asian'},
                },
            ],
        })
        assert ['Filipino / Filipino-American'] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'Filipino/Filipino American'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Asian IPEDS'},
                    'group': {'description': 'Asian'},
                },
            ],
        })
        assert ['Japanese / Japanese American'] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'Japanese/Japanese American'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Asian IPEDS'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Other Asian (not including Middle Eastern)'},
                    'group': {'description': 'Asian'},
                },
            ],
        })
        assert ['Korean / Korean-American'] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'Korean'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Not Specified'},
                    'group': {'description': 'Not Specified'},
                },
                {
                    'detail': {'description': 'Asian IPEDS'},
                    'group': {'description': 'Asian'},
                },
            ],
        })
        assert ['Thai'] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'Thai'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Asian IPEDS'},
                    'group': {'description': 'Asian'},
                },
            ],
        })
        assert ['Vietnamese'] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'Vietnamese'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Asian IPEDS'},
                    'group': {'description': 'Asian'},
                },
            ],
        })
        assert ['Other Asian'] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'Bangladeshi'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Cambodian'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Indonesian'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Malaysian'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Laotian'},
                    'group': {'description': 'Asian'},
                },
            ],
        })
        assert ['White'] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'Afghan'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'Egyptian'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'European/European descent'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'Iranian'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'Israeli'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'Armenian'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'Assyrian/Chaldean'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'Georgian'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'Turkish'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'White IPEDS'},
                    'group': {'description': 'White'},
                },
            ],
        })
        assert ['Not Specified'] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'Not Specified'},
                    'group': {'description': 'Not Specified'},
                },
            ],
        })
        assert ['Not Specified'] == simplified_ethnicities({
            'ethnicities': None,
        })
        assert [
            'African-American / Black', 'American Indian / Alaska Native', 'Japanese / Japanese American',
            'Mexican / Mexican-American / Chicano', 'White',
        ] == simplified_ethnicities({
            'ethnicities': [
                {
                    'detail': {'description': 'African American/Black IPEDS'},
                    'group': {'description': 'Black/African American'},
                },
                {
                    'detail': {'description': 'African'},
                    'group': {'description': 'Black/African American'},
                },
                {
                    'detail': {'description': 'American Indian/Alaskan Native'},
                    'group': {'description': 'American Indian/Alaska Native'},
                },
                {
                    'detail': {'description': 'African American/Black'},
                    'group': {'description': 'Black/African American'},
                },
                {
                    'detail': {'description': 'European/European descent'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'Japanese/Japanese American'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Asian IPEDS'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Latin American/Latino'},
                    'group': {'description': 'Hispanic/Latino'},
                },
                {
                    'detail': {'description': 'Mexican/Mexican American/Chicano'},
                    'group': {'description': 'Hispanic/Latino'},
                },
                {
                    'detail': {'description': 'Other African American/Black'},
                    'group': {'description': 'Black/African American'},
                },
                {
                    'detail': {'description': 'Other White/Caucasian'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'Hispanic/Latino IPEDS'},
                    'group': {'description': 'Hispanic/Latino'},
                },
                {
                    'detail': {'description': 'Native American/Alaska Native IPEDS'},
                    'group': {'description': 'American Indian/Alaska Native'},
                },
                {
                    'detail': {'description': 'White IPEDS'},
                    'group': {'description': 'White'},
                },
            ],
        })

    def test_ethnicity_filter_values(self, app):
        assert [
            'African-American / Black', 'American Indian / Alaska Native', 'Japanese / Japanese American',
            'Mexican / Mexican-American / Chicano',
        ] == ethnicity_filter_values({
            'ethnicities': [
                {
                    'detail': {'description': 'African American/Black IPEDS'},
                    'group': {'description': 'Black/African American'},
                },
                {
                    'detail': {'description': 'African'},
                    'group': {'description': 'Black/African American'},
                },
                {
                    'detail': {'description': 'American Indian/Alaskan Native'},
                    'group': {'description': 'American Indian/Alaska Native'},
                },
                {
                    'detail': {'description': 'African American/Black'},
                    'group': {'description': 'Black/African American'},
                },
                {
                    'detail': {'description': 'European/European descent'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'Japanese/Japanese American'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Asian IPEDS'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Latin American/Latino'},
                    'group': {'description': 'Hispanic/Latino'},
                },
                {
                    'detail': {'description': 'Mexican/Mexican American/Chicano'},
                    'group': {'description': 'Hispanic/Latino'},
                },
                {
                    'detail': {'description': 'Other African American/Black'},
                    'group': {'description': 'Black/African American'},
                },
                {
                    'detail': {'description': 'Other White/Caucasian'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'Hispanic/Latino IPEDS'},
                    'group': {'description': 'Hispanic/Latino'},
                },
                {
                    'detail': {'description': 'Native American/Alaska Native IPEDS'},
                    'group': {'description': 'American Indian/Alaska Native'},
                },
                {
                    'detail': {'description': 'White IPEDS'},
                    'group': {'description': 'White'},
                },
            ],
        })

    def test_simplified_gender(self, app):
        assert 'Female' == simplified_gender({'gender': {'genderOfRecord': {'description': 'Female'}}})
        assert 'Male' == simplified_gender({'gender': {'genderOfRecord': {'description': 'Male'}}})
        assert 'Decline to State' == simplified_gender({'gender': {'genderOfRecord': {'description': 'Decline to State'}}})

        assert 'Female' == simplified_gender(
            {'gender': {'genderOfRecord': {'description': 'Female'}, 'genderIdentity': {'description': 'Female'}}},
        )
        assert 'Female' == simplified_gender(
            {'gender': {'genderOfRecord': {'description': 'Male'}, 'genderIdentity': {'description': 'Female'}}},
        )
        assert 'Female' == simplified_gender(
            {'gender': {'genderOfRecord': {'description': 'Male'}, 'genderIdentity': {'description': 'Trans Female/Trans Woman'}}},
        )
        assert 'Female' == simplified_gender(
            {'gender': {'genderOfRecord': {'description': 'Decline to State'}, 'genderIdentity': {'description': 'Female'}}},
        )

        assert 'Male' == simplified_gender(
            {'gender': {'genderOfRecord': {'description': 'Male'}, 'genderIdentity': {'description': 'Male'}}},
        )
        assert 'Male' == simplified_gender(
            {'gender': {'genderOfRecord': {'description': 'Female'}, 'genderIdentity': {'description': 'Male'}}},
        )
        assert 'Male' == simplified_gender(
            {'gender': {'genderOfRecord': {'description': 'Female'}, 'genderIdentity': {'description': 'Trans Male/Trans Man'}}},
        )
        assert 'Male' == simplified_gender(
            {'gender': {'genderOfRecord': {'description': 'Decline to State'}, 'genderIdentity': {'description': 'Male'}}},
        )

        assert 'Genderqueer/Gender Non-Conform' == simplified_gender({
            'gender': {
                'genderOfRecord': {'description': 'Female'},
                'genderIdentity': {'description': 'Genderqueer/Gender Non-Conform'},
            },
        })
        assert 'Different Identity' == simplified_gender(
            {'gender': {'genderOfRecord': {'description': 'Male'}, 'genderIdentity': {'description': 'Different Identity'}}},
        )

    def test_underrepresented_minority(self, app):
        assert underrepresented_minority({
            'ethnicities': [
                {
                    'detail': {'description': 'European/European descent'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'American Indian/Alaskan Native'},
                    'group': {'description': 'American Indian/Alaska Native'},
                },
                {
                    'detail': {'description': 'Chinese'},
                    'group': {'description': 'Asian'},
                },
            ],
        })
        assert underrepresented_minority({
            'ethnicities': [
                {
                    'detail': {'description': 'Asian Indian'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Caribbean'},
                    'group': {'description': 'Black/African American'},
                },
                {
                    'detail': {'description': 'European/European descent'},
                    'group': {'description': 'White'},
                },
            ],
        })
        assert underrepresented_minority({
            'ethnicities': [
                {
                    'detail': {'description': 'Chinese'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Cuban/Cuban American'},
                    'group': {'description': 'Hispanic/Latino'},
                },
            ],
        })
        assert not underrepresented_minority({
            'ethnicities': [
                {
                    'detail': {'description': 'Japanese/Japanese American'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Pakistani'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Asian IPEDS'},
                    'group': {'description': 'Asian'},
                },
            ],
        })
        assert not underrepresented_minority({
            'ethnicities': [
                {
                    'detail': {'description': 'Armenian'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'Chinese'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'European/European descent'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'Indonesian'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'Asian IPEDS'},
                    'group': {'description': 'Asian'},
                },
                {
                    'detail': {'description': 'White IPEDS'},
                    'group': {'description': 'White'},
                },
            ],
        })
        assert not underrepresented_minority({
            'ethnicities': [
                {
                    'detail': {'description': 'European/European descent'},
                    'group': {'description': 'White'},
                },
                {
                    'detail': {'description': 'Guamanian/Chamorro'},
                    'group': {'description': 'Native Hawaiian/Oth Pac Island'},
                },
                {
                    'detail': {'description': 'Iraqi'},
                    'group': {'description': 'White'},
                },
            ],
        })
        assert not underrepresented_minority({'ethnicities': None})
