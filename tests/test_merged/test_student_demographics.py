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

from nessie.merged.student_demographics import ethnicity_filter_values, simplified_ethnicities, simplified_gender, \
    underrepresented_minority


class TestStudentDemographics:

    def test_simplified_ethnicities(self, app):
        assert ['African-American / Black'] == simplified_ethnicities({
            'ethnicities':
                'Black/African American : African + Not Specified : Not Specified + '
                'Black/African American : African American/Black + '
                'Black/African American : Caribbean + Black/African American : Other African American/Black',
        })
        assert ['American Indian / Alaska Native'] == simplified_ethnicities({
            'ethnicities':
                'American Indian/Alaska Native : American Indian/Alaskan Native + '
                'American Indian/Alaska Native : Native American/Alaska Native IPEDS',
        })
        assert ['Pacific Islander'] == simplified_ethnicities({
            'ethnicities':
                'Native Hawaiian/Oth Pac Island : Fijian + '
                'Native Hawaiian/Oth Pac Island : Native Hawaiian/Other Pacific Islander IPEDS + '
                'Native Hawaiian/Oth Pac Island : Samoan + '
                'Native Hawaiian/Oth Pac Island : Tongan + Native Hawaiian/Oth Pac Island : Guamanian/Chamorro',
        })
        assert ['Mexican / Mexican-American / Chicano'] == simplified_ethnicities({
            'ethnicities':
                'Hispanic/Latino : Mexican/Mexican American/Chicano + '
                'Hispanic/Latino : Other Hispanic, Latin American or Spanish Origin + '
                'Hispanic/Latino : Hispanic/Latino IPEDS',
        })
        assert ['Puerto Rican'] == simplified_ethnicities({
            'ethnicities':
                'Hispanic/Latino : Other Hispanic, Latin American or Spanish Origin + Hispanic/Latino : Puerto Rican '
                '+ Hispanic/Latino : Hispanic/Latino IPEDS',
        })
        assert ['Other Spanish-American / Latino'] == simplified_ethnicities({
            'ethnicities':
                'Hispanic/Latino : Other Hispanic, Latin American or Spanish Origin +  Hispanic/Latino : Hispanic/Latino IPEDS',
        })
        assert ['Chinese / Chinese-American'] == simplified_ethnicities({
            'ethnicities': 'Asian : Chinese + Asian : Asian IPEDS + Asian : Taiwanese',
        })
        assert ['East Indian / Pakistani'] == simplified_ethnicities({
            'ethnicities':
                'Asian : Asian Indian + Asian : Pakistani + Asian : Asian IPEDS + '
                'Asian : Other Asian (not including Middle Eastern)',
        })
        assert ['Filipino / Filipino-American'] == simplified_ethnicities({
            'ethnicities': 'Asian : Filipino/Filipino American + Asian : Asian IPEDS',
        })
        assert ['Japanese / Japanese American'] == simplified_ethnicities({
            'ethnicities': 'Asian : Japanese/Japanese American + Asian : Asian IPEDS + '
                           'Asian : Other Asian (not including Middle Eastern)',
        })
        assert ['Korean / Korean-American'] == simplified_ethnicities({
            'ethnicities': 'Asian : Korean + Not Specified : Not Specified + Asian : Asian IPEDS',
        })
        assert ['Thai'] == simplified_ethnicities({
            'ethnicities': 'Asian : Thai + Asian : Asian IPEDS',
        })
        assert ['Vietnamese'] == simplified_ethnicities({
            'ethnicities': 'Asian : Vietnamese + Asian : Asian IPEDS',
        })
        assert ['Other Asian'] == simplified_ethnicities({
            'ethnicities': 'Asian : Bangladeshi + Asian : Cambodian + Asian : Indonesian + Asian : Malaysian + Asian : Laotian',
        })
        assert ['White'] == simplified_ethnicities({
            'ethnicities':
                'White : Afghan + White : Egyptian + White : European/European descent + White : Iranian + '
                'White : Israeli + White : Armenian + White : Assyrian/Chaldean + White : Georgian + '
                'White : Turkish + White : White IPEDS',
        })
        assert ['Not Specified'] == simplified_ethnicities({
            'ethnicities': 'Not Specified : Not Specified',
        })
        assert ['Not Specified'] == simplified_ethnicities({
            'ethnicities': None,
        })
        assert [
            'African-American / Black', 'American Indian / Alaska Native', 'Japanese / Japanese American',
            'Mexican / Mexican-American / Chicano', 'White',
        ] == simplified_ethnicities({
            'ethnicities':
                'Black/African American : African American/Black IPEDS + '
                'Black/African American : African + American Indian/Alaska Native : American Indian/Alaskan Native + '
                'Black/African American : African American/Black + White : European/European descent + '
                'Asian : Japanese/Japanese American + Asian : Asian IPEDS + Hispanic/Latino : Latin American/Latino + '
                'Hispanic/Latino : Mexican/Mexican American/Chicano + '
                'Black/African American : Other African American/Black + '
                'White : Other White/Caucasian + '
                'Hispanic/Latino : Hispanic/Latino IPEDS + '
                'American Indian/Alaska Native : Native American/Alaska Native IPEDS + White : White IPEDS',
        })

    def test_ethnicity_filter_values(self, app):
        assert [
            'African-American / Black', 'American Indian / Alaska Native', 'Japanese / Japanese American',
            'Mexican / Mexican-American / Chicano',
        ] == ethnicity_filter_values({
            'ethnicities':
                'Black/African American : African American/Black IPEDS + Black/African American : African + '
                'American Indian/Alaska Native : American Indian/Alaskan Native + '
                'Black/African American : African American/Black + '
                'White : European/European descent + Asian : Japanese/Japanese American + '
                'Asian : Asian IPEDS + Hispanic/Latino : Latin American/Latino + '
                'Hispanic/Latino : Mexican/Mexican American/Chicano + '
                'Black/African American : Other African American/Black + '
                'White : Other White/Caucasian + Hispanic/Latino : Hispanic/Latino IPEDS + '
                'American Indian/Alaska Native : Native American/Alaska Native IPEDS + White : White IPEDS',
        })

    def test_simplified_gender(self, app):
        assert 'Female' == simplified_gender({'gender_of_record': 'Female', 'gender_identity': None})
        assert 'Male' == simplified_gender({'gender_of_record': 'Male', 'gender_identity': None})
        assert 'Decline to State' == simplified_gender(
            {'gender_of_record': 'Decline to State', 'gender_identity': None},
        )

        assert 'Female' == simplified_gender({'gender_of_record': 'Female', 'gender_identity': 'Female'})
        assert 'Female' == simplified_gender({'gender_of_record': 'Male', 'gender_identity': 'Female'})
        assert 'Female' == simplified_gender(
            {'gender_of_record': 'Male', 'gender_identity': 'Trans Female/Trans Woman'},
        )
        assert 'Female' == simplified_gender({'gender_of_record': 'Decline to State', 'gender_identity': 'Female'})

        assert 'Male' == simplified_gender({'gender_of_record': 'Male', 'gender_identity': 'Male'})
        assert 'Male' == simplified_gender({'gender_of_record': 'Female', 'gender_identity': 'Male'})
        assert 'Male' == simplified_gender({'gender_of_record': 'Female', 'gender_identity': 'Trans Male/Trans Man'})
        assert 'Male' == simplified_gender({'gender_of_record': 'Decline to State', 'gender_identity': 'Male'})

        assert 'Genderqueer/Gender Non-Conform' == simplified_gender(
            {'gender_of_record': 'Female', 'gender_identity': 'Genderqueer/Gender Non-Conform'})
        assert 'Different Identity' == simplified_gender(
            {'gender_of_record': 'Male', 'gender_identity': 'Different Identity'})

    def test_underrepresented_minority(self, app):
        assert underrepresented_minority({
            'ethnicities':
                'White : European/European descent + American Indian/Alaska Native : American Indian/Alaskan Native + '
                'Asian : Chinese',
        })
        assert underrepresented_minority({
            'ethnicities':
                'Asian : Asian Indian + Black/African American : Caribbean + White : European/European descent',
        })
        assert underrepresented_minority({'ethnicities': 'Asian : Chinese + Hispanic/Latino : Cuban/Cuban American'})
        assert not underrepresented_minority(
            {'ethnicities': 'Asian : Japanese/Japanese American + Asian : Pakistani + Asian : Asian IPEDS'},
        )
        assert not underrepresented_minority({
            'ethnicities':
                'White : Armenian + Asian : Chinese + White : European/European descent + Asian : Indonesian + '
                'Asian : Asian IPEDS + White : White IPEDS',
        })
        assert not underrepresented_minority({
            'ethnicities':
                'White : European/European descent + Native Hawaiian/Oth Pac Island : Guamanian/Chamorro + White : Iraqi',
        })
        assert not underrepresented_minority({'ethnicities': None})
