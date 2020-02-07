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


import csv
import random

from faker import Faker

RECORD_COUNT = 20000
faker = Faker()


def create_csv_file():
    with open('faker_admissions_dataset.csv', 'w', newline='') as csvfile:

        fieldnames = [
            'applyuc_cpid',
            'cs_empl_id',
            'freshman_or_transfer',
            'admit_status',
            'current_sir',
            'college',
            'first_name',
            'last_name',
            'birthdate',
            'email',
            'daytime',
            'mobile',
            'permanent_street_1',
            'permanent_street_2',
            'permanent_city',
            'permanent_region',
            'permanent_postal',
            'permanent_country',
            'sex',
            'gender_identity',
            'xethnic',
            'hispanic',
            'urem',
            'first_generation_student',
            'first_generation_college',
            'parent_1_education_level',
            'parent_2_education_level',
            'hs_unweighted_gpa',
            'hs_weighted_gpa',
            'transfer_gpa',
            'act_composite',
            'act_math',
            'act_english',
            'act_reading',
            'act_writing',
            'sat_total',
            'sat_r_evidence_based_rw_section',
            'sat_r_math_section',
            'sat_r_essay_reading',
            'sat_r_essay_analysis',
            'sat_r_essay_writing',
            'application_fee_waiver_flag',
            'foster_care_flag',
            'family_is_single_parent',
            'student_is_single_parent',
            'family_dependents_num',
            'student_dependents_num',
            'family_income',
            'student_income',
            'is_military_dependent',
            'military_status',
            'reentry_status',
            'athlete_status',
            'summer_bridge_status',
            'last_school_lcff_plus_flag',
            'special_program_cep',
        ]

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        status_flags = ['Yes', 'No', '']
        boolean_flags = ['T', 'F', '']
        sex_flags = ['M', 'F', '']
        gender_id_flags = ['Male', 'Female', 'Not Specified', 'Other', '']
        education_level = ['CollegeAttended', 'HighSchoolGraduate', 'AssociateDegree', 'BachelorDegree', 'MasterDegree', 'DoctoralDegree', '']
        college = [
            'College of Letters and Science', 'College of Engineering', 'College of Natural Reources', 'College of Business',
            'College of Chemistry', 'College of Environmental Design', '',
        ]
        freshman_flag = ['Freshman', 'Transfer', '']
        ethnic = ['AfricanAmericanBlack', 'White', 'Asian', 'PacificIslander', 'EastIndian', 'NativeAmerican', 'LatinX', 'NotSpecified', 'Other', '']

        writer.writeheader()
        for i in range(RECORD_COUNT):
            writer.writerow(
                {
                    'applyuc_cpid': faker.ean8(),
                    'cs_empl_id': faker.ean8(),
                    'freshman_or_transfer': faker.words(1, freshman_flag, True)[0],
                    'admit_status': faker.words(1, status_flags, True)[0],
                    'current_sir': faker.words(1, status_flags, True)[0],
                    'college': faker.words(1, college, True)[0],
                    'first_name': faker.first_name(),
                    'last_name': faker.last_name(),
                    'birthdate': faker.date_of_birth(tzinfo=None, minimum_age=17, maximum_age=35),
                    'email': faker.email(),
                    'daytime': faker.phone_number(),
                    'mobile': faker.phone_number(),
                    'permanent_street_1': faker.street_address(),
                    'permanent_street_2': faker.secondary_address(),
                    'permanent_city': faker.city(),
                    'permanent_region': faker.state_abbr(),
                    'permanent_postal': faker.zipcode(),
                    'permanent_country': faker.bank_country(),
                    'sex': faker.words(1, sex_flags, True)[0],
                    'gender_identity': faker.words(1, gender_id_flags, True)[0],
                    'xethnic': faker.words(1, ethnic, True)[0],
                    'hispanic': faker.words(1, boolean_flags, True)[0],
                    'urem': faker.words(1, status_flags, True)[0],
                    'first_generation_student': faker.words(1, boolean_flags, True)[0],
                    'first_generation_college': faker.words(1, boolean_flags, True)[0],
                    'parent_1_education_level': faker.words(1, education_level, True)[0],
                    'parent_2_education_level': faker.words(1, education_level, True)[0],
                    'hs_unweighted_gpa': round(random.uniform(0.0, 4.0), 2),
                    'hs_weighted_gpa': round(random.uniform(0.0, 4.0), 2),
                    'transfer_gpa': round(random.uniform(0.0, 4.0), 2),
                    'act_composite': round(random.uniform(1.0, 36.0), 2),
                    'act_math': round(random.uniform(1.0, 36.0), 2),
                    'act_english': round(random.uniform(1.0, 36.0), 2),
                    'act_reading': round(random.uniform(1.0, 36.0), 2),
                    'act_writing': round(random.uniform(1.0, 36.0), 2),
                    'sat_total': faker.random_int(400, 1600),
                    'sat_r_evidence_based_rw_section': faker.random_int(200, 800),
                    'sat_r_math_section': faker.random_int(200, 800),
                    'sat_r_essay_reading': faker.random_int(2, 8),
                    'sat_r_essay_analysis': faker.random_int(2, 8),
                    'sat_r_essay_writing': faker.random_int(2, 8),
                    'application_fee_waiver_flag': faker.words(1, status_flags, True)[0],
                    'foster_care_flag': faker.words(1, status_flags, True)[0],
                    'family_is_single_parent': faker.words(1, status_flags, True)[0],
                    'student_is_single_parent': faker.words(1, status_flags, True)[0],
                    'family_dependents_num': faker.random_int(0, 5),
                    'student_dependents_num': faker.random_int(0, 5),
                    'family_income': faker.random_int(10000, 100000),
                    'student_income': faker.random_int(0, 1000),
                    'is_military_dependent': faker.words(1, status_flags, True)[0],
                    'military_status': faker.words(1, status_flags, True)[0],
                    'reentry_status': faker.words(1, status_flags, True)[0],
                    'athlete_status': faker.words(1, status_flags, True)[0],
                    'summer_bridge_status': faker.words(1, status_flags, True)[0],
                    'last_school_lcff_plus_flag': faker.words(1, status_flags, True)[0],
                    'special_program_cep': faker.words(1, status_flags, True)[0],
                },
            )


if __name__ == '__main__':
    create_csv_file()
