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

import json

from nessie.merged.sis_profile import merge_sis_profile_academic_status, parse_merged_sis_profile
import pytest
from tests.util import capture_app_logs, mock_s3


@pytest.fixture
def force_sis_profile_v2(app):
    original = app.config['STUDENT_V1_API_PREFERRED']
    app.config['STUDENT_V1_API_PREFERRED'] = False
    yield
    app.config['STUDENT_V1_API_PREFERRED'] = original


@pytest.fixture()
def sis_api_profiles(app, student_tables):
    from nessie.externals import redshift
    from nessie.jobs.import_sis_student_api import ImportSisStudentApi
    with mock_s3(app):
        ImportSisStudentApi().run_wrapped()
    sql = f"""SELECT sid, feed FROM student_test.sis_api_profiles"""
    return redshift.fetch(sql)


@pytest.fixture()
def sis_api_degree_progress(app, student_tables):
    from nessie.externals import redshift
    sql = f"""SELECT sid, feed FROM student_test.sis_api_degree_progress"""
    return redshift.fetch(sql)


@pytest.fixture()
def sis_api_last_registrations(app, metadata_db, student_tables):
    from nessie.externals import redshift
    from nessie.jobs.import_registrations import ImportRegistrations
    with mock_s3(app):
        ImportRegistrations().run_wrapped()
    sql = f"""SELECT sid, feed FROM student_test.student_last_registrations"""
    return redshift.fetch(sql)


def merged_profile(sid, profile_rows, degree_progress_rows, last_registration_rows):
    profile_feed = next((r['feed'] for r in profile_rows if r['sid'] == sid), None)
    progress_feed = next((r['feed'] for r in degree_progress_rows if r['sid'] == sid), None)
    last_registration_feed = next((r['feed'] for r in last_registration_rows if r['sid'] == sid), None)
    return parse_merged_sis_profile(profile_feed, progress_feed, last_registration_feed)


@pytest.mark.usefixtures('force_sis_profile_v2')
class TestMergedSisProfile:
    """Test merged SIS profile."""

    def test_skips_concurrent_academic_status(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        """Skips concurrent academic status if another academic status exists."""
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['academicCareer'] == 'UGRD'
        assert profile['plans'][0]['program'] == 'Undergrad Letters & Science'

    def test_falls_back_on_concurrent_academic_status(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        """Selects concurrent academic status if no other academic status exists."""
        profile = merged_profile('1234567890', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['academicCareer'] == 'UCBX'
        assert profile['plans'][0]['program'] == 'UCBX Concurrent Enrollment'

    def test_withdrawal_cancel_ignored_if_empty(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert 'withdrawalCancel' not in profile

    def test_withdrawal_cancel_included_if_present(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('2345678901', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['withdrawalCancel']['description'] == 'Withdrew'
        assert profile['withdrawalCancel']['reason'] == 'Personal'
        assert profile['withdrawalCancel']['date'] == '2017-10-31'
        assert profile['withdrawalCancel']['termId'] == '2178'

    def test_degree_progress(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['degreeProgress']['reportDate'] == '2017-03-03'
        assert len(profile['degreeProgress']['requirements']) == 4
        assert profile['degreeProgress']['requirements'][0] == {'entryLevelWriting': {'status': 'Satisfied'}}

    def test_no_holds(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['holds'] == []

    def test_multiple_holds(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('2345678901', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        holds = profile['holds']
        assert len(holds) == 2
        assert holds[0]['reason']['code'] == 'CSBAL'
        assert holds[1]['reason']['code'] == 'ADVHD'

    def test_current_term(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['currentRegistration']['term']['id'] == '2178'
        assert profile['level']['code'] == '30'
        assert profile['level']['description'] == 'Junior'
        assert profile['currentTerm']['unitsMaxOverride'] == 24
        assert profile['currentTerm']['unitsMinOverride'] == 15

    def test_zero_gpa_when_gpa_units(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        for row in sis_api_profiles:
            if row['sid'] == '11667051':
                feed = json.loads(row['feed'], strict=False)
                feed['academicStatuses'][1]['cumulativeGPA']['average'] = 0
                row['feed'] = json.dumps(feed)
                break
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['cumulativeGPA'] == 0

    def test_null_gpa_when_no_gpa_units(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        for row in sis_api_profiles:
            if row['sid'] == '11667051':
                feed = json.loads(row['feed'], strict=False)
                feed['academicStatuses'][1]['cumulativeGPA']['average'] = 0
                feed['academicStatuses'][1]['cumulativeUnits'][1]['unitsTaken'] = 0
                row['feed'] = json.dumps(feed)
                break
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['cumulativeGPA'] is None

    def test_expected_graduation_term(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['expectedGraduationTerm']['id'] == '2198'
        assert profile['expectedGraduationTerm']['name'] == 'Fall 2019'

    def test_transfer_true_if_notation_present(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('2345678901', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['transfer'] is True

    def test_transfer_false_if_notation_not_present(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        profile = merged_profile('11667051', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['transfer'] is False

    def test_no_registrations_in_list(self, app, sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations):
        """Falls back to last term-with-units if the student is not active in the current term."""
        profile = merged_profile('1234567890', sis_api_profiles, sis_api_degree_progress, sis_api_last_registrations)
        assert profile['currentRegistration']['term']['id'] == '2172'
        assert profile['level']['code'] == '20'
        assert profile['level']['description'] == 'Sophomore'

    class TestMergeAcademicStatus:
        """Test other combinations of careers and plans."""

        def test_discontinued_ugrd_active_ucbx(self, app):
            feed = {
                'academicStatuses': [
                    _active_ucbx_academic_status(),
                    _discontinued_ugrd_academic_status(),
                ],
                'affiliations': [
                    _active_ucbx_affiliation(),
                    _discontinued_ugrd_affiliation(),
                ],
            }
            profile = {}
            merge_sis_profile_academic_status(feed, profile)
            assert profile['academicCareer'] == 'UGRD'
            assert profile['academicCareerStatus'] == 'Inactive'
            assert profile['plans'][0]['description'] == 'Interdisciplinary Studies BA'
            assert profile['plans'][0]['status'] == 'Cancelled'
            assert profile['plans'][1]['description'] == 'Summer Domestic Visitor UG'
            assert profile['plans'][1]['status'] == 'Discontinued'

        def test_completed_ugrd_active_grad(self, app):
            feed = {
                'academicStatuses': [
                    _active_grad_academic_status(),
                    _completed_ugrd_academic_status(),
                ],
                'affiliations': [
                    _active_grad_affiliation(),
                    _completed_ugrd_affiliation(),
                ],
            }
            profile = {}
            merge_sis_profile_academic_status(feed, profile)
            assert profile['academicCareer'] == 'UGRD'
            assert profile.get('academicCareerStatus') == 'Completed'
            assert profile.get('academicCareerCompleted') == '2018-05-17'

        def test_affiliations_conflict(self, app, caplog):
            feed = {
                'academicStatuses': [
                    _active_grad_academic_status(),
                ],
                'affiliations': [
                    _active_ucbx_affiliation(),
                ],
            }
            profile = {}
            with capture_app_logs(app):
                merge_sis_profile_academic_status(feed, profile)
                assert profile['academicCareer'] == 'GRAD'
                assert profile.get('academicCareerStatus') is None
                assert profile['plans'][0]['description'] == 'On-Campus/Online Prfsnl MPH'
                assert profile['plans'][0]['status'] == 'Active'
                assert 'Conflict between affiliations and academicStatuses' in caplog.text


def _active_grad_affiliation():
    return {
        'detail': 'Active',
        'fromDate': '2018-06-26',
        'status': {
            'code': 'ACT',
            'description': 'Active',
        },
        'type': {
            'code': 'GRADUATE',
            'description': 'Graduate Student',
        },
    }


def _active_ucbx_affiliation():
    return {
        'detail': 'Active',
        'fromDate': '2018-06-26',
        'status': {
            'code': 'ACT',
            'description': 'Active',
            'formalDescription': 'Active',
        },
        'type': {
            'code': 'EXTENSION',
            'description': 'UCB Extension Student',
            'formalDescription': 'An individual with a UCB Extension-based Career/Program/Plan.',
        },
    }


def _completed_ugrd_affiliation():
    return {
        'detail': 'Completed',
        'fromDate': '2014-12-14',
        'status': {
            'code': 'INA',
            'description': 'Inactive',
            'formalDescription': 'Inactive',
        },
        'toDate': '2018-07-22',
        'type': {
            'code': 'UNDERGRAD',
            'description': 'Undergraduate Student',
            'formalDescription': 'An individual with an Undergraduate-based Career/Program/Plan.',
        },
    }


def _discontinued_ugrd_affiliation():
    return {
        'detail': 'Discontinued',
        'status': {
            'code': 'INA',
            'description': 'Inactive',
            'formalDescription': 'Inactive',
        },
        'type': {
            'code': 'UNDERGRAD',
            'description': 'Undergraduate Student',
            'formalDescription': 'An individual with an Undergraduate-based Career/Program/Plan.',
        },
    }


def _active_ucbx_academic_status():
    return {
        'studentCareer': {
            'academicCareer': {
                'code': 'UCBX',
                'description': 'UCB Ext',
                'formalDescription': 'UC Berkeley Extension',
            },
        },
        'studentPlans': [
            {
                'academicPlan': {
                    'academicProgram': {
                        'academicCareer': {
                            'code': 'UCBX',
                            'description': 'UCB Ext',
                            'formalDescription': 'UC Berkeley Extension',
                        },
                    },
                    'plan': {
                        'code': '30XCECCENX',
                        'description': 'UCBX Concurrent Enrollment',
                        'formalDescription': 'UC Berkeley Extension Concurrent Enrollment',
                    },
                },
                'statusInPlan': {
                    'action': {
                        'code': 'DISC',
                        'description': 'Discontinu',
                        'formalDescription': 'Discontinuation',
                    },
                    'reason': {
                        'code': '1TRM',
                        'description': 'DISC After One Term',
                        'formalDescription': 'Discontinuation After One Term',
                    },
                    'status': {
                        'code': 'DC',
                        'description': 'Discontinu',
                        'formalDescription': 'Discontinued',
                    },
                },
            },
            {
                'academicPlan': {
                    'academicProgram': {
                        'academicCareer': {
                            'code': 'UCBX',
                            'description': 'UCB Ext',
                            'formalDescription': 'UC Berkeley Extension',
                        },
                    },
                    'plan': {
                        'code': '30XCECCENX',
                        'description': 'UCBX Concurrent Enrollment',
                        'formalDescription': 'UC Berkeley Extension Concurrent Enrollment',
                    },
                    'type': {
                        'code': 'SS',
                        'description': 'Major - Self-Supporting',
                        'formalDescription': 'Major - Self-Supporting',
                    },
                },
                'statusInPlan': {
                    'action': {
                        'code': 'MATR',
                        'description': 'Matriculat',
                        'formalDescription': 'Matriculation',
                    },
                    'reason': {
                        'code': 'NEW',
                        'description': 'New Admits',
                        'formalDescription': 'New Admits (both New and Transfer students)',
                    },
                    'status': {
                        'code': 'AC',
                        'description': 'Active',
                        'formalDescription': 'Active in Program',
                    },
                },
            },
        ],
    }


def _completed_ugrd_academic_status():
    return {
        'studentCareer': {
            'academicCareer': {
                'code': 'UGRD',
                'description': 'Undergrad',
                'formalDescription': 'Undergraduate',
            },
            'toDate': '2018-05-17',
        },
        'studentPlans': [
            {
                'academicPlan': {
                    'academicProgram': {
                        'academicCareer': {
                            'code': 'UGRD',
                            'description': 'Undergrad',
                            'formalDescription': 'Undergraduate',
                        },
                        'academicGroup': {
                            'code': 'CLS',
                            'description': 'L&S',
                            'formalDescription': 'College of Letters and Science',
                        },
                        'program': {
                            'code': 'UCLS',
                            'description': 'UG L&S',
                            'formalDescription': 'Undergrad Letters & Science',
                        },
                    },
                    'plan': {
                        'code': '25628U',
                        'description': 'Interdisciplinary Studies BA',
                        'formalDescription': 'Interdisciplinary Studies',
                    },
                    'type': {
                        'code': 'MAJ',
                        'description': 'Major - Regular Acad/Prfnl',
                        'formalDescription': 'Major - Regular Acad/Prfnl',
                    },
                },
                'statusInPlan': {
                    'action': {
                        'code': 'COMP',
                        'description': 'Completion',
                        'formalDescription': 'Completion of Program',
                    },
                    'status': {
                        'code': 'CM',
                        'description': 'Completed',
                        'formalDescription': 'Completed Program',
                    },
                },
                'toDate': '2019-05-17',
            },
        ],
    }


def _discontinued_ugrd_academic_status():
    return {
        'studentCareer': {
            'academicCareer': {
                'code': 'UGRD',
                'description': 'Undergrad',
                'formalDescription': 'Undergraduate',
            },
            'toDate': '2018-10-01',
        },
        'studentPlans': [
            {
                'academicPlan': {
                    'academicProgram': {
                        'academicCareer': {
                            'code': 'UGRD',
                            'description': 'Undergrad',
                            'formalDescription': 'Undergraduate',
                        },
                        'academicGroup': {
                            'code': 'CLS',
                            'description': 'L&S',
                            'formalDescription': 'College of Letters and Science',
                        },
                        'program': {
                            'code': 'UCLS',
                            'description': 'UG L&S',
                            'formalDescription': 'Undergrad Letters & Science',
                        },
                    },
                    'plan': {
                        'code': '25628U',
                        'description': 'Interdisciplinary Studies BA',
                        'formalDescription': 'Interdisciplinary Studies',
                    },
                    'type': {
                        'code': 'MAJ',
                        'description': 'Major - Regular Acad/Prfnl',
                        'formalDescription': 'Major - Regular Acad/Prfnl',
                    },
                },
                'statusInPlan': {
                    'action': {
                        'code': 'WADM',
                        'description': 'Adm W/drwl',
                        'formalDescription': 'Administrative Withdrawal',
                    },
                    'status': {
                        'code': 'CN',
                        'description': 'Cancelled',
                        'formalDescription': 'Cancelled',
                    },
                },
            },
            {
                'academicPlan': {
                    'academicProgram': {
                        'academicCareer': {
                            'code': 'UGRD',
                            'description': 'Undergrad',
                            'formalDescription': 'Undergraduate',
                        },
                        'academicGroup': {
                            'code': 'UGND',
                            'description': 'UG Non-Deg',
                            'formalDescription': 'Undergraduate Non-Degree',
                        },
                        'program': {
                            'code': 'UNODG',
                            'description': 'UG NonDeg',
                            'formalDescription': 'Undergrad Non-Degree/NonFinAid',
                        },
                    },
                    'plan': {
                        'code': '99000U',
                        'description': 'Summer Domestic Visitor UG',
                        'formalDescription': 'Undeclared Summer Session Visitor',
                    },
                    'type': {
                        'code': 'MAJ',
                        'description': 'Major - Regular Acad/Prfnl',
                        'formalDescription': 'Major - Regular Acad/Prfnl',
                    },
                },
                'statusInPlan': {
                    'action': {
                        'code': 'DISC',
                        'description': 'Discontinu',
                        'formalDescription': 'Discontinuation',
                    },
                    'reason': {
                        'code': 'NPRO',
                        'description': 'Non-Progressing',
                        'formalDescription': 'Non-Progressing',
                    },
                    'status': {
                        'code': 'DC',
                        'description': 'Discontinu',
                        'formalDescription': 'Discontinued',
                    },
                },
            },
        ],
    }


def _active_grad_academic_status():
    return {
        'studentCareer': {
            'academicCareer': {
                'code': 'GRAD',
                'description': 'Graduate',
                'formalDescription': 'Graduate',
            },
        },
        'studentPlans': [
            {
                'academicPlan': {
                    'academicProgram': {
                        'academicCareer': {
                            'code': 'GRAD',
                            'description': 'Graduate',
                            'formalDescription': 'Graduate',
                        },
                        'academicGroup': {
                            'code': 'GRAD',
                            'description': 'Grad Div',
                            'formalDescription': 'Graduate Division',
                        },
                        'program': {
                            'code': 'GSSDP',
                            'description': 'GR SSD Pgm',
                            'formalDescription': 'Graduate Self-Supporting Pgms',
                        },
                    },
                    'plan': {
                        'code': '962C4MPHG',
                        'description': 'On-Campus/Online Prfsnl MPH',
                        'formalDescription': 'On-Campus/Online Professional Master of Public Health',
                    },
                    'type': {
                        'code': 'SS',
                        'description': 'Major - Self-Supporting',
                        'formalDescription': 'Major - Self-Supporting',
                    },
                },
                'statusInPlan': {
                    'action': {
                        'code': 'DATA',
                        'description': 'Data Chg',
                        'formalDescription': 'Data Change',
                    },
                    'reason': {
                        'code': 'GTOI',
                        'description': 'Grad Term - Auto Opt-In',
                        'formalDescription': 'Graduation Term - Automatic Opt-In',
                    },
                    'status': {
                        'code': 'AC',
                        'description': 'Active',
                        'formalDescription': 'Active in Program',
                    },
                },
            },
        ],
    }
