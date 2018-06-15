"""
Copyright ©2018. The Regents of the University of California (Regents). All Rights Reserved.

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

from nessie.lib import asc
from nessie.models.athletics import Athletics
from nessie.models.student import Student


def asc_data_row(sid, group_code, group_name, team_code, team_name, academic_yr, is_active, is_intensive='No', status=''):
    return {
        'SID': sid,
        'SportCode': group_code,
        'Sport': group_name,
        'SportCodeCore': team_code,
        'SportCore': team_name,
        'AcadYr': academic_yr,
        'ActiveYN': is_active,
        'IntensiveYN': is_intensive,
        'SportStatus': status,
    }


def find_athlete(team, sid):
    return next(athlete for athlete in team.athletes if athlete.sid == sid)


class TestAscStudentDataMerge:
    """Merge ASC student data with other sources."""

    def test_initial_assumptions(self, app):
        water_polo_team = Athletics.query.filter_by(group_code='MWP').first()
        assert water_polo_team is None
        football_backs = Athletics.query.filter_by(group_code='MFB-DB').first()
        assert len(football_backs.athletes) == 3
        # John starts without a team.
        assert Student.find_by_sid('8901234567').athletics == []
        # PaulK defends the line.
        assert Student.find_by_sid('3456789012').athletics[0].group_code == 'MFB-DL'
        # Sandeep is busy.
        assert len(Student.find_by_sid('5678901234').athletics) == 3
        # Siegfried is a mug at everything.
        inactive_student = Student.find_by_sid('890127492')
        assert not inactive_student.is_active_asc
        assert inactive_student.status_asc == 'Trouble'
        assert len(inactive_student.athletics) == 5

    def test_empty_import(self, app):
        """Gracefully handles empty dataset."""
        status = asc.merge_student_athletes([], delete_students=False)
        assert {0} == set(status.values())

    def test_students_on_multiple_teams(self, app):
        """Maps one student to more than one team."""
        jane_sid = '1234567890'
        polo_code = 'WWP'
        volleyball_code = 'WVB'
        asc_data = [
            asc_data_row(
                jane_sid,
                polo_code,
                'Women\'s Water Polo',
                'WWP',
                'Women\'s Water Polo',
                '2017-18',
                'Yes',
            ),
            asc_data_row(
                jane_sid,
                volleyball_code,
                'Women\'s Volleyball',
                'WVB',
                'Women\'s Volleyball',
                '2017-18',
                'Yes',
            ),
        ]
        # Run import script
        status = asc.merge_student_athletes(asc_data, delete_students=False)
        assert 2 == status['new_team_groups']
        assert 1 == status['new_students']
        assert 2 == status['new_memberships']
        polo_team = Athletics.query.filter_by(group_code=polo_code).first()
        assert find_athlete(polo_team, jane_sid)
        volleyball_team = Athletics.query.filter_by(group_code=volleyball_code).first()
        assert find_athlete(volleyball_team, jane_sid)
        assert True

    def test_student_inactive(self, app):
        """Only imports inactive students if they are assigned to a team."""
        jane_sid = '1234567890'
        polo_code = 'WWP'
        asc_data = [
            asc_data_row(
                jane_sid,
                polo_code,
                'Women\'s Water Polo',
                'WWP',
                'Women\'s Water Polo',
                '2017-18',
                'No',
                'Yes',
                status='Not Active',
            ),
            asc_data_row(
                '96',
                '',
                '',
                '',
                '',
                '2017-18',
                'No',
                'Yes',
                status='TvParty2nite',
            ),
        ]
        # Run import script
        status = asc.merge_student_athletes(asc_data, delete_students=False)
        assert 1 == status['new_students']
        saved_student = Student.find_by_sid(jane_sid)
        assert saved_student.is_active_asc is False
        assert saved_student.status_asc == 'Not Active'

    def test_student_half_active(self, app):
        """A student who is active on one team is considered an active athlete."""
        sid = '1234567890'
        asc_data = [
            asc_data_row(
                sid,
                'WWP',
                'Women\'s Water Polo',
                'WWP',
                'Women\'s Water Polo',
                '2017-18',
                'No',
                'Yes',
                status='Not Active',
            ),
            asc_data_row(
                sid,
                'WFH',
                'Women\'s Field Hockey',
                'WFH',
                'Women\'s Field Hockey',
                '2017-18',
                'Yes',
                'Yes',
                status='Practice',
            ),
            asc_data_row(
                sid,
                'WTE',
                'Women\'s Tennis',
                'WTE',
                'Women\'s Tennis',
                '2017-18',
                'No',
                'Yes',
                status='Not Squad',
            ),
        ]
        # Run import script
        status = asc.merge_student_athletes(asc_data, delete_students=False)
        assert 1 == status['new_students']
        saved_student = Student.find_by_sid(sid)
        assert saved_student.is_active_asc is True
        assert len(saved_student.athletics) == 1
        assert saved_student.athletics[0].group_code == 'WFH'

    def test_student_intensive(self, app):
        """Marks intensive status if set."""
        jane_sid = '1234567890'
        polo_code = 'WWP'
        asc_data = [
            asc_data_row(
                jane_sid,
                polo_code,
                'Women\'s Water Polo',
                'WWP',
                'Women\'s Water Polo',
                '2017-18',
                'Yes',
                'Yes',
            ),
        ]
        # Run import script
        status = asc.merge_student_athletes(asc_data, delete_students=False)
        assert 1 == status['new_students']
        saved_student = Student.find_by_sid(jane_sid)
        assert saved_student.in_intensive_cohort
        assert saved_student.is_active_asc is True

    def test_ambiguous_group_name(self, app):
        sid = '1234567890'
        ambiguous_group = 'MFB'
        asc_data = [
            asc_data_row(
                sid,
                ambiguous_group,
                'Football',
                ambiguous_group,
                'Football',
                '2017-18',
                'Yes',
            ),
        ]
        # Run import script
        status = asc.merge_student_athletes(asc_data, delete_students=False)
        assert 1 == status['new_team_groups']
        eccentric_football_group = Athletics.query.filter_by(group_code=ambiguous_group).first()
        assert 'Football - Other' == eccentric_football_group.group_name
