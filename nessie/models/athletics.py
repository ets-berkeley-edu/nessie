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


from nessie import db
from nessie.models.base import Base
from nessie.models.db_relationships import student_athletes


class Athletics(Base):
    __tablename__ = 'athletics'

    group_code = db.Column(db.String(80), nullable=False, primary_key=True)
    group_name = db.Column(db.String(255))
    team_code = db.Column(db.String(80))
    team_name = db.Column(db.String(255))
    athletes = db.relationship('Student', secondary=student_athletes, back_populates='athletics')

    def __repr__(self):
        return f"""<TeamGroup {self.group_name} ({self.group_code}),
            team {self.team_code} ({self.team_name}),
            updated_at={self.updated_at},
            created_at={self.created_at}>"""

    def to_api_json(self):
        return {
            'groupCode': self.group_code,
            'groupName': self.group_name,
            'name': self.group_name,
            'teamCode': self.team_code,
            'teamName': self.team_name,
        }
