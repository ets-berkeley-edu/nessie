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


from nessie import db, std_commit
from nessie.models.base import Base
from nessie.models.db_relationships import student_athletes
from sqlalchemy.orm import joinedload


def sqlalchemy_bindings(values, column_name):
    # In support of SQLAlchemy expression language
    bindings = {}
    for index, value in enumerate(values, start=1):
        bindings[column_name + str(index)] = value
    return bindings


class Student(Base):
    __tablename__ = 'students'

    sid = db.Column(db.String(80), nullable=False, primary_key=True)
    uid = db.Column(db.String(80))
    first_name = db.Column(db.String(255), nullable=False)
    last_name = db.Column(db.String(255), nullable=False)
    in_intensive_cohort = db.Column(db.Boolean, nullable=False, default=False)
    athletics = db.relationship('Athletics', secondary=student_athletes, back_populates='athletes')
    is_active_asc = db.Column(db.Boolean, nullable=False, default=True)
    status_asc = db.Column(db.String(80))

    def __repr__(self):
        return f"""<Athlete sid={self.sid}, uid={self.uid}, first_name={self.first_name}, last_name={self.last_name},
            in_intensive_cohort={self.in_intensive_cohort}, is_active_asc={self.is_active_asc},
            status_asc={self.status_asc}, updated={self.updated_at}, created={self.created_at}>"""

    @classmethod
    def find_by_sid(cls, sid):
        return cls.query.filter_by(sid=sid).first()

    @classmethod
    def get_all(cls, order_by=None, is_active_asc=None):
        query = Student.query
        if is_active_asc is not None:
            query = query.filter(cls.is_active_asc.is_(is_active_asc))
        students = query.options(joinedload('athletics')).all()
        if order_by and len(students) > 0:
            # For now, only one order_by value is supported
            if order_by == 'groupName':
                students = sorted(students, key=lambda student: student.athletics and student.athletics[0].group_name)
        return [s.to_expanded_api_json() for s in students]

    @classmethod
    def delete_student(cls, sid):
        student = Student.query.filter(Student.sid == sid).first()
        student.athletics = []
        db.session.delete(student)
        std_commit()
        return

    def to_api_json(self):
        return {
            'sid': self.sid,
            'uid': self.uid,
            'firstName': self.first_name,
            'lastName': self.last_name,
            'name': self.first_name + ' ' + self.last_name,
            'inIntensiveCohort': self.in_intensive_cohort,
            'isActiveAsc': self.is_active_asc,
            'statusAsc': self.status_asc,
        }

    def to_expanded_api_json(self):
        api_json = self.to_api_json()
        if self.athletics:
            api_json['athletics'] = sorted((a.to_api_json() for a in self.athletics), key=lambda a: a['groupName'])
        return api_json
