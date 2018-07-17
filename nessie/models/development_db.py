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


from flask import current_app as app
from nessie import db, std_commit
from nessie.externals import redshift
# Models below are included so that db.create_all will find them.
from nessie.models.json_cache import JsonCache # noqa
import psycopg2.sql
from sqlalchemy.sql import text


def clear():
    with open(app.config['BASE_DIR'] + '/scripts/db/drop_schema.sql', 'r') as ddlfile:
        ddltext = ddlfile.read()
    db.session().execute(text(ddltext))
    std_commit(allow_test_environment=True)


def load():
    load_schemas()
    load_development_data()
    return db


def load_development_data():
    load_student_athletes()


def load_schemas():
    """Create DB schema from SQL file."""
    with open(app.config['BASE_DIR'] + '/scripts/db/schema.sql', 'r') as ddlfile:
        ddltext = ddlfile.read()
    db.session().execute(text(ddltext))
    std_commit(allow_test_environment=True)


def create_student(sid, active=True, intensive=False, status_asc=None, team_groups=None):
    schema = psycopg2.sql.Identifier(app.config['REDSHIFT_SCHEMA_ASC'])
    sql = """INSERT INTO {schema}.students
        (sid, active, intensive, status_asc, group_code, group_name, team_code, team_name, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, current_timestamp, current_timestamp)
    """
    if len(team_groups):
        for t in team_groups:
            redshift.execute(
                sql,
                params=(sid, active, intensive, status_asc, t['group_code'], t['group_name'], t['team_code'], t['team_name']),
                schema=schema,
            )
    else:
        redshift.execute(
            sql,
            params=(sid, active, intensive, status_asc, None, None, None, None),
            schema=schema,
        )


def load_student_athletes():
    """Use Postgres to mock the Redshift students schema on local test runs."""
    schema = app.config['REDSHIFT_SCHEMA_ASC']
    redshift.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
    redshift.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
    redshift.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.students
    (
        sid VARCHAR NOT NULL,
        active BOOLEAN NOT NULL,
        intensive BOOLEAN NOT NULL,
        status_asc VARCHAR,
        group_code VARCHAR,
        group_name VARCHAR,
        team_code VARCHAR,
        team_name VARCHAR,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL
    )""")
    redshift.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.student_profiles
    (
        sid VARCHAR NOT NULL,
        profile VARCHAR(max) NOT NULL
    )""")

    football_defensive_backs = {
        'group_code': 'MFB-DB',
        'group_name': 'Football, Defensive Backs',
        'team_code': 'FBM',
        'team_name': 'Football',
    }
    football_defensive_line = {
        'group_code': 'MFB-DL',
        'group_name': 'Football, Defensive Line',
        'team_code': 'FBM',
        'team_name': 'Football',
    }
    womens_field_hockey = {
        'group_code': 'WFH',
        'group_name': 'Women\'s Field Hockey',
        'team_code': 'FHW',
        'team_name': 'Women\'s Field Hockey',
    }
    mens_baseball = {
        'group_code': 'MBB',
        'group_name': 'Men\'s Baseball',
        'team_code': 'BAM',
        'team_name': 'Men\'s Baseball',
    }
    mens_tennis = {
        'group_code': 'MTE',
        'group_name': 'Men\'s Tennis',
        'team_code': 'TNM',
        'team_name': 'Men\'s Tennis',
    }
    womens_tennis = {
        'group_code': 'WTE',
        'group_name': 'Women\'s Tennis',
        'team_code': 'TNW',
        'team_name': 'Women\'s Tennis',
    }
    create_student(
        sid='11667051',
        intensive=True,
        team_groups=[womens_field_hockey, womens_tennis],
    )
    create_student(
        sid='8901234567',
        intensive=True,
        team_groups=[],
    )
    create_student(
        sid='2345678901',
        team_groups=[football_defensive_backs, football_defensive_line],
    )
    create_student(
        sid='3456789012',
        intensive=True,
        team_groups=[football_defensive_line],
    )
    create_student(
        sid='5678901234',
        team_groups=[football_defensive_backs, football_defensive_line, mens_tennis],
    )
    create_student(
        sid='7890123456',
        intensive=True,
        team_groups=[mens_baseball],
    )
    create_student(
        sid='890127492',
        active=False,
        intensive=True,
        status_asc='Trouble',
        # 'A mug is a mug in everything.' - Colonel Harrington
        team_groups=[
            football_defensive_backs,
            football_defensive_line,
            mens_tennis,
            womens_field_hockey,
            womens_tennis,
        ],
    )

    std_commit(allow_test_environment=True)
