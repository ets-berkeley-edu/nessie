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


import logging
import os


# Base directory for the application (one level up from this config file).
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

TIMEZONE = 'America/Los_Angeles'

# Some defaults.
CSRF_ENABLED = True
CSRF_SESSION_KEY = 'secret'
# Used to encrypt session cookie.
SECRET_KEY = 'secret'

# Override in local configs.
HOST = '0.0.0.0'
PORT = 1234

AWS_ACCESS_KEY_ID = 'key'
AWS_SECRET_ACCESS_KEY = 'secret'
AWS_REGION = 'aws region'

CANVAS_DATA_API_KEY = 'some key'
CANVAS_DATA_API_SECRET = 'some secret'
CANVAS_DATA_HOST = 'foo.instructure.com'

DEGREE_PROGRESS_API_URL = 'https://secreturl.berkeley.edu/PSFT_CS'
DEGREE_PROGRESS_API_USERNAME = 'secretuser'
DEGREE_PROGRESS_API_PASSWORD = 'secretpassword'

ENROLLMENTS_API_ID = 'secretid'
ENROLLMENTS_API_KEY = 'secretkey'
ENROLLMENTS_API_URL = 'https://secreturl.berkeley.edu/enrollments'

# True on master node, false on worker nodes.
JOB_SCHEDULING_ENABLED = True
# See http://apscheduler.readthedocs.io/en/latest/modules/triggers/cron.html for supported schedule formats.
JOB_SYNC_CANVAS_SNAPSHOTS = {'hour': 1, 'minute': 0}
JOB_RESYNC_CANVAS_SNAPSHOTS = {'hour': 1, 'minute': 40}
JOB_GENERATE_ALL_TABLES = {'hour': 2, 'minute': 00}

LOCH_S3_BUCKET = 'bucket_name'
LOCH_S3_REGION = 'us-west-2'

LOCH_S3_CANVAS_DATA_PATH_CURRENT_TERM = 'canvas/path/to/current/term'
LOCH_S3_CANVAS_DATA_PATH_DAILY = 'canvas/path/to/daily'
LOCH_S3_CANVAS_DATA_PATH_HISTORICAL = 'canvas/path/to/historical'

LOCH_S3_SIS_DATA_PATH = 'sis/path'

LOCH_CANVAS_DATA_REQUESTS_CUTOFF_DATE = '20180101'

LOGGING_FORMAT = '[%(asctime)s] - %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
LOGGING_LOCATION = 'nessie.log'
LOGGING_LEVEL = logging.DEBUG

REDSHIFT_DATABASE = 'database'
REDSHIFT_HOST = 'redshift cluster'
REDSHIFT_PASSWORD = 'password'
REDSHIFT_PORT = 1234
REDSHIFT_USER = 'username'

REDSHIFT_IAM_ROLE = 'iam role'

REDSHIFT_SCHEMA_BOAC = 'BOAC schema name'
REDSHIFT_SCHEMA_CANVAS = 'Canvas schema name'
REDSHIFT_SCHEMA_INTERMEDIATE = 'Intermediate schema name'
REDSHIFT_SCHEMA_METADATA = 'Metadata schema name'
REDSHIFT_SCHEMA_SIS = 'SIS schema name'

STUDENT_API_ID = 'secretid'
STUDENT_API_KEY = 'secretkey'
STUDENT_API_URL = 'https://secreturl.berkeley.edu/students'

WORKER_HOST = 'hard-working-nessie.berkeley.edu'
WORKER_USERNAME = 'username'
WORKER_PASSWORD = 'password'

# True on worker nodes, false on master node.
WORKER_QUEUE_ENABLED = False
WORKER_THREADS = 5
