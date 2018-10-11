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

SQLALCHEMY_DATABASE_URI = 'postgres://nessie:nessie@localhost:5432/nessie'
SQLALCHEMY_TRACK_MODIFICATIONS = False

CAS_SERVER = 'https://auth-test.berkeley.edu/cas/'
CAS_LOGOUT_URL = 'https://auth-test.berkeley.edu/cas/logout'

# Some defaults.
CSRF_ENABLED = True
CSRF_SESSION_KEY = 'secret'
# Used to encrypt session cookie.
SECRET_KEY = 'secret'
# Used to authorize administrative API.
API_USERNAME = 'username'
API_PASSWORD = 'password'
# UIDs of authorized 'Admin' users
AUTHORIZED_USERS = [0000000, 1111111, 2222222]

# Override in local configs.
HOST = '0.0.0.0'
PORT = 1234

AWS_ACCESS_KEY_ID = 'key'
AWS_DMS_VPC_ROLE = 'dms vpc role'
AWS_SECRET_ACCESS_KEY = 'secret'
AWS_REGION = 'aws region'

ASC_ATHLETES_API_URL = 'https://secreturl.berkeley.edu/intensives.php?AcadYr=2017-18'
ASC_ATHLETES_API_KEY = 'secret'
ASC_THIS_ACAD_YR = '2017-18'

BOAC_REFRESHERS = [{'API_KEY': 'Regents of the University of California', 'URL': 'https://ets-boac.example.com/api/refresh_me'}]

CANVAS_DATA_API_KEY = 'some key'
CANVAS_DATA_API_SECRET = 'some secret'
CANVAS_DATA_HOST = 'foo.instructure.com'

CANVAS_HTTP_URL = 'https://wottsamatta.instructure.com'
CANVAS_HTTP_TOKEN = 'yet another secret'

CURRENT_TERM = 'Fall 2017'

DEGREE_PROGRESS_API_URL = 'https://secreturl.berkeley.edu/PSFT_CS'
DEGREE_PROGRESS_API_USERNAME = 'secretuser'
DEGREE_PROGRESS_API_PASSWORD = 'secretpassword'

EARLIEST_TERM = 'Fall 2013'

ENROLLMENTS_API_ID = 'secretid'
ENROLLMENTS_API_KEY = 'secretkey'
ENROLLMENTS_API_URL = 'https://secreturl.berkeley.edu/enrollments'

# True on master node, false on worker nodes.
# Override by embedding "master" or "worker" in the EB_ENVIRONMENT environment variable.
JOB_SCHEDULING_ENABLED = True

# See http://apscheduler.readthedocs.io/en/latest/modules/triggers/cron.html for supported schedule formats.
# Schedules should be provided as dictionaries in configs, e.g. {'hour': 12, 'minute': 30}
JOB_SYNC_CANVAS_SNAPSHOTS = {}
JOB_RESYNC_CANVAS_SNAPSHOTS = {}
JOB_IMPORT_CANVAS_ENROLLMENTS = {}
JOB_IMPORT_STUDENT_POPULATION = {}
JOB_IMPORT_DEGREE_PROGRESS = {}
JOB_IMPORT_LRS_INCREMENTALS = {}
JOB_IMPORT_SIS_ENROLLMENTS = {}
JOB_IMPORT_SIS_STUDENTS = {}
JOB_GENERATE_ALL_TABLES = {}
JOB_GENERATE_CURRENT_TERM_FEEDS = {}
JOB_REFRESH_BOAC_CACHE = {}

LDAP_HOST = 'nds-test.berkeley.edu'
LDAP_BIND = 'mybind'
LDAP_PASSWORD = 'secret'

LRS_DATABASE_URI = 'postgres://lrs:lrs@localhost:5432/lrs'

LRS_INCREMENTAL_REPLICATION_TASK_ID = 'task-id'
LRS_INCREMENTAL_TRANSIENT_BUCKET = 'transient bucket'
LRS_INCREMENTAL_TRANSIENT_PATH = 'lrs/transient/path'
LRS_INCREMENTAL_DESTINATION_BUCKETS = ['bucket', 'list']
LRS_INCREMENTAL_DESTINATION_PATH = 'lrs/destination/path'
LRS_INCREMENTAL_ETL_PATH_REDSHIFT = 'lrs/etl/path/redshift'

LRS_CANVAS_CALIPER_SCHEMA_PATH = 'lrs/path/to/caliper/schema'
LRS_CANVAS_CALIPER_INPUT_DATA_PATH = 'lrs/input/data/s3/location'
LRS_CANVAS_CALIPER_EXPLODE_OUTPUT_PATH = 'lrs/glue/output/s3/location'

LRS_CANVAS_GLUE_JOB_NAME = 'job_name_env'
LRS_CANVAS_GLUE_JOB_CAPACITY = 2
LRS_CANVAS_GLUE_JOB_TIMEOUT = 20
LRS_CANVAS_GLUE_JOB_SCRIPT_PATH = 's3://<bucket>/path/to/glue/script'
LRS_GLUE_TEMP_DIR = 'glue/temp/dir'
LRS_GLUE_SERVICE_ROLE = 'glue-service-role-name'

LOCH_S3_BUCKET = 'bucket_name'
LOCH_S3_REGION = 'us-west-2'

LOCH_S3_CANVAS_DATA_PATH_CURRENT_TERM = 'canvas/path/to/current/term'
LOCH_S3_CANVAS_DATA_PATH_DAILY = 'canvas/path/to/daily'
LOCH_S3_CANVAS_DATA_PATH_HISTORICAL = 'canvas/path/to/historical'

LOCH_S3_ASC_DATA_PATH = 'asc/path'
LOCH_S3_CALNET_DATA_PATH = 'calnet/path'
LOCH_S3_COE_DATA_PATH = 'coe/path'
LOCH_S3_SIS_DATA_PATH = 'sis/path'
LOCH_S3_SIS_API_DATA_PATH = 'sisapi/path'

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

REDSHIFT_SCHEMA_ASC = 'ASC schema name'
REDSHIFT_SCHEMA_BOAC = 'BOAC schema name'
REDSHIFT_SCHEMA_CALNET = 'CalNet schema name'
REDSHIFT_SCHEMA_CANVAS = 'Canvas schema name'
REDSHIFT_SCHEMA_COE = 'COE schema name'
REDSHIFT_SCHEMA_COE_EXTERNAL = 'External COE schema name'
REDSHIFT_SCHEMA_INTERMEDIATE = 'Intermediate schema name'
REDSHIFT_SCHEMA_LRS = 'External LRS schema name'
REDSHIFT_SCHEMA_METADATA = 'Metadata schema name'
REDSHIFT_SCHEMA_SIS = 'SIS schema name'
REDSHIFT_SCHEMA_SIS_INTERNAL = 'Internal SIS schema name'
REDSHIFT_SCHEMA_STUDENT = 'Student schema name'

STUDENT_API_ID = 'secretid'
STUDENT_API_KEY = 'secretkey'
STUDENT_API_URL = 'https://secreturl.berkeley.edu/students'

TERMS_API_ID = 'secretid'
TERMS_API_KEY = 'secretkey'
TERMS_API_URL = 'https://secreturl.berkeley.edu/terms'

WORKER_HOST = 'hard-working-nessie.berkeley.edu'

# Thread queues will be ignored if "master" is embedded in the EB_ENVIRONMENT environment variable.
WORKER_QUEUE_ENABLED = True
WORKER_THREADS = 5
