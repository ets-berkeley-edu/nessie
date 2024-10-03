"""
Copyright ©2024. The Regents of the University of California (Regents). All Rights Reserved.

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

from datetime import date, datetime, timedelta
import hashlib
import inspect
import re

from dateutil.rrule import DAILY, rrule
from flask import current_app as app
from nessie.lib.berkeley import earliest_term_id
import pytz

"""Generic utilities."""


def encoded_tsv_row(elements):
    def _to_tsv_string(e):
        if e is None:
            return ''
        else:
            return str(e)
    return '\t'.join([_to_tsv_string(e) for e in elements]).encode()


def write_to_tsv_file(f, elements):
    f.write(encoded_tsv_row(elements) + b'\n')
    return 1


def fill_pattern_from_args(pattern, func, *args, **kw):
    return pattern.format(**get_args_dict(func, *args, **kw))


def get_args_dict(func, *args, **kw):
    arg_names = inspect.getfullargspec(func)[0]
    resp = dict(zip(arg_names, args))
    resp.update(kw)
    return resp


def localize_datetime(dt):
    return dt.astimezone(pytz.timezone(app.config['TIMEZONE']))


def localized_datestamp(date_to_stamp=None):
    if not date_to_stamp:
        date_to_stamp = datetime.now()
    elif not hasattr(date_to_stamp, 'astimezone'):
        date_to_stamp = datetime(date_to_stamp.year, date_to_stamp.month, date_to_stamp.day)
    return localize_datetime(date_to_stamp).strftime('%Y-%m-%d')


def hashed_datestamp(date_to_stamp=None):
    datestamp = localized_datestamp(date_to_stamp)
    return hashlib.md5(datestamp.encode('utf-8')).hexdigest() + '-' + datestamp


# As described in NS-993, the SIS note attachments we receive don't always follow
# the naming convention { EMPLID }_{ SAA_NOTE_ID }_{ SAA_SEQ_NBR }{ .ext }
# and need to be brought into compliance.
def normalize_sis_note_attachment_file_name(path):
    file_name = path.split('/')[-1]
    match = re.search(r'(^\d+_\d+_\d+)(.*?)(\.\w{1,8})??$', file_name)
    if match is None:
        app.logger.warning(f'Encountered an unexpected file name pattern: {file_name}')
        return file_name
    corrected_file_name, discard_chars, file_ext = match.groups()
    if file_ext is None:
        file_ext = ''
    return f'{corrected_file_name}{file_ext}'


def split_tsv_row(row):
    return tuple([v if len(v) else None for v in row.decode().split('\t')])


def vacuum_whitespace(_str):
    """Collapse multiple-whitespace sequences into a single space; remove leading and trailing whitespace."""
    if not _str:
        return None
    return ' '.join(_str.split())


def get_s3_asc_daily_path(cutoff=None):
    return app.config['LOCH_S3_ASC_DATA_PATH'] + '/daily/' + hashed_datestamp(cutoff)


def get_s3_boa_api_daily_path(cutoff=None):
    return app.config['LOCH_S3_BOA_DATA_API_PATH'] + '/daily/' + hashed_datestamp(cutoff)


def get_s3_boa_rds_data_daily_path(cutoff=None):
    return app.config['LOCH_S3_BOA_RDS_DATA_PATH_DAILY'] + '/' + localized_datestamp(cutoff) + '/public'


def get_s3_boac_analytics_incremental_path(cutoff=None):
    return app.config['LOCH_S3_BOAC_ANALYTICS_DATA_PATH'] + '/incremental/' + hashed_datestamp(cutoff)


def get_s3_calnet_daily_path(cutoff=None):
    return app.config['LOCH_S3_CALNET_DATA_PATH'] + '/daily/' + hashed_datestamp(cutoff)


def get_s3_canvas_daily_path(cutoff=None):
    return app.config['LOCH_S3_CANVAS_DATA_PATH_DAILY'] + '/' + hashed_datestamp(cutoff)


def get_s3_canvas_data_2_daily_path(cutoff=None):
    return app.config['LOCH_S3_CANVAS_DATA_2_PATH_DAILY'] + '/' + localized_datestamp(cutoff)


def get_s3_coe_daily_path(cutoff=None):
    # TODO: When COE is delivering data daily then unleash the following logic
    # return app.config['LOCH_S3_COE_DATA_PATH'] + '/daily/' + hashed_datestamp(cutoff)
    return app.config['LOCH_S3_COE_DATA_PATH']


def get_s3_edl_daily_path(cutoff=None):
    return app.config['LOCH_S3_EDL_DATA_PATH'] + '/daily/' + hashed_datestamp(cutoff)


def get_s3_oua_daily_path(cutoff=None):
    return app.config['LOCH_S3_OUA_DATA_PATH'] + '/daily/' + hashed_datestamp(cutoff)


def get_s3_piazza_data_path(archive=None):
    # archives need to be, or become, one of the piazza zip file names, without .zip, i.e. type_yyyy-mm-dd
    if archive == 'latest' or archive is None:
        # we have decided to use 'yesterday' (local time, as the default for 'latest')
        archive = (date.today() - timedelta(days=1)).strftime('daily_%Y-%m-%d')
    match = re.match(r'(\w+)\_(\d{4}\-\d{2}\-\d{2})', archive)
    frequency = match[1]
    datestamp = match[2]
    return frequency, datestamp, archive, app.config['LOCH_S3_PIAZZA_DATA_PATH'] + '/' + archive.replace('-', '/').replace('_', '/')


def get_s3_sis_attachment_current_paths(begin_dt=None):
    if not begin_dt:
        paths = ['']
    else:
        # Calculate a range of dates between begin_dt (inclusive) and today (exclusive).
        # Ensure both dates are in UTC before converting to local time.
        start_date = localize_datetime(pytz.utc.localize(begin_dt)).date()
        today = localize_datetime(pytz.utc.localize(datetime.utcnow())).date()
        paths = [d.strftime('%Y/%m/%d') for d in rrule(DAILY, dtstart=start_date, until=today)]
    return ['/'.join([app.config['LOCH_S3_ADVISING_NOTE_ATTACHMENT_SOURCE_PATH'], path]) for path in paths]


def get_s3_sis_attachment_path(datestamp):
    if datestamp == 'all':
        return [app.config['LOCH_S3_ADVISING_NOTE_ATTACHMENT_SOURCE_PATH']]
    if datestamp:
        return ['/'.join([app.config['LOCH_S3_ADVISING_NOTE_ATTACHMENT_SOURCE_PATH']] + datestamp.split('-'))]


def get_s3_sis_api_daily_path(cutoff=None):
    # Path for stashed SIS API data that doesn't need to be queried by Redshift Spectrum.
    return app.config['LOCH_S3_SIS_API_DATA_PATH'] + '/daily/' + hashed_datestamp(cutoff)


def get_s3_sis_daily_path(cutoff=None):
    return app.config['LOCH_S3_SIS_DATA_PATH'] + '/daily/' + hashed_datestamp(cutoff)


def get_s3_sis_sysadm_daily_path(cutoff=None):
    return app.config['LOCH_S3_SIS_DATA_PATH'] + '/sis-sysadm/daily/' + hashed_datestamp(cutoff)


def get_s3_ycbm_daily_path(cutoff=None):
    return app.config['LOCH_S3_YCBM_DATA_PATH'] + '/daily/' + hashed_datestamp(cutoff)


def resolve_sql_template_string(template_string, **kwargs):
    """Our DDL template files are simple enough to use standard Python string formatting."""
    s3_prefix = 's3://' + app.config['LOCH_S3_BUCKET'] + '/'
    s3_protected_prefix = 's3://' + app.config['LOCH_S3_PROTECTED_BUCKET'] + '/'
    template_data = {
        'earliest_academic_history_term_id': app.config['EARLIEST_ACADEMIC_HISTORY_TERM_ID'],
        'earliest_term_id': earliest_term_id(),
        'edl_iam_role': app.config['AWS_EDL_ROLE_ARN'],
        'history_dept_notes_default_advisor_uid': app.config['HISTORY_DEPT_NOTES_DEFAULT_ADVISOR_UID'],
        'rds_app_boa_user': app.config['RDS_APP_BOA_USER'],
        'rds_app_ripley_user': app.config['RDS_APP_RIPLEY_USER'],
        'rds_dblink_role_damien': app.config['RDS_DBLINK_ROLE_DAMIEN'],
        'rds_dblink_role_diablo': app.config['RDS_DBLINK_ROLE_DIABLO'],
        'rds_dblink_to_redshift': app.config['REDSHIFT_DATABASE'] + '_redshift',
        'rds_schema_advising_appointments': app.config['RDS_SCHEMA_ADVISING_APPOINTMENTS'],
        'rds_schema_advising_notes': app.config['RDS_SCHEMA_ADVISING_NOTES'],
        'rds_schema_advisor': app.config['RDS_SCHEMA_ADVISOR'],
        'rds_schema_asc': app.config['RDS_SCHEMA_ASC'],
        'rds_schema_boac': app.config['RDS_SCHEMA_BOAC'],
        'rds_schema_coe': app.config['RDS_SCHEMA_COE'],
        'rds_schema_data_science': app.config['RDS_SCHEMA_DATA_SCIENCE'],
        'rds_schema_e_i': app.config['RDS_SCHEMA_E_I'],
        'rds_schema_eop': app.config['RDS_SCHEMA_EOP'],
        'rds_schema_history_dept': app.config['RDS_SCHEMA_HISTORY_DEPT'],
        'rds_schema_metadata': app.config['RDS_SCHEMA_METADATA'],
        'rds_schema_oua': app.config['RDS_SCHEMA_OUA'],
        'rds_schema_sis_advising_notes': app.config['RDS_SCHEMA_SIS_ADVISING_NOTES'],
        'rds_schema_sis_internal': app.config['RDS_SCHEMA_SIS_INTERNAL'],
        'rds_schema_terms': app.config['RDS_SCHEMA_TERMS'],
        'rds_schema_student': app.config['RDS_SCHEMA_STUDENT'],
        'redshift_app_boa_user': app.config['REDSHIFT_APP_BOA_USER'],
        'redshift_dblink_group': app.config['REDSHIFT_DBLINK_GROUP'],
        'redshift_schema_advisor': app.config['REDSHIFT_SCHEMA_ADVISOR'],
        'redshift_schema_advisor_internal': app.config['REDSHIFT_SCHEMA_ADVISOR_INTERNAL'],
        'redshift_schema_asc': app.config['REDSHIFT_SCHEMA_ASC'],
        'redshift_schema_asc_advising_notes': app.config['REDSHIFT_SCHEMA_ASC_ADVISING_NOTES'],
        'redshift_schema_asc_advising_notes_internal': app.config['REDSHIFT_SCHEMA_ASC_ADVISING_NOTES_INTERNAL'],
        'redshift_schema_boa_rds_data': app.config['REDSHIFT_SCHEMA_BOA_RDS_DATA'],
        'redshift_schema_boac': app.config['REDSHIFT_SCHEMA_BOAC'],
        'redshift_schema_canvas': app.config['REDSHIFT_SCHEMA_CANVAS'],
        'redshift_schema_canvas_data_2': app.config['REDSHIFT_SCHEMA_CANVAS_DATA_2'],
        'redshift_schema_coe': app.config['REDSHIFT_SCHEMA_COE'],
        'redshift_schema_coe_external': app.config['REDSHIFT_SCHEMA_COE_EXTERNAL'],
        'redshift_schema_data_science_advising': app.config['REDSHIFT_SCHEMA_DATA_SCIENCE_ADVISING'],
        'redshift_schema_data_science_advising_internal': app.config['REDSHIFT_SCHEMA_DATA_SCIENCE_ADVISING_INTERNAL'],
        'redshift_schema_e_i_advising_notes': app.config['REDSHIFT_SCHEMA_E_I_ADVISING_NOTES'],
        'redshift_schema_e_i_advising_notes_internal': app.config['REDSHIFT_SCHEMA_E_I_ADVISING_NOTES_INTERNAL'],
        'redshift_schema_edl': app.config['REDSHIFT_SCHEMA_EDL'],
        'redshift_schema_edl_external': app.config['REDSHIFT_SCHEMA_EDL_EXTERNAL'],
        'redshift_schema_edl_external_edw': app.config['REDSHIFT_SCHEMA_EDL_EXTERNAL_EDW'],
        'redshift_schema_edl_external_staging': app.config['REDSHIFT_SCHEMA_EDL_EXTERNAL_STAGING'],
        'redshift_schema_eop_advising_notes': app.config['REDSHIFT_SCHEMA_EOP_ADVISING_NOTES'],
        'redshift_schema_eop_advising_notes_internal': app.config['REDSHIFT_SCHEMA_EOP_ADVISING_NOTES_INTERNAL'],
        'redshift_schema_gradescope': app.config['REDSHIFT_SCHEMA_GRADESCOPE'],
        'redshift_schema_history_dept_advising': app.config['REDSHIFT_SCHEMA_HISTORY_DEPT_ADVISING'],
        'redshift_schema_history_dept_advising_internal': app.config['REDSHIFT_SCHEMA_HISTORY_DEPT_ADVISING_INTERNAL'],
        'redshift_schema_intermediate': app.config['REDSHIFT_SCHEMA_INTERMEDIATE'],
        'redshift_schema_lrs_external': app.config['REDSHIFT_SCHEMA_LRS'],
        'redshift_schema_oua': app.config['REDSHIFT_SCHEMA_OUA'],
        'redshift_schema_sisedo': app.config['REDSHIFT_SCHEMA_SISEDO'],
        'redshift_schema_sisedo_internal': app.config['REDSHIFT_SCHEMA_SISEDO_INTERNAL'],
        'redshift_schema_student': app.config['REDSHIFT_SCHEMA_STUDENT'],
        'redshift_schema_ycbm': app.config['REDSHIFT_SCHEMA_YCBM'],
        'redshift_schema_ycbm_internal': app.config['REDSHIFT_SCHEMA_YCBM_INTERNAL'],
        'redshift_iam_role': app.config['REDSHIFT_IAM_ROLE'],
        'loch_s3_asc_data_path': s3_prefix + get_s3_asc_daily_path(),
        'loch_s3_boa_rds_path_daily': s3_prefix + get_s3_boa_rds_data_daily_path(),
        'loch_s3_boac_analytics_incremental_path': s3_prefix + get_s3_boac_analytics_incremental_path(),
        'loch_s3_calnet_data_path': s3_prefix + get_s3_calnet_daily_path(),
        'loch_s3_canvas_data_path_today': s3_prefix + get_s3_canvas_daily_path(),
        'loch_s3_canvas_data_2_path_today': s3_prefix + get_s3_canvas_data_2_daily_path(),
        'loch_s3_canvas_data_path_historical': s3_prefix + app.config['LOCH_S3_CANVAS_DATA_PATH_HISTORICAL'],
        'loch_s3_coe_data_path': s3_prefix + get_s3_coe_daily_path(),
        'loch_s3_dsa_data_path': s3_protected_prefix + app.config['LOCH_S3_DSA_DATA_PATH'],
        'loch_s3_e_i_aggregated_notes': s3_prefix + app.config['LOCH_S3_E_I_DATA_PATH'] + '/aggregated_notes',
        'loch_s3_e_i_aggregated_topics': s3_prefix + app.config['LOCH_S3_E_I_DATA_PATH'] + '/aggregated_topics',
        'loch_s3_edl_data_path': s3_prefix + app.config['LOCH_S3_EDL_DATA_PATH'],
        'loch_s3_edl_data_path_today': s3_prefix + get_s3_edl_daily_path(),
        'loch_s3_eop_data_path': s3_prefix + app.config['LOCH_S3_EOP_DATA_PATH'],
        'loch_s3_gradescope_data_path': s3_prefix + app.config['LOCH_S3_GRADESCOPE_DATA_PATH'],
        'loch_s3_history_dept_data_path': s3_protected_prefix + app.config['LOCH_S3_HISTORY_DEPT_DATA_PATH'],
        'loch_s3_oua_data_path': s3_protected_prefix + get_s3_oua_daily_path(),
        'loch_s3_slate_sftp_path': s3_protected_prefix + app.config['LOCH_S3_SLATE_DATA_SFTP_PATH'],
        'loch_s3_sis_data_path': s3_prefix + app.config['LOCH_S3_SIS_DATA_PATH'],
        'loch_s3_sis_api_data_path': s3_prefix + get_s3_sis_api_daily_path(),
        'loch_s3_ycbm_data_path': s3_prefix + app.config['LOCH_S3_YCBM_DATA_PATH'],
    }
    # Kwargs may be passed in to modify default template data.
    template_data.update(kwargs)
    return template_string.format(**template_data)


def resolve_sql_template(sql_filename, **kwargs):
    with open(app.config['BASE_DIR'] + f'/nessie/sql_templates/{sql_filename}', encoding='utf-8') as file:
        template_string = file.read()
    # Let's leave the preprended copyright and license text out of this.
    template_string = re.sub(r'^/\*.*?\*/\s*', '', template_string, flags=re.DOTALL)
    return resolve_sql_template_string(template_string, **kwargs)


def to_boolean(s):
    try:
        if isinstance(s, str):
            return s.lower().startswith('t')
        else:
            return bool(s)
    except (TypeError, ValueError):
        return False


def to_float(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def utc_now():
    return datetime.utcnow().replace(tzinfo=pytz.utc)
