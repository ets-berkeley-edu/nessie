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


from datetime import datetime
import hashlib
import inspect

from flask import current_app as app
import pytz


"""Generic utilities."""


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
    return localize_datetime(date_to_stamp or datetime.now()).strftime('%Y-%m-%d')


def hashed_datestamp(date_to_stamp=None):
    datestamp = localized_datestamp(date_to_stamp)
    return hashlib.md5(datestamp.encode('utf-8')).hexdigest() + '-' + datestamp


def split_tsv_row(row):
    return [v if len(v) else None for v in row.split('\t')]


def vacuum_whitespace(_str):
    """Collapse multiple-whitespace sequences into a single space; remove leading and trailing whitespace."""
    if not _str:
        return None
    return ' '.join(_str.split())


def get_s3_asc_daily_path(cutoff=None):
    return app.config['LOCH_S3_ASC_DATA_PATH'] + '/daily/' + hashed_datestamp(cutoff)


def get_s3_calnet_daily_path(cutoff=None):
    return app.config['LOCH_S3_CALNET_DATA_PATH'] + '/daily/' + hashed_datestamp(cutoff)


def get_s3_canvas_daily_path():
    return app.config['LOCH_S3_CANVAS_DATA_PATH_DAILY'] + '/' + hashed_datestamp()


def get_s3_coe_daily_path(cutoff=None):
    # TODO: When COE is delivering data daily then unleash the following logic
    # return app.config['LOCH_S3_COE_DATA_PATH'] + '/daily/' + hashed_datestamp(cutoff)
    return app.config['LOCH_S3_COE_DATA_PATH']


def get_s3_sis_daily_path(cutoff=None):
    return app.config['LOCH_S3_SIS_DATA_PATH'] + '/daily/' + hashed_datestamp(cutoff)


def get_s3_sis_api_daily_path(cutoff=None):
    # Path for stashed SIS API data that doesn't need to be queried by Redshift Spectrum.
    return app.config['LOCH_S3_SIS_API_DATA_PATH'] + '/daily/' + hashed_datestamp(cutoff)


def resolve_sql_template_string(template_string, **kwargs):
    """Our DDL template files are simple enough to use standard Python string formatting."""
    s3_prefix = 's3://' + app.config['LOCH_S3_BUCKET'] + '/'
    template_data = {
        'redshift_schema_asc': app.config['REDSHIFT_SCHEMA_ASC'],
        'redshift_schema_boac': app.config['REDSHIFT_SCHEMA_BOAC'],
        'redshift_schema_calnet': app.config['REDSHIFT_SCHEMA_CALNET'],
        'redshift_schema_canvas': app.config['REDSHIFT_SCHEMA_CANVAS'],
        'redshift_schema_coe': app.config['REDSHIFT_SCHEMA_COE'],
        'redshift_schema_coe_external': app.config['REDSHIFT_SCHEMA_COE_EXTERNAL'],
        'redshift_schema_intermediate': app.config['REDSHIFT_SCHEMA_INTERMEDIATE'],
        'redshift_schema_metadata': app.config['REDSHIFT_SCHEMA_METADATA'],
        'redshift_schema_sis': app.config['REDSHIFT_SCHEMA_SIS'],
        'redshift_schema_sis_internal': app.config['REDSHIFT_SCHEMA_SIS_INTERNAL'],
        'redshift_schema_student': app.config['REDSHIFT_SCHEMA_STUDENT'],
        'redshift_iam_role': app.config['REDSHIFT_IAM_ROLE'],
        'loch_s3_asc_data_path': s3_prefix + get_s3_asc_daily_path(),
        'loch_s3_calnet_data_path': s3_prefix + get_s3_calnet_daily_path(),
        'loch_s3_canvas_data_path_today': s3_prefix + get_s3_canvas_daily_path(),
        'loch_s3_canvas_data_path_historical': s3_prefix + app.config['LOCH_S3_CANVAS_DATA_PATH_HISTORICAL'],
        'loch_s3_canvas_data_path_current_term': s3_prefix + app.config['LOCH_S3_CANVAS_DATA_PATH_CURRENT_TERM'],
        'loch_s3_coe_data_path': s3_prefix + get_s3_coe_daily_path(),
        'loch_s3_sis_data_path': s3_prefix + app.config['LOCH_S3_SIS_DATA_PATH'],
        'loch_s3_sis_api_data_path': s3_prefix + get_s3_sis_api_daily_path(),
    }
    # Kwargs may be passed in to modify default template data.
    template_data.update(kwargs)
    return template_string.format(**template_data)


def resolve_sql_template(sql_filename, **kwargs):
    with open(app.config['BASE_DIR'] + f'/nessie/sql_templates/{sql_filename}', encoding='utf-8') as file:
        template_string = file.read()
    return resolve_sql_template_string(template_string, **kwargs)
