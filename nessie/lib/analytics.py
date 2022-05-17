"""
Copyright ©2022. The Regents of the University of California (Regents). All Rights Reserved.

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

from itertools import groupby
import json
import math

from flask import current_app as app
from nessie.externals.redshift import copy_for_pandas
from nessie.lib.util import write_to_tsv_file
from numpy import nan
import pandas
from scipy.stats import percentileofscore


def generate_analytics_feeds_for_course(output_file, term_id, canvas_site_row, site_enrollments_stream, site_submissions_stream):
    count = 0

    course_id = int(canvas_site_row.get('canvas_course_id'))
    sis_sections = canvas_site_row.get('sis_section_ids')
    if sis_sections:
        sis_sections = set([s for s in sis_sections.split(',')])
    else:
        sis_sections = set()

    enrollments = copy_for_pandas(site_enrollments_stream)
    if not enrollments:
        return 0

    df = pandas.DataFrame(enrollments, columns=['canvas_user_id', 'current_score', 'last_activity_at'])
    metrics = ['current_score', 'last_activity_at']
    course_distributions = get_distributions_for_metric(df, metrics)
    course_analytics = {metric: analytics_for_course(course_distributions, metric) for metric in metrics}

    submissions_by_user_id = groupby(site_submissions_stream, lambda r: int(r['reference_user_id']))
    submission_tracker = {'user_id': 0, 'submissions': []}

    for enrollment in enrollments:
        user_id = int(enrollment['canvas_user_id'])
        df_enrollment = df.loc[df['canvas_user_id'].values == user_id]

        analytics_feed = _generate_analytics_feed(df_enrollment, course_analytics, course_distributions, len(enrollments))

        while submission_tracker['user_id'] < user_id:
            submission_tracker['user_id'], submission_tracker['submissions'] = next(submissions_by_user_id, (user_id, []))
        if submission_tracker['user_id'] == user_id:
            submission_rows = submission_tracker['submissions']
        else:
            submission_rows = []
        analytics_feed['assignmentsSubmitted'] = _generate_submission_analytics(course_id, user_id, submission_rows)

        canvas_site_feed = {
            'canvasCourseId': course_id,
            'courseName': canvas_site_row.get('canvas_course_name'),
            'courseCode': canvas_site_row.get('canvas_course_code'),
            'courseTerm': canvas_site_row.get('canvas_course_term'),
            'analytics': analytics_feed,
        }

        enrolled_sections = enrollment.get('sis_section_ids')
        if enrolled_sections:
            enrolled_sections = ','.join(sorted(sis_sections.intersection([s for s in enrolled_sections.split(',')])))

        write_to_tsv_file(
            output_file,
            [
                enrollment['sid'],
                term_id,
                enrolled_sections,
                json.dumps(canvas_site_feed),
            ],
        )
        count += 1

    return count


def _generate_analytics_feed(student_row, course_analytics, course_distributions, enrollment_count):
    return {
        'currentScore': analytics_for_student(
            student_row,
            'current_score',
            course_analytics,
            course_distributions,
        ),
        'lastActivity': analytics_for_student(
            student_row,
            'last_activity_at',
            course_analytics,
            course_distributions,
        ),
        'courseEnrollmentCount': enrollment_count,
    }


def _generate_submission_analytics(canvas_course_id, canvas_user_id, submission_rows):
    submissions = copy_for_pandas(submission_rows)
    df = pandas.DataFrame(submissions, columns=['canvas_user_id', 'submissions_turned_in'])
    student_row = df.loc[df['canvas_user_id'].values == int(canvas_user_id)]
    if submissions and student_row.empty:
        app.logger.warn(f'Canvas user id {canvas_user_id}, course id {canvas_course_id} not found in Data Loch assignments; will assume 0 score')
        df, student_row = append_missing_row(
            df,
            canvas_user_id,
            pandas.DataFrame({
                'canvas_user_id': [int(canvas_user_id)],
                'submissions_turned_in': [0],
            }),
        )
    course_distributions = get_distributions_for_metric(df, ['submissions_turned_in'])
    course_analytics = {'submissions_turned_in': analytics_for_course(course_distributions, 'submissions_turned_in')}
    return analytics_for_student(
        student_row,
        'submissions_turned_in',
        course_analytics,
        course_distributions,
    )


def append_missing_row(df, canvas_user_id, student_row):
    df = df.append(student_row, ignore_index=True)
    # Fetch newly appended row, mostly for the sake of its properly set-up index.
    student_row = df.loc[df['canvas_user_id'].values == int(canvas_user_id)]
    return df, student_row


def get_distributions_for_metric(df, metrics):
    distributions = {}
    for metric in metrics:
        distributions[metric] = {
            'dfcol': df[metric],
        }
        # If no data exists for a column, the Pandas 'nunique' function reports zero unique values.
        # However, some feeds (such as Canvas student summaries) return (mostly) zero values rather than empty lists,
        # and we've also seen some Canvas feeds which mix nulls and zeroes.
        # Setting non-numbers to zero works acceptably for most current analyzed feeds, apart from lastActivity (see below).
        distributions[metric]['dfcol'].fillna(0, inplace=True)
        distributions[metric]['unique_scores'] = distributions[metric]['dfcol'].unique().tolist()
        # When calculating z-scores and means for lastActivity, zeroed-out "no activity" values must be dropped, since zeros
        # and Unix timestamps don't play well in the same distribution. We retain the original dataset for intuitive-percentile
        # calculation: the course mean's intuitive percentile must match that of any real student who happens to have the same
        # raw value.
        if metric == 'last_activity_at':
            distributions[metric]['dfcol_normalized'] = distributions[metric]['dfcol'].replace(0, nan).dropna()
        else:
            distributions[metric]['dfcol_normalized'] = distributions[metric]['dfcol']
    return distributions


def analytics_for_course(distributions, metric):
    dfcol = distributions[metric]['dfcol']
    dfcol_normalized = distributions[metric]['dfcol_normalized']
    unique_scores = distributions[metric]['unique_scores']

    nunique = dfcol.nunique()
    if nunique == 0 or (nunique == 1 and dfcol.max() == 0.0):
        return {
            'boxPlottable': False,
            'student': {
                'percentile': None,
                'raw': None,
                'roundedUpPercentile': None,
            },
            'courseDeciles': None,
            'courseMean': None,
            'displayPercentile': None,
        }

    # If only ten or fewer values are shared across the student population, the 'universal' percentile figure and the
    # box-and-whisker graph will usually look odd. With such sparse data sets, a text summary and an (optional)
    # histogram are more readable.
    box_plottable = (nunique > 10)

    column_quantiles = quantiles(dfcol, 10)

    course_mean = dfcol_normalized.mean()
    if course_mean and not math.isnan(course_mean):
        # Spoiler: this will be '50.0'.
        comparative_percentile_of_mean = zptile(zscore(dfcol_normalized, course_mean))
        intuitive_percentile_of_mean = int(percentileofscore(dfcol.tolist(), course_mean, kind='weak'))
        matrixy_comparative_percentile_of_mean = percentileofscore(unique_scores, course_mean, kind='strict')
    else:
        comparative_percentile_of_mean = None
        matrixy_comparative_percentile_of_mean = None
        intuitive_percentile_of_mean = None

    return {
        'boxPlottable': box_plottable,
        'courseDeciles': column_quantiles,
        'courseMean': {
            'matrixyPercentile': matrixy_comparative_percentile_of_mean,
            'percentile': comparative_percentile_of_mean,
            'raw': course_mean,
            'roundedUpPercentile': intuitive_percentile_of_mean,
        },
    }


def analytics_for_student(student_row, metric, course_analytics, distributions):
    # If the course had no salient data, we've already filled in a placeholder student element and are done.
    if course_analytics[metric].get('student'):
        return course_analytics[metric]

    dfcol = distributions[metric]['dfcol']
    dfcol_normalized = distributions[metric]['dfcol_normalized']
    unique_scores = distributions[metric]['unique_scores']

    student_row = student_row.fillna(0)
    intuitive_percentile = rounded_up_percentile(dfcol, student_row)
    # The intuitive percentile is our best option for display, whether or not the distribution is boxplottable.
    # Note, however, that if all students have the same score, then all students are in the "100th percentile."
    display_percentile = ordinal(intuitive_percentile)

    column_value = student_row[metric].values[0]
    raw_value = round(column_value.item())

    column_zscore = zscore(dfcol_normalized, column_value)
    comparative_percentile = zptile(column_zscore)
    # For purposes of matrix plotting, improve visual spread by calculating percentile against a range of unique scores.
    matrixy_comparative_percentile = percentileofscore(unique_scores, column_value, kind='strict')

    student_analytics = {
        'student': {
            'matrixyPercentile': matrixy_comparative_percentile,
            'percentile': comparative_percentile,
            'raw': raw_value,
            'roundedUpPercentile': intuitive_percentile,
        },
        'displayPercentile': display_percentile,
    }

    student_analytics.update(course_analytics[metric])
    return student_analytics


def ordinal(nbr):
    rounded = round(nbr)
    mod_ten = rounded % 10
    if (mod_ten == 1) and (rounded != 11):
        suffix = 'st'
    elif (mod_ten == 2) and (rounded != 12):
        suffix = 'nd'
    elif (mod_ten == 3) and (rounded != 13):
        suffix = 'rd'
    else:
        suffix = 'th'
    return f'{rounded}{suffix}'


def quantiles(series, count):
    """Return a given number of evenly spaced quantiles for a given series."""
    return [round(series.quantile(n / count)) for n in range(0, count + 1)]


def rounded_up_percentile(dataframe, student_row):
    """Given a dataframe and an individual student row, return a more easily understood meaning of percentile.

    Z-score percentile is useful in a scatterplot to spot outliers in the overall population across contexts.
    (If 90% of the course's students received a score of '5', then one student with a '5' is not called out.)
    Rounded-up matches what non-statisticians would expect when viewing one particular student in one
    particular course context. (If only 10% of the course's students did better than '5', then this student
    with a '5' is in the 90th percentile.)
    """
    percentile = dataframe.rank(pct=True, method='max')[student_row.index].values[0]
    percentile = int(percentile * 100)
    return percentile


def zptile(z_score):
    """Derive percentile from zscore."""
    if z_score is None:
        return None
    else:
        return round(50 * (math.erf(z_score / 2 ** .5) + 1))


def zscore(dataframe, value):
    """Given a dataframe and an individual value, return a zscore."""
    if dataframe.std(ddof=0) == 0:
        return None
    else:
        return (value - dataframe.mean()) / dataframe.std(ddof=0)
