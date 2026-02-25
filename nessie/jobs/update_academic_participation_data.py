"""
Copyright ©2025. The Regents of the University of California (Regents). All Rights Reserved.

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

import csv
import os
import re
import tempfile
from itertools import groupby

from csv_diff import compare, load_csv
from flask import current_app as app

from nessie.externals import blue_sftp, canvas_api, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.berkeley import current_term_id, term_code_for_sis_id
from nessie.lib.queries import get_sis_default_meeting_dates, get_sis_enrollments, get_sis_instructors, get_sis_sections
from nessie.lib.util import get_s3_explorance_term_export_previous_path, get_s3_explorance_term_export_timestamped_path, get_s3_explorance_term_path

"""Logic for current-term enrollments index job."""


class UpdateAcademicParticipationData(BackgroundJob):

    def run(self, use_canvas=False):  # noqa: C901, PLR0912, PLR0915
        term_id = current_term_id()
        term_prefix = term_code_for_sis_id(term_id)

        app.logger.info(f'Starting academic participation update for term {term_id} (use_canvas={use_canvas})')

        section_id_path = get_s3_explorance_term_path() + '/sources/sis_section_ids.csv'
        section_id_csv = s3.get_tsv_stream(section_id_path, zipped=False)
        if not section_id_csv:
            raise BackgroundJobError(f'Failed to retrieve section ID source CSV: {section_id_path}.')

        section_ids = [row['sis_section_id'] for row in section_id_csv]

        sis_enrollments = get_sis_enrollments(term_id, section_ids)
        sis_instructors = get_sis_instructors(term_id, section_ids)
        sis_sections = get_sis_sections(term_id, section_ids)

        sis_default_meeting_dates = {row['session_code']: row for row in get_sis_default_meeting_dates(term_id)}

        def _get_course_id(section_id):
            return '-'.join([term_prefix, str(section_id)])

        course_sites_by_section_id = {}
        if use_canvas:
            app.logger.info('Retrieving Canvas data...')

            canvas_sections_file = tempfile.NamedTemporaryFile()
            sections_report = canvas_api.get_csv_report('sections', download_path=canvas_sections_file.name, term_id=term_prefix)
            if not sections_report:
                raise BackgroundJobError(f'Could not retrieve sections report for term {term_id}')

            def _extract_section_id(row):
                if row.get('section_id'):
                    match = re.match(r'SEC:\d{4}-\w-\d{5}', row['section_id'])
                    if match:
                        return match[0]

            sections_by_id = {}
            with open(canvas_sections_file.name, 'r') as f:
                sorted_rows = sorted(csv.DictReader(f), key=lambda r: (_extract_section_id(r) or '', r['canvas_course_id']))
                for section_id, csv_rows in groupby(sorted_rows, key=_extract_section_id):
                    sections_by_id[section_id] = next(csv_rows)

            for section_id in section_ids:
                # Check for a published course site associated with the section.
                course_id = _get_course_id(section_id)
                canvas_section = sections_by_id.get('SEC:' + course_id)
                if canvas_section:
                    course_site = canvas_api.get_course_site(canvas_section['canvas_course_id'])
                    if course_site and course_site.get('workflow_state') == 'available':
                        course_sites_by_section_id[course_id] = canvas_section['sis_course_id']
            app.logger.info(f'Found {len(course_sites_by_section_id)} course sites for {len(section_ids)} sections')

        # sources/canvasproject.csv is an export of responses from Blue, from which we collect instructor opt-outs.
        blue_responses = s3.get_tsv_stream(f'{get_s3_explorance_term_path()}/sources/canvasproject.csv', delimiter=',', zipped=False)
        project_opt_outs = {r['Subject']: r['Opted In'] for r in blue_responses}

        courses = []
        course_ids = set()

        # Fetch last export, if any, and update department form and Canvas course ID as needed.
        self.last_export = get_s3_explorance_term_export_previous_path()
        if self.last_export:
            app.logger.info(f'Fetching previous export ({self.last_export}courses.csv)...')
            for row in s3.get_tsv_stream(f'{self.last_export}courses.csv', delimiter=',', zipped=False):
                course_id = row['course_id']
                # Update opt-out status for existing courses
                if project_opt_outs.get('Data2_' + course_id) == 'No':
                    row['evaluation_type'] = 'OO'
                    row['dept_form'] = 'AP_EMAIL'

                # Update department form and canvas course ID
                if course_id in course_sites_by_section_id:
                    row['dept_form'] = 'AP'
                    row['canvas_course_id'] = course_sites_by_section_id[course_id]
                elif row.get('canvas_course_id'):
                    row['dept_form'] = 'AP'
                else:
                    row['dept_form'] = 'AP_EMAIL'
                courses.append(row)
                course_ids.add(course_id)

        # Append any sections not included in the previous export.
        app.logger.info('Building courses export...')
        for sis_section in sis_sections:
            course_id = _get_course_id(sis_section['sis_section_id'])
            default_dates = sis_default_meeting_dates.get(sis_section['session_code'])
            if course_id not in course_ids:
                courses.append({
                    'course_id': course_id,
                    'course_id_2': course_id,
                    'course_name': sis_section['sis_course_name'],
                    'cross_listed_flag': '',
                    'cross_listed_name': '',
                    'dept_name': '',
                    'catalog_id': '',
                    'instruction_format': sis_section['sis_instruction_format'],
                    'section_num': sis_section['sis_section_num'],
                    'primary_secondary_cd': '',
                    'evaluate': '',
                    'dept_form': 'AP' if course_id in course_sites_by_section_id else 'AP_EMAIL',
                    'evaluation_type': 'OO' if project_opt_outs.get('Data2_' + course_id) == 'No' else '',
                    'modular_course': '',
                    'start_date': (sis_section['meeting_start_date'] or default_dates['start_date']) + ' 00:00:00',
                    'end_date': (sis_section['meeting_end_date'] or default_dates['end_date']) + ' 00:00:00',
                    'canvas_course_id': course_sites_by_section_id.get(course_id, ''),
                    'qb_mapping': '',
                })
                course_ids.add(course_id)

        instructors = []
        course_instructors = []
        instructor_uids = set()

        app.logger.info('Building instructor export...')
        for instructor_assignment in sis_instructors:
            if instructor_assignment['ldap_uid'] not in instructor_uids:
                instructors.append({
                    'ldap_uid': instructor_assignment['ldap_uid'],
                    'sis_id': instructor_assignment['sis_id'],
                    'first_name': instructor_assignment['first_name'],
                    'last_name': instructor_assignment['last_name'],
                    'email_address': instructor_assignment['email_address'],
                    'blue_role': 23,
                })
                instructor_uids.add(instructor_assignment['ldap_uid'])

            course_instructors.append({
                'course_id': _get_course_id(instructor_assignment['sis_section_id']),
                'ldap_uid': instructor_assignment['ldap_uid'],
                'role': 'instructor',
            })

        # In order for a survey to be created in Blue, there must be at least one instructor.
        # Adding Oski to all courses ensures this requirement is satisfied.
        instructors.append({
            'ldap_uid': 12345,
            'sis_id': 'UID:12345',
            'first_name': 'Oski',
            'last_name': 'Bear',
            'email_address': '',
            'blue_role': 23,
        })

        for course_id in course_ids:
            course_instructors.append({
                'course_id': course_id,
                'ldap_uid': 12345,
                'role': 'instructor',
            })

        students = []
        course_students = []
        student_uids = set()

        app.logger.info('Building student export...')
        for enrollment in sis_enrollments:
            if enrollment['ldap_uid'] not in student_uids:
                students.append({
                    'ldap_uid': enrollment['ldap_uid'],
                    'sis_id': enrollment['sis_id'],
                    'first_name': enrollment['first_name'],
                    'last_name': enrollment['last_name'],
                    'email_address': enrollment['email_address'],
                })
                student_uids.add(enrollment['ldap_uid'])

            course_students.append({
                'course_id': _get_course_id(enrollment['sis_section_id']),
                'ldap_uid': enrollment['ldap_uid'],
            })

        app.logger.info('Uploading CSVs...')

        self.upload_path = get_s3_explorance_term_export_timestamped_path()
        self.diff_results = {}

        with blue_sftp.get_sftp_client() as sftp:
            self._export_csv(sftp, courses, COURSE_HEADERS, 'courses.csv')
            self._export_csv(sftp, instructors, INSTRUCTOR_HEADERS, 'instructors.csv')
            self._export_csv(sftp, students, STUDENT_HEADERS, 'students.csv')
            self._export_csv(sftp, course_instructors, COURSE_INSTRUCTOR_HEADERS, 'course_instructor.csv')
            self._export_csv(sftp, course_students, COURSE_STUDENT_HEADERS, 'course_student.csv')

        return f'Academic participation updated for term {term_id} (use_canvas={use_canvas}). {self.diff_results or "No changes."}'

    def _export_csv(self, sftp, rows, headers, filename):
        tmpfile = tempfile.NamedTemporaryFile()

        with open(tmpfile.name, mode='wt', encoding='utf-8') as f:
            csv_writer = csv.DictWriter(f, fieldnames=headers)
            csv_writer.writeheader()
            csv_writer.writerows(rows)

        filesize = os.stat(tmpfile.name).st_size
        with open(tmpfile.name, mode='rb') as f:
            try:
                if sftp:
                    sftp.putfo(f, filename.replace('.csv', f"{app.config['BLUE_SFTP_SUFFIX']}.csv"), file_size=filesize)
            except Exception as e:
                app.logger.exception(f'SFTP upload failed ({filename}.csv, {filesize} bytes); aborting further uploads.', exc_info=e)
                raise BackgroundJobError(f'Could not upload {filename}.csv')

            f.seek(0)
            s3.upload_file(f, self.upload_path + '/' + filename)

        try:
            if self.last_export:
                diff_key = 'course_id' if filename == 'courses.csv' else 'ldap_uid'
                previous_file = s3.get_text_reader(self.last_export + filename)

                with open(tmpfile.name, mode='r') as f:
                    csv_diff = compare(
                        load_csv(previous_file, key=diff_key),
                        load_csv(f, key=diff_key),
                    )
            else:
                csv_diff = {'added': rows, 'removed': [], 'changed': []}

            if len(csv_diff['added']) or len(csv_diff['removed']) or len(csv_diff['changed']):
                self.diff_results[filename.replace('.csv', '')] = {}
                for key in ('added', 'removed', 'changed'):
                    if len(csv_diff[key]):
                        self.diff_results[filename.replace('.csv', '')][key] = len(csv_diff[key])
        except Exception as e:
            app.logger.exception(f'Failed to generate diff ({filename}.csv), continuing.', exc_info=e)


COURSE_HEADERS = [
    'course_id', 'course_id_2', 'course_name', 'cross_listed_flag',
    'cross_listed_name', 'dept_name', 'catalog_id', 'instruction_format',
    'section_num', 'primary_secondary_cd', 'evaluate', 'dept_form',
    'evaluation_type', 'modular_course', 'start_date', 'end_date',
    'canvas_course_id', 'qb_mapping',
]


INSTRUCTOR_HEADERS = ['ldap_uid', 'sis_id', 'first_name', 'last_name', 'email_address', 'blue_role']


STUDENT_HEADERS = ['ldap_uid', 'sis_id', 'first_name', 'last_name', 'email_address']


COURSE_INSTRUCTOR_HEADERS = ['course_id', 'ldap_uid', 'role']


COURSE_STUDENT_HEADERS = ['course_id', 'ldap_uid']
