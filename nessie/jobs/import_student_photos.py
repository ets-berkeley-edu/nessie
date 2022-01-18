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

from time import sleep

from flask import current_app as app
from nessie.externals import s3
from nessie.externals.cal1card_photo_api import get_cal1card_photo
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.lib.metadata import update_photo_import_status
from nessie.lib.queries import get_active_student_ids, get_sids_with_photos

"""Logic for student photo import job."""


class ImportStudentPhotos(BackgroundJob):

    rds_schema = app.config['RDS_SCHEMA_STUDENT']

    def run(self):
        students_without_photos = _get_students_without_photos()

        app.logger.info(f'Starting student photo import job for {len(students_without_photos)} students...')

        successes = []
        failures = []
        photo_not_found = []
        index = 0

        for csid in students_without_photos.keys():
            index += 1
            app.logger.info(f'Fetching photo for SID {csid}, ({index} of {len(students_without_photos)})')
            uid = students_without_photos.get(csid)
            if not uid:
                app.logger.error(f'No UID found for SID {csid}.')
                failures.append(csid)
                continue

            photo = get_cal1card_photo(uid)
            if photo:
                s3_photo_key = f"{app.config['LOCH_S3_CAL1CARD_PHOTOS_PATH']}/{uid}.jpg"
                if s3.upload_data(photo, s3_photo_key, bucket=app.config['LOCH_S3_PUBLIC_BUCKET']):
                    successes.append(csid)
                else:
                    app.logger.error(f'Photo upload failed for SID {csid}.')
                    failures.append(csid)
            elif photo is False:
                app.logger.info(f'No photo returned for SID {csid}.')
                photo_not_found.append(csid)
            elif photo is None:
                app.logger.error(f'Photo import failed for SID {csid}.')
                failures.append(csid)

            sleep(app.config['CAL1CARD_PHOTO_API_THROTTLE'])

        if (len(successes) == 0) and (len(photo_not_found) == 0) and (len(failures) > 0):
            raise BackgroundJobError('Failed to import student photos.')
        else:
            update_photo_import_status(successes, failures, photo_not_found)
            status = 'Student photo import completed: '
            if len(successes):
                status += f'{len(successes)} succeeded, '
            if len(photo_not_found):
                status += f'{len(photo_not_found)} had no photo available, '
            status += f'{len(failures)} failed.'
            return status


def _get_students_without_photos():
    active_student_ids = get_active_student_ids()
    previous_imports = {r['sid'] for r in get_sids_with_photos()}
    return {r['sid']: r['ldap_uid'] for r in active_student_ids if r['sid'] not in previous_imports}
