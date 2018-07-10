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


"""Logic for SIS degree progress API import job."""


from flask import current_app as app
from nessie.externals import sis_degree_progress_api
from nessie.jobs.background_job import BackgroundJob
from nessie.models import json_cache
from nessie.models.student import get_all_student_ids


class ImportDegreeProgress(BackgroundJob):

    def run(self, csids=None):
        if not csids:
            csids = get_all_student_ids()
        app.logger.info(f'Starting SIS degree progress API import job for {len(csids)} students...')
        success_count = 0
        failure_count = 0

        json_cache.clear('sis_degree_progress_api_%')

        # TODO The SIS degree progress API will return useful data only for students with a UGRD current registration.
        # We get that registration from the SIS student API, which is imported concurrently with this job. Is there an
        # alternative way to filter out non-UGRD students?
        index = 1
        for csid in csids:
            app.logger.info(f'Fetching degree progress API for SID {csid} ({index} of {len(csids)}')
            if sis_degree_progress_api.get_degree_progress(csid):
                success_count += 1
            else:
                failure_count += 1
                app.logger.error(f'SIS get_degree_progress failed for CSID {csid}.')
            index += 1
        app.logger.info(f'SIS degree progress API import job completed: {success_count} succeeded, {failure_count} failed.')
        return True
