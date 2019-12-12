"""
Copyright ©2020. The Regents of the University of California (Regents). All Rights Reserved.

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

import os

from nessie import factory
from tests.util import override_config


class TestFactory:

    def test_enable_scheduling_through_config(self, app):
        with override_config(app, 'JOB_SCHEDULING_ENABLED', True):
            factory.configure_scheduler_mode(app)
            assert app.config['JOB_SCHEDULING_ENABLED'] is True
            assert app.config['WORKER_QUEUE_ENABLED'] is True

    def test_disable_scheduling_through_env(self, app):
        with override_config(app, 'JOB_SCHEDULING_ENABLED', True):
            os.environ['EB_ENVIRONMENT'] = 'nessie-worker-bee'
            factory.configure_scheduler_mode(app)
            assert app.config['JOB_SCHEDULING_ENABLED'] is False
            assert app.config['WORKER_QUEUE_ENABLED'] is True

    def test_no_thread_limits_if_master_env(self, app):
        with override_config(app, 'JOB_SCHEDULING_ENABLED', True):
            os.environ['EB_ENVIRONMENT'] = 'nessie-master-bee'
            factory.configure_scheduler_mode(app)
            assert app.config['JOB_SCHEDULING_ENABLED'] is True
            assert app.config['WORKER_QUEUE_ENABLED'] is False
