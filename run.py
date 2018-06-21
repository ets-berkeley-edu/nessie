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


"""
Usage mode A:

>>> python run.py

Usage mode B:

>>> export FLASK_APP=run.py
>>> flask run --help
>>> flask run --debugger
>>> flask initdb
"""

import os
import subprocess

from nessie.factory import create_app

# When running under WSGI, system environment variables are not automatically made available to Python code, and
# an app restart will result in configurations being lost. We work around this with an explicit load from the shell
# environment, sourcing from the Elastic Beanstalk-provided /opt/python/current/env file if available.
if __name__.startswith('_mod_wsgi'):
    command = ['bash', '-c', '{ source /opt/python/current/env || true; } && env']
    shell_environment = subprocess.Popen(command, stdout=subprocess.PIPE)
    for line in shell_environment.stdout:
        key, _, value = line.decode('utf-8').rstrip().partition('=')
        os.environ[key] = value

application = create_app()


@application.cli.command()
def initdb():
    from nessie.models import development_db
    development_db.load_schemas()


host = application.config['HOST']
port = application.config['PORT']

if __name__ == '__main__':
    application.logger.info('Starting development server on %s:%s', host, port)
    application.run(host=host, port=port)
elif __name__.startswith('_mod_wsgi'):
    # Grab the current EC2 instance id and stash it in an environment variable.
    try:
        instance_metadata = subprocess.Popen(['ec2-metadata', '-i'], stdout=subprocess.PIPE)
        os.environ['EC2_INSTANCE_ID'] = next(instance_metadata.stdout).decode('utf-8').partition(':')[2].strip()
    except Exception as e:
        application.logger.error(f'Could not retrieve current EC2 instance id: {e}')

    application.logger.info('Will start WSGI server on %s:%s', host, port)
