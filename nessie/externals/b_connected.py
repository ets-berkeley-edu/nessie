"""
Copyright ©2021. The Regents of the University of California (Regents). All Rights Reserved.

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

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
from smtplib import SMTP

from flask import current_app as app


class BConnected:

    def __init__(self):
        self.bcop_smtp_password = app.config['BCOP_SMTP_PASSWORD']
        self.bcop_smtp_port = app.config['BCOP_SMTP_PORT']
        self.bcop_smtp_server = app.config['BCOP_SMTP_SERVER']
        self.bcop_smtp_username = app.config['BCOP_SMTP_USERNAME']

    def send(
            self,
            message,
            recipient,
            subject_line,
    ):
        if not message or not subject_line or not recipient:
            app.logger.error(
                'Attempted to send a message without required fields: '
                f'(recipient={recipient}, subject_line={subject_line}, message={message}')
            return False
        subject_line = _decorated_subject_line(subject_line)

        def _send():
            smtp = SMTP(self.bcop_smtp_server, port=self.bcop_smtp_port)
            # TLS encryption
            smtp.starttls()
            smtp.set_debuglevel(app.logger.level == logging.DEBUG)
            smtp.login(self.bcop_smtp_username, self.bcop_smtp_password)

            emails_sent_to = set()

            if app.config['EMAIL_FEATURE_ENABLED']:
                from_address = f"Nessie Operations <{app.config['EMAIL_FROM_ADDRESS']}>"

                for email_address in self.get_email_addresses(user=recipient):
                    msg = MIMEMultipart('alternative')
                    msg['From'] = from_address
                    msg['To'] = email_address

                    if app.config['EMAIL_TEST_MODE']:
                        # Append intended recipient email address to verify when testing.
                        intended_email = recipient['email']
                        msg['Subject'] = f'{subject_line} (To: {intended_email})'
                    else:
                        msg['Subject'] = subject_line

                    # TODO: 'plain' text version of email?
                    msg.attach(MIMEText(message, 'plain'))
                    msg.attach(MIMEText(message, 'html'))
                    # Send
                    smtp.sendmail(from_addr=from_address, to_addrs=email_address, msg=msg.as_string())

                    emails_sent_to.add(email_address)
            else:
                _write_email_to_log(message=message, recipient=recipient, subject_line=subject_line)

            app.logger.info(f'Email sent to {", ".join(list(emails_sent_to))} with subject: {subject_line}')
            # Disconnect
            smtp.quit()

        # Send email
        if app.config['EMAIL_FEATURE_ENABLED']:
            _send()
        else:
            app.logger.info('Skipping send() because email feature is disabled.')

        return True

    def ping(self):
        with SMTP(self.bcop_smtp_server, port=self.bcop_smtp_port) as smtp:
            smtp.noop()
            return True

    @classmethod
    def get_email_addresses(cls, user):
        if app.config['EMAIL_TEST_MODE']:
            config_value = app.config['EMAIL_REDIRECT_WHEN_TESTING']
            return config_value if isinstance(config_value, list) else [config_value]
        else:
            return [user['email']]


def _decorated_subject_line(subject_line):
    eb_env = app.config['EB_ENVIRONMENT'] if 'EB_ENVIRONMENT' in app.config else None
    prefix = '' if 'prod' in (eb_env or '') else f"[{eb_env or 'nessie-local'}] "
    return f'{prefix}{subject_line}'


def _write_email_to_log(message, recipient, subject_line):
    app.logger.info(f"""

        To: {recipient['name']} <{recipient['email']}>
        From: Nessie Operations <{app.config['EMAIL_FROM_ADDRESS']}>
        Subject: {subject_line}

        Message:
        {message}

    """)
