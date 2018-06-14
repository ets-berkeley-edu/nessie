#!/bin/bash

# -------------------------------------------------------------------

# Abort immediately if a command fails
set -e


if [ "$EUID" -ne 0 ]; then
  echo "Sorry, you must use 'sudo' to run this script."; echo
  exit 1
fi

export PATH="${PATH}:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/opt/aws/bin:/home/ec2-user/.local/bin:/home/ec2-user/bin"
export PYTHONPATH="/opt/python/current/app:/opt/python/run/venv/lib/python3.6/site-packages:/opt/python/run/venv/lib64/python3.6/site-packages"
export NESSIE_ENV=production

/usr/bin/pip-3.6 install Flask==0.12.2

cd /opt/python/current/app

export FLASK_APP=run.py
flask initdb

exit 0
