#!/bin/bash
CONFIG_NAME=$(echo "${EB_ENVIRONMENT}" | sed -E -e 's/(-highlands|-lowlands)//')
PYTHONPATH='' aws s3 cp s3://la-deploy-configs/nessie/${CONFIG_NAME}.py config/production-local.py
printf "\nEB_ENVIRONMENT = '${EB_ENVIRONMENT}'\n\n" >> config/production-local.py
chown webapp config/production-local.py
chmod 400 config/production-local.py
