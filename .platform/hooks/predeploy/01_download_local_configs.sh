#!/bin/bash
ENV_NAME=$(echo "${EB_ENVIRONMENT}" | sed -E -e 's/(nessie-highlands-|nessie-lowlands-)//')
PYTHONPATH='' aws s3 cp s3://la-deploy-configs/nessie/nessie-al2-${ENV_NAME}.py config/production-local.py
printf "\nEB_ENVIRONMENT = '${EB_ENVIRONMENT}'\n\n" >> config/production-local.py
chown webapp config/production-local.py
chmod 400 config/production-local.py
