#!/bin/bash

AWS="/snap/bin/aws"
FIND="/usr/bin/find"
MAIL="/usr/bin/mail"
RM="/bin/rm"

ADMIN="ets-analytics@lists.berkeley.edu"
TDATE=$(date +"%Y-%m-%d")
#ARCHIVE_LOC="pdg-share/coe"
APP_HOME="/home/app_aws"
UPLOAD_DIR="${APP_HOME}/uploads_coe/uploads"
COE_FILE="coe_student_adviser_${TDATE}.tsv"
LOG_DIR="${APP_HOME}/scripts/logs/coe"
LOG="${LOG_DIR}/coe_upload_${TDATE}.log"
MSG=""

# clear out log before running script
cat /dev/null > ${LOG}

# Function delete older coe files
# usage: delete_old_files "env"
delete_old_files () {
local_env=${1}
${AWS} s3 ls s3://la-nessie-${env}/coe-data/students/coe_student_adviser_ | while read -r line;  do
file=`echo $line|awk {'print $4'}`
if [ "$file" != "${COE_FILE}" ]; then
  ${AWS} s3 rm s3://la-nessie-${env}/coe-data/students/$file >>${LOG} 2>&1
  local_result=$?
  if [ ${local_result} -ne 0 ]; then
    MSG="la-transfer failed to delete old coe file(s) on ${local_env}"
    ${MAIL} -s "ACTION REQ: ${MSG}" ${ADMIN} < ${LOG}
  fi
fi
done;
}

# Function to upload COE_FILE
# usage: upload_file "local_s3_loc"
upload_file () {
local_s3_loc=${1}
${AWS} s3 cp --sse AES256 ${UPLOAD_DIR}/${COE_FILE} s3://${local_s3_loc}/ >>${LOG} 2>&1
local_result=$?
if [ ${local_result} -ne 0 ]; then
  MSG="la-transfer failured to upload ${COE_FILE} file to S3"
  ${MAIL} -s "ACTION REQ: ${MSG}" ${ADMIN} < ${LOG}
  exit
fi
return ${local_result}
}

## Exit script if current COE_FILE doesn't exist; nessie uses last uploaded file
if [ ! -s "${UPLOAD_DIR}/${COE_FILE}" ]; then
  echo "${COE_FILE} does not exist or is empty on la-transfer host" >> ${LOG}
  ${MAIL} -s "ACTION REQ: coe tsv upload" ${ADMIN} < ${LOG}
  exit
fi

## Archive COE_FILE to S3 pdg-share
#upload_file "${ARCHIVE_LOC}"

## Upload current COE_FILE to S3 nessie buckets and delete older files
for env in dev qa prod
do
  echo -en "${env}:\n" >> ${LOG}
  s3_loc="la-nessie-${env}/coe-data/students"
  upload_file "${s3_loc}"
  delete_old_files "${env}"
done

# Mail all is well
#${MAIL} -s "OK: coe tsv files uploaded" ${ADMIN} < ${LOG}

## Delete old upload logs
${FIND} ${LOG_DIR}/ -name "coe_upload_*" -type f -mtime +7 -exec $RM {} \; >/dev/null 2>&1
