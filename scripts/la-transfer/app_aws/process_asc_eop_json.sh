#!/bin/bash

AWS="/snap/bin/aws"
ECHO="/bin/echo"
FIND="/usr/bin/find"
MAIL="/usr/bin/mail"
RM="/bin/rm"

ADMIN="ets-analytics@lists.berkeley.edu"
DEPT=$1
TDATE=$(date +"%Y-%m-%d")
ASC_FILE="${DEPT}_advising_notes_${TDATE}.json"
APP_HOME="/home/app_aws"
UPLOAD_DIR="${APP_HOME}/uploads_asc/uploads"
LOG_DIR="${APP_HOME}/scripts/logs/asc"
LOG="${LOG_DIR}/${DEPT}_upload_${TDATE}.log"
MSG=""

case "$1" in
  asc)
    S3_FOLDER="asc"
    ;;
  eop)
    S3_FOLDER="e-and-i"
    ;;
  *)
    ${ECHO} "Usage: $0 {asc|eop}" && exit 0
    ;;
esac

# Function to upload ASC_FILE
# usage: upload_file "local_s3_loc"
upload_file () {
  local_s3_loc=${1}
  local_file=${2}
  ${AWS} s3 cp --sse AES256 ${UPLOAD_DIR}/${local_file} s3://${local_s3_loc}/ >>${LOG} 2>&1
  local_result=$?
  if [ ${local_result} -ne 0 ]; then
    MSG="la-transfer failured to upload ${local_file} file to S3"
    ${MAIL} -s "ACTION REQ: ${MSG}" ${ADMIN} < ${LOG}
    exit
  fi
return ${local_result}
}

## Exit script if current ASC_FILE doesn't exist; nessie uses last uploaded file
if [ ! -s "${UPLOAD_DIR}/${ASC_FILE}" ]; then
  ${ECHO} "${ASC_FILE} does not exist or is empty on la-transfer host" >> ${LOG}
  ${MAIL} -s "ACTION REQ: ${DEPT} json upload" ${ADMIN} < ${LOG}
  exit
fi

## Upload current ASC_FILE to S3 nessie buckets and delete older files
for env in dev qa prod
do
  ${ECHO} -en "${env}:\n" >> ${LOG}
  s3_loc="la-nessie-${env}/${S3_FOLDER}-data/${S3_FOLDER}-sftp/incremental/advising_notes"
  upload_file "${s3_loc}" "$ASC_FILE"
done

# Upload daily json to S3 archive bucket.
#${ECHO} -en "Archive:\n" >> ${LOG}
#s3_loc="la-archive/la-nessie-prod/${S3_FOLDER}-data/${S3_FOLDER}-sftp/incremental/advising_notes"
#upload_file "${s3_loc}" "$ASC_FILE"

# Mail all is well
#${MAIL} -s "OK: ${DEPT} json file uploaded" ${ADMIN} < ${LOG}

## Delete old upload logs
${FIND} ${LOG_DIR}/ -name "${DEPT}_upload_*" -type f -mtime +7 -exec $RM {} \; >/dev/null 2>&1
