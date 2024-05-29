#!/bin/bash

AWS="/snap/bin/aws"
DATE="/bin/date"
FIND="/usr/bin/find"
MAIL="/usr/bin/mail"
RM="/bin/rm"
ADMIN="ets-analytics@lists.berkeley.edu"
DAY=$(${DATE} +"%d")
MO=$(${DATE} +"%m")
YR=$(${DATE} +"%Y")
TDATE="${YR}${MO}${DAY}"
APP_HOME="/home/app_aws"
LOG_DIR="${APP_HOME}/scripts/logs/oua"
LOG="${LOG_DIR}/oua_upload_${TDATE}.log"
LS_LOCAL="${LOG_DIR}/ls_local_${TDATE}.out"
LS_S3="${LOG_DIR}/ls_s3_${TDATE}.out"
LS_DIFF="${LOG_DIR}/ls_diff_${TDATE}.out"
UPLOAD_DIR="${APP_HOME}/uploads_oua/prod"
MSG=""

# Development. clear out log before running script
cat /dev/null > ${LOG}

# Function to upload OUA_FILE
# usage: upload_file "local_s3_loc"
upload_file () {
local_s3_loc=${1}
local_file=${2}
${AWS} s3 cp --sse AES256 ${local_file} s3://${local_s3_loc}/ >>${LOG} 2>&1
local_result=$?
if [ ${local_result} -ne 0 ]; then
  MSG="failure to upload oua file to ${local_s3_loc}"
  ${MAIL} -s "ACTION REQ: ${MSG}" ${ADMIN} < ${LOG}
  exit 1
fi
return ${local_result}
}

# Function to check uploaded SIS Attachments
# usage: cmp_files "s3_loc"
# To Do for added check
cmp_files () {
  local_s3_loc=${1}
  ${LS} ${UPLOAD_DIR} | ${EGREP} -v "^total " | ${AWK} '{print $9","$5}' > ${LS_LOCAL}
  ${AWS} s3 ls --recursive s3://${local_s3_loc}/ | ${AWK} '{print $4","$3}' | ${AWK} -F"/" '{print $(NF)}' | ${SORT} > ${LS_S3}
  ${DIFF} ${LS_LOCAL} ${LS_S3} > ${LS_DIFF}
  if [ -s "${LS_DIFF}" ]; then
    ${ECHO} -en "ERROR: files uploaded don't match ${local_s3_loc}:\n" >> ${LOG}
    ${ECHO} "$(date)\nkey:\n< = local files\n> = s3 files" >> ${LOG}
    ${EGREP} "<|>" ${LS_DIFF} >> ${LOG}
  fi
}

## Exit script if current OUA_FILE doesn't exist; nessie uses last uploaded file
# Abort if no new file.  Only retrieve first file matching criteria. Unlikely to receive more than one file per day.
OUA_FILE=( ${UPLOAD_DIR}/oua_admissions_${TDATE}T*.csv )
if [ ! -s ${OUA_FILE} ]; then
  echo "${OUA_FILE} does not exist or is empty on la-transfer host" >> ${LOG}
  # OUA does not send files every month, so disabling the following.
  #${MAIL} -s "ACTION REQ: oua csv upload" ${ADMIN} < ${LOG}
  exit
fi

## Upload current OUA_FILE to S3 nessie buckets
for env in dev qa prod
do
  echo -en "${env}:\n" >> ${LOG}
  s3_loc="la-nessie-protected-${env}/oua-data/slate-sftp/${YR}/${MO}/${DAY}"
  upload_file "${s3_loc}" "$OUA_FILE"
done

## Delete old upload logs
${FIND} ${LOG_DIR}/ -name "oua_upload_*" -type f -mtime +7 -exec $RM {} \; >/dev/null 2>&1
