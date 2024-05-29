#!/bin/bash

AWK="/usr/bin/awk"
AWS="/snap/bin/aws"
DATE="/bin/date"
DIFF="/usr/bin/diff"
ECHO="/bin/echo"
EGREP="/bin/egrep"
FIND="/usr/bin/find"
LS="/bin/ls -l"
MAIL="/usr/bin/mail"
RM="/bin/rm"
SORT="/usr/bin/sort"

ADMIN="ets-analytics@lists.berkeley.edu"
APP_HOME="/home/app_aws"
DAY=$(${DATE} +"%d")
HR=$(${DATE} +"%H%M")
MO=$(${DATE} +"%m")
YR=$(${DATE} +"%Y")
LDATE="${YR}${MO}${DAY}.${HR}"
LOG_DIR="${APP_HOME}/scripts/logs/sis"
LOG="${LOG_DIR}/sis_upload_${LDATE}.log"
LS_LOCAL="${LOG_DIR}/ls_local_${LDATE}.out"
LS_S3="${LOG_DIR}/ls_s3_${LDATE}.out"
LS_DIFF="${LOG_DIR}/ls_diff_${LDATE}.out"
MSG=""

case $1 in
  dev|tst|qat|prd)
    ENV=$1
    UPLOAD_DIR="${APP_HOME}/uploads_sis/$1/out"
  ;;
  *)
    ${ECHO} "Usage: $0 {dev|tst|qat|prd}" && exit 0
  ;;
esac

# Function to upload SIS Attachments
# usage: upload_files "s3_loc"
upload_files () {
  local_s3_loc=${1}
  ${AWS} s3 cp --sse AES256 ${UPLOAD_DIR}/ s3://${local_s3_loc}/ --recursive --quiet
  if [ $? -ne 0 ]; then
    ${ECHO} "ERROR: aws s3 cp failure to upload sis attachment files to ${local_s3_loc}" >> ${LOG}
    ${MAIL} -s "ACTION REQ: aws s3 cp failure to upload sis attachment files" ${ADMIN} < ${LOG}
    exit
  fi
  return ${local_result}
}

# Function to check uploaded SIS Attachments
# usage: cmp_files "s3_loc"
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

## Exit script if upload directory is empty
if [ -n "$(${FIND} "${UPLOAD_DIR}" -maxdepth 0 -type d -empty 2>/dev/null)" ]; then
  ${ECHO} "Directory is empty" >> ${LOG}
  #Directory is frequently empty, so commenting out alert
  #${MAIL} -s "WARNING: ${UPLOAD_DIR} is empty" ${ADMIN} < ${LOG}
  exit
fi

## Upload attachments to S3 nessie buckets.  If dev|tst|qa, upload to dev|qa S3 buckets only
case ${ENV} in
  dev|tst|qat)
    for bucket in dev qa
    do
      ${ECHO} -en "${bucket}:\n" >> ${LOG}
      s3_loc="la-nessie-protected-${bucket}/sis-data/sis-sftp/incremental/advising-notes/attachment-files/${YR}/${MO}/${DAY}"
      upload_files "${s3_loc}"
      cmp_files "${s3_loc}"
    done
  ;;
  prd)
    for bucket in dev qa prod
    do
      ${ECHO} -en "${bucket}:\n" >> ${LOG}
      s3_loc="la-nessie-protected-${bucket}/sis-data/sis-sftp/incremental/advising-notes/attachment-files/${YR}/${MO}/${DAY}"
      upload_files "${s3_loc}"
      cmp_files "${s3_loc}"
    done
  ;;
  *)
    ${ECHO} "Usage: $0 {dev|tst|qat|prd}" && exit 0
  ;;
esac

# Check that files uploaded correctly and Mail results
if ${EGREP} -q "ERROR" ${LOG}; then
  ${CAT} ${LOG} ${LS_DIFF} | ${MAIL} -s "ACTION REQUIRED: Error uploaded SIS attachment files" ${ADMIN}
#else
#  ${MAIL} -s "OK: SIS attachment files" ${ADMIN} < ${LS_S3}
fi

## Delete old upload logs
${FIND} ${LOG_DIR}/ -name "sis_upload_*" -type f -mtime +30 -exec $RM {} \; >/dev/null 2>&1
