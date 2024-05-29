#!/bin/bash
# This script cleans up files in note_uploads/out in preparation for the next upload
# It must run after process_sis_attachments.sh cron job by app_aws completes

DATE="/bin/date"
FIND="/usr/bin/find"
MAIL="/usr/bin/mail"
RM="/bin/rm"

ADMIN="rtl-ops@calgroups.berkeley.edu"
TDATE=$(${DATE} +"%Y%m%d")
HR=$(${DATE} +"%H")
LOGDIR="${HOME}/scripts/logs"
LOG="${LOGDIR}/cleanup_${TDATE}.${HR}.log"

echo "------- ${TDATE} -------" > ${LOG}

for env in dev tst qat prd
do
  UPLOAD_DIR="${HOME}/note_uploads/${env}/out"
  if [ -d "${UPLOAD_DIR}" ]; then
    echo "---> Processing ${UPLOAD_DIR}" >> ${LOG}
    $FIND ${UPLOAD_DIR} -type f -exec $RM {} \; >/dev/null 2>&1
    retval="$?"
    if [ $retval -ne 0 ]; then
      echo "  ACTION REQ: Unable to delete files in ${UPLOAD_DIR}" >> ${LOG}
    else
      echo "OK: Files deleted in ${UPLOAD_DIR}" >> ${LOG}
    fi
  else
    echo "  ACTION REQ: ${UPLOAD_DIR} does not exist" >> ${LOG}
  fi
done

# cleanup infected directory instead of having clamscan do it
for env in dev tst qat prd
do
  INFECTED_DIR="${HOME}/note_uploads/infected/${env}"
  if [ -d "${INFECTED_DIR}" ]; then
    echo "---> Processing ${INFECTED_DIR}" >> ${LOG}
    $FIND ${INFECTED_DIR} -type f -exec $RM {} \; >/dev/null 2>&1
    retval="$?"
    if [ $retval -ne 0 ]; then
      echo "  ACTION REQ: Unable to delete files in ${INFECTED_DIR}" >> ${LOG}
    else
      echo "OK: Files deleted in ${INFECTED_DIR}" >> ${LOG}
    fi
  else
    echo "  ACTION REQ: ${INFECTED_DIR} does not exist" >> ${LOG}
  fi
done

# Email results to admin
if grep -q "ACTION REQ:" ${LOG}; then
  ${MAIL} -s "ACTION REQ: Error deleting ftp files" ${ADMIN} < ${LOG}
#else
#  ${MAIL} -s "OK: ftp files deleted" ${ADMIN} < ${LOG}
fi
