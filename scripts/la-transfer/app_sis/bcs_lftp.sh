#!/bin/bash
# https://unix.stackexchange.com/questions/254841/lftp-script-to-download-files

AWK="/usr/bin/awk"
CAT="/bin/cat"
CLAMSCAN="/usr/bin/clamscan"
DATE="/bin/date"
DIFF="/usr/bin/diff"
ECHO="/bin/echo -e"
EGREP="/bin/egrep"
FIND="/usr/bin/find"
LFTP="/usr/bin/lftp"
LS="/bin/ls -l"
RM="/bin/rm"
SED="/bin/sed"

HR=$(${DATE} +"%H")
DAY=$(${DATE} +"%d")
MO=$(${DATE} +"%m")
YR=$(${DATE} +"%Y")
LDATE="${YR}${MO}${DAY}.${HR}"
RM_DAYS=30
PROTOCOL="sftp"
USER="app_ftets"
MGET="mget"
REGEX="*.*"

LOGDIR="${HOME}/scripts/logs"
LOG="${LOGDIR}/lftp_${LDATE}.log"
INFECTED_LOG="${LOGDIR}/infected_${LDATE}.log"
LS_LOCAL="${LOGDIR}/ls_local_${LDATE}.out"
LS_TMP="${LOGDIR}/ls_tmp.out"
LS_REMOTE="${LOGDIR}/ls_remote_${LDATE}.out"
LS_DIFF="${LOGDIR}/ls_diff_${LDATE}.out"

case $1 in
  dev | tst)
    URL="bcsftpdev.is.berkeley.edu"
    ;;
  qat | prd)
    URL="bcsftp.is.berkeley.edu"
    ;;
  *)
    echo "Usage: $0 {dev|tst|qat|prd}" && exit 0
    ;;
esac

ENV="$1"
INFECTED_DIR="${HOME}/note_uploads/infected/${ENV}"
LOCAL_DIR="${HOME}/note_uploads/${ENV}/out"
REMOTE_BASE="/opt/bcs/tmp/${ENV}/ftp/ets/attach"
REMOTE_DIR="${REMOTE_BASE}/out"
REMOTE_STAGING="${REMOTE_BASE}/staging"
REMOTE_TSF="${REMOTE_BASE}/transferred/${YR}/${MO}/${DAY}"

cd ${LOCAL_DIR}
if [  ! $? -eq 0 ]; then
  ${ECHO} "ERROR $(${DATE}) Cannot cd to ${LOCAL_DIR}. Please make sure this local directory is valid\n" >> ${LOG}
  exit 1
fi

# Move all files to staging folder to take snapshot of working files
# Mirror staging folder to local folder
# After 30 days, delete transferred/date folder
# Note: "mkdir: Access failed: Failure" if running script more than once on same day
#lftp  ${PROTOCOL}://${URL} <<- DOWNLOAD
${LFTP} ${PROTOCOL}://${USER}:@${URL} <<- DOWNLOAD >> ${LOG}
    cache flush
    mkdir -p ${REMOTE_TSF}
    mmv ${REMOTE_DIR}/${REGEX} ${REMOTE_STAGING}/
    mirror -c --no-recursion ${REMOTE_STAGING} ${LOCAL_DIR}
    cd ${REMOTE_STAGING}
    cache flush
    cls -l > ${LS_TMP}
    mmv ./${REGEX} ${REMOTE_TSF}/
    bye
DOWNLOAD

if [ ! $? -eq 0 ]; then
  ${ECHO} "ERROR $(${DATE}) Could NOT download files. Make sure the credentials and server information are correct\n" >> ${LOG}
  exit 1
fi

if [ ! -s "${LS_TMP}" ]; then
  ${ECHO} "${REMOTE_DIR} did not contain any files" >> ${LOG}
  exit 0
fi

# Compare downloaded files with remote files
${LS} -l ${LOCAL_DIR} | ${AWK} '{print $5" "$9}' | ${SED} '/^[[:space:]]*$/d' > ${LS_LOCAL}
${AWK} '{print $5" "$9}' ${LS_TMP} | ${SED} '/^[[:space:]]*$/d' > ${LS_REMOTE}
${DIFF} ${LS_LOCAL} ${LS_REMOTE} >> ${LS_DIFF}
if [ -s "${LS_DIFF}" ]; then
  ${ECHO} "$(${DATE})\nkey:\n< = local files\n> = remote files" >> ${LOG}
  ${EGREP} "<|>" ${LS_DIFF} >> ${LOG}
fi

# Run clamav against downloaded sis files
${CLAMSCAN} --move=${INFECTED_DIR} --log=${INFECTED_LOG} -r ${LOCAL_DIR}
if [ $? -eq 1 ]; then
  ${CAT} ${INFECTED_LOG} >> ${LOG}
fi

# Clean up
${RM} ${LS_TMP}
${FIND} ${LOGDIR} -type f -mtime +${RM_DAYS} -exec ${RM} {} \; >/dev/null 2>&1

exit 0
