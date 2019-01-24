#!/bin/bash

# Fail the entire script when one of the commands in it fails
set -e

echo_usage() {
  echo "SYNOPSIS"
  echo "     ${0} -d db_connection -b s3_bucket"; echo
  echo "DESCRIPTION"
  echo "Available options"
  echo "     -d      Database connection information in the form 'host:port:database:username'. Required."
  echo "     -b      S3 bucket name to which data should be uploaded. Required."
}

while getopts "d:b:" arg; do
  case ${arg} in
    d)
      db_params=(${OPTARG//:/ })
      db_host=${db_params[0]}
      db_port=${db_params[1]}
      db_database=${db_params[2]}
      db_username=${db_params[3]}
      db_password=${db_params[4]}
      ;;
    b)
      s3_bucket="${OPTARG}"
      ;;
  esac
done

db_host_segments=(${db_host//./ })
db_shortname=${db_host_segments[0]}

# Validation
[[ "${db_host}" && "${db_port}" && "${db_database}" && "${db_username}" ]] || {
  echo "[ERROR] You must specify complete SuiteC database connection information."; echo
  echo_usage
  exit 1
}
[[ "${s3_bucket}" ]] || {
  echo "[ERROR] You must specify the S3 bucket to which data will be uploaded."; echo
  echo_usage
  exit 1
}

if ! [[ "${db_password}" ]]; then
  echo -n "Enter database password: "
  read -s db_password; echo; echo
fi

# Export password so that it need not be re-entered for every extract.
export PGPASSWORD=${db_password}

# Clean up any leftover data in the local folder.
rm -rf ~/suitec-data
mkdir -p ~/suitec-data

# Declare an array containing all SuiteC tables to be copied to S3, except the special case of events.
suitec_tables=( activities activity_types asset_users asset_whiteboard_elements assets assets_categories
canvas categories chats comments courses pinned_user_assets users whiteboard_elements
whiteboard_members whiteboard_sessions whiteboards )

for i in "${suitec_tables[@]}"
do
  # Download extracts to local directories.
  mkdir -p ~/suitec-data/$i
  echo "Downloading $i from database ${db_database} on ${db_shortname}."
  psql -h ${db_host} -p ${db_port} -d ${db_database} -U ${db_username} -c \
    "COPY (SELECT * FROM $i) TO STDOUT WITH NULL AS ''" > ~/suitec-data/$i/$i.tsv
done

# The events table doesn't stretch back to the beginning of time but is copied month by month. We timestamp the export so
# as not to overwrite older exports in S3.
timestamp="$(date +%Y%m%d_%H%M%S)"
first_of_month="$(date +%Y-%m)-01"
mkdir -p ~/suitec-data/events
echo "Downloading events prior to ${first_of_month} from database ${db_database} on ${db_shortname}."
psql -h ${db_host} -p ${db_port} -d ${db_database} -U ${db_username} -c \
  "COPY (SELECT * FROM events WHERE created_at < '${first_of_month}') to STDOUT with NULL AS ''"\
  > ~/suitec-data/events/events_${timestamp}.tsv

# Copy the entire folder structure into the specified S3 bucket.
aws s3 cp --recursive ~/suitec-data/ s3://${s3_bucket}/suitec-data/${db_shortname}/public --sse

echo
echo "Delete previous months from events table on '${db_database}' at ${db_host}:${db_port}?"
echo "(Check that your local export at ~/suitec-data/events/events_${timestamp}.tsv looks right, and that it was"
echo "successfully uploaded to s3://${s3_bucket}/suitec-data/${db_shortname}/public/events/events_${timestamp}.tsv.)"
echo -n "Type capital 'D' to proceed with DELETE: "
read confirmation; echo

[[ "${confirmation}" = 'D' ]] || {
  echo "Aborting."
  exit 1
}

psql -h ${db_host} -p ${db_port} -d ${db_database} -U ${db_username} -c \
  "DELETE FROM events WHERE created_at < '${first_of_month}'"
psql -h ${db_host} -p ${db_port} -d ${db_database} -U ${db_username} -c "VACUUM FULL events"
echo "All done!"
