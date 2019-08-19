#!/bin/bash
########################################################################################################################
#
#   Program name: compute_max_date.sh
#   Program type: Unix Shell script
#   Author      : Kriti Singh
#   Date        : 05/29/2016
#
#   Description : This Shell script computes the max dtm_created date from the incoming.
#                 kom_vod_order table for a particular month_id. This is done in order to
#                 source the new records for that month.
#
#   Usage       : compute_max_date.sh <month_id YYYYMM>
#                 Where month_id is the month for which the max date is to be extracted
#                 start_date - is the max date extracted from the kom_vod_order table for this month_id
#                 end_date - is the last day of the given month_id
#
#
########################################################################################################################

# Set path
export AUTODIR=/UTIL/app/util

LOGFILE=${AUTODIR}/SvodUsageDemographics/logs/compute_max_date.log
trap 'echo >>$LOGFILE aborted \(Sorry\) ; exit 2' 1 2 3 5 15

# redirect output and error to the log file
exec 1>${LOGFILE}
exec 2>>${LOGFILE}

checkExitCode( )
{
  exitcode=$1
  desc=$2

  if [ $exitcode -ne 0 ]
  then
    echo "Error: $desc"
    exit 1
  fi
}


findMaxDate( )
{
    month_id=$1
    first=01
    #first day of the month
    firstDayStr=${month_id}${first}
    firstDay=$(date -d ${firstDayStr} +'%Y-%m-%d')
    echo "first day of the month ${month_id} is ${firstDay}"

    lastDay=$(date -d "${firstDay} +1 month - 1 day" +%Y-%m-%d)
    #lastDay=$(date -d "$tmp" +%Y-%m-%d)
    echo "last day of thie month ${month_id} is ${lastDay}"

    hdfs dfs -rm -r /incoming/svodusagedemo/encrypted_kom_vod_order/month_id=${month_id}
    #commenting out the below line so everytime the entire month of data is sourced from oracle
    hive -e "ALTER TABLE incoming.kom_vod_order ADD IF NOT EXISTS PARTITION(month_id=${month_id});"
    start_date=$(hive -e "SELECT TO_DATE(MAX(dtm_created)) FROM incoming.kom_vod_order WHERE month_id=${month_id};")  2>&1

    if [ -z "$start_date" ]; then
        echo "start date or max date from kom_vod_order for the given month id is empty or null"
        start_date=${firstDay}
    else
        echo "start date is not empty or null"
    fi


    echo "start_date=${start_date}"
    end_date=$(date +%Y-%m-%d)
    end_date=${lastDay}
    echo "end_date=${end_date}"
    checkExitCode $? "Could not get max date for $month_id"
}



main( )
{
  numArgs=$#

  if [ $numArgs -ne 1 ]
  then
    echo "Usage: compute_max_date.sh <month_id YYYYMM>"
    exit 1
  fi

  MONTH_ID=$1

  findMaxDate $MONTH_ID
 }

 main $@


exit 0
