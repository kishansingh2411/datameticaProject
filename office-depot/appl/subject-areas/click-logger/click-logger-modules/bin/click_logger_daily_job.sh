#!/bin/bash

#######################################################################################
#                              General Details                                        #
#######################################################################################
#                                                                                     #
# Name                                                                                #
#     : click logger                                                                  #
# File                                                                                #
#     : clicklogger_daily_job.sh                                                      #
# Description                                                                         #
#     : To partiton hive table on daily basis and merging hourl avro files            #   
#        into day avro file									                          #
#       	                                                                          #
#                                                                                     #
#                                                                                     #
#                                                                                     #
#                                                                                     #
# Author                                                                              #
#     : SarfarazKhan                                                                  #
#                                                                                     #
#######################################################################################

# Find absolute path to this script which is used to define module, project, subject area home directory paths.
pushd . > /dev/null
   SCRIPT_HOME="${BASH_SOURCE[0]}";
   while([ -h "${SCRIPT_HOME}" ]); 
do
   cd "`dirname "${SCRIPT_HOME}"`"
   SCRIPT_HOME="$(readlink "`basename "${SCRIPT_HOME}"`")";
done
   cd "`dirname "${SCRIPT_HOME}"`" > /dev/null
   SCRIPT_HOME="`pwd`";
popd  > /dev/null

# Set module, project, subject area home paths.
   MODULE_HOME=`dirname ${SCRIPT_HOME}`
   MODULES_HOME=`dirname ${MODULE_HOME}`
   SUBJECT_AREA_HOME=`dirname ${MODULES_HOME}`
   PROJECT_HOME=`dirname ${SUBJECT_AREA_HOME}`
   PROJECTS_HOME=`dirname ${PROJECT_HOME}`

   source '$MODULE_HOME/etc/click-logger-modules.properties'
   source $PROJECT_HOME/bin/functions.sh

##############################################################################
#                                                                            #
#   Picking up the Machine time,date and year                                #
#                                                                            #
##############################################################################


   previous_day=`TZ=GMT+24 date +%d`
   batch_month=`TZ=GMT+24 date +%m`
   batch_year=`TZ=GMT+24 date +%Y`

   recordType=daily
   jobName=$CLICK_STREAM_DAILY_SSH_ACTION
 
   startTime=`date +%H%M%S`
   currentTime=`date +%Y%m%d%H%M%S`

   LogFilePath=$click_tmp
   LogFileName=$LogFilePath/"$jobName"_"$currentTime"
##############################################################################
#                                                                            #
#  Pig will Merge all hourly data into day data                              #
#                                                                            #
##############################################################################




   pig -p pig_input=$avro_incoming_dir -p year=$batch_year -p month=$batch_month -p day=$previous_day -p pig_lib=$pig_lib -f $MODULE_HOME/$pig_script_path/$pig_script_name

   OUT=$?
if [ $OUT -eq 1 ];then

   fn_log_info "Failed to Merge  Hourly Data into a Day Partitioned by previous day[$previous_day] hour [$batch_hour] records" "${LogFileName}_MERGING".log

else

   fn_log_info "Merged  Hourly Data into a Day Partitioned by previous day[$previous_day] hour [$batch_hour] records" "${LogFileName}_MERGING".log
fi

##############################################################################
#                                                                            #
#  Drop Partitions in hive table on hourly basis                             #
#                                                                            #
##############################################################################


for ((i=0;i <=23;i++))
do


   hive -e "USE $DB_INCOMING;ALTER TABLE $table_name DROP IF EXISTS PARTITION(year=$batch_year,month=$batch_month,day=$previous_day,hour=$i)"

   OUT=$?
if [ $OUT -eq 1 ];then

   fn_log_info "Failed Dropping Hive table  Partitioned [$i] by previous day [$previous_day] hour [$batch_hour] records" "${LogFileName}_DROPPING".log

else

   fn_log_info "Successfully Dropped Hive table  Partitioned [$i] by previous day [$previous_day] hour [$batch_hour] records" "${LogFileName}_DROPPING".log
fi
done

##############################################################################
#                                                                            #
#  Adding Partition in hive table on Daily cloumn                            #
#                                                                            #
##############################################################################


   hive -e "USE $DB_INCOMING; ALTER TABLE  $table_name ADD IF NOT EXISTS PARTITION (year='$batch_year',month='$batch_month',day='$previous_day', hour='$merged_hours');"

   exitCode=$?
if [ $exitCode -eq 1 ];then

   fn_log_info "Failed Adding Hive table  Partitioned [$previous_day] by previous day  [$batch_hour] records" "${LogFileName}_ADDING".log

else

   fn_log_info "Adding Hive table  Partitioned [$previous_day] by previous day  [$batch_hour] records" "${LogFileName}_ADDING".log
fi

##############################################################################
#                                                                            #
#                                  END OF SCRIPT                             #
#                                                                            #
##############################################################################
