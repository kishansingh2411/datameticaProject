#!/bin/bash

#######################################################################################
#                              General Details                                        #
#######################################################################################
#                                                                                     #
# Name                                                                                #
#     : click logger                                                                  #
# File                                                                                #
#     : click_logger_alter_hourly_job.sh                                              #
# Description                                                                         #
#     : To perform hourly partiton on hive table of click logger                      #
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
while([ -h "${SCRIPT_HOME}" ]); do
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


   source $MODULE_HOME/etc/click-logger-modules.properties
   source $PROJECT_HOME/bin/functions.sh
##############################################################################
#                                                                            #
#   Picking up the Machine time,date and year                                #
#                                                                            #
##############################################################################


  
   batch_hour=`TZ=GMT date +%H`
   current_day=`TZ=GMT-1 date +%d`
  
   previous_day=`TZ=GMT+23 date +%d`
   batch_month=`TZ=GMT+23 date +%m`
  
   batch_year=`TZ=GMT+23 date +%Y`
  
   currentTime=`date +%Y%m%d%H%M%S`
   jobName=$CLICK_STREAM_HOURLY_SSH_ACTION

   LogFilePath=$click_tmp
   LogFileName=$LogFilePath/"$jobName"_"$currentTime"
##############################################################################
#                                                                            #
#   Partitioning the hive table on hourly basis                              #
#                                                                            #
###############################################################################

if [ $batch_hour -eq 23 ];then

   hive -e "use $DB_INCOMING; alter table  $table_name add IF NOT EXISTS partition (year='$batch_year',month='$batch_month',day='$previous_day',hour='$batch_hour');"

     
 
   
   exitCode=$?
if [ $exitCode -eq 1 ];then
     
   
   fn_log_info "Hive table [$table_name] Not Partitioned by previous day[$previous_day] records" "${LogFileName}_PREVIOUS_DAY".log

else
     
   fn_log_info "Hive table [$table_name] Partitioned by previous day[$previous_day] hour [$batch_hour] records" "${LogFileName}_PREVIOUS_DAY".log
  
fi

##############################################################################
#                                                                            #
#   Partitioning the hive table on hourly basis till 22nd hour               #
#                                                                            #
##############################################################################
   
   
else
   
   hive -e "use $DB_INCOMING; alter table  $table_name add IF NOT EXISTS partition (year='$batch_year',month='$batch_month',day='$current_day',hour='$batch_hour');"
 

   
   exitCode=$?
if [ $exitCode -eq 1 ];then
   
   fn_log_info "Hive table [$table_name] Not Partitioned by current day [$current_day] hour [$batch_hour] records" "${LogFileName}_CURRENT_DAY".log

else
   
   fn_log_info "Hive table [$table_name] Partitioned by current day [$current_day] hour [$batch_hour] records" "${LogFileName}_CURRENT_DAY".log
   
fi
   
fi

##############################################################################
#                                                                            #
#                             End of Script		                             #
#                                                                            #
##############################################################################

