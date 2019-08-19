#!/bin/sh

##############################################################################
#                              General Details                               #
##############################################################################
#                                                                            #
# Name                                                                       #
#     : create_hive_tables      								   	 		 #
# File                                                                       #
#     : create_hive_tables.sh                                   	         #
# Description                                                                #
#     : Create Databse Schema and tables for all layers.                     #
#                                                                            #
#                                                                            #
#                                                                            #
# Author                                                                     #
#     : Deepanshu                         			 					     #
#                                                                            #
##############################################################################

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

source $MODULE_HOME/bin/coremetrics_functions.sh

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

startTime=`date +"%T"`
batchId=`cat $SUBJECT_AREA_HOME$BATCH_ID_FILE`
jobName=$CREATE_SCHEMA_SSH_ACTION
logFileName=$jobName"_"$batchId".log"
logFilePath=$PROJECTS_HOME$PROJ_TMP/$batchId/$logFileName
current_date=`date +%Y%m%d`

##############################################################################
#																			 #
# Creating hive schema and tables	 								 		 #
#																			 #
##############################################################################	

   if [ "$1" == "ON" ]
then
   debug_mode=$DEBUG_ON
else
   debug_mode=$DEBUG_OFF
   fi		
		
fn_create_hive_tbl "$debug_mode" "${logFilePath}"

hadoop fs -chmod -R 777 $HDFS_PREFIX > /dev/null

exitCode="$?"

endTime=`date +"%T"`
   
hive -e " use $DB_AUDIT ; alter table $JOB_STATISTIC add partition(current_Date=$current_date)"

fn_job_statistics "$startTime" "$endTime" "$exitCode" "$1" "$NOTAPPLICABLE" "$jobName" "$NOTAPPLICABLE" "$logFileName"

##############################################################################
#                                    End                                     #
##############################################################################