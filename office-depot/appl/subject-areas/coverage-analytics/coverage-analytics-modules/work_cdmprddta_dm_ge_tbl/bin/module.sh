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
#     : Shweta Karwa                     			 					     #
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

source $MODULES_HOME/bin/coverageAnalytics_functions.sh

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

startTime=`date +"%T"`
batchId=`cat $SUBJECT_AREA_HOME$BATCH_ID_FILE`
jobName=$WORK_CDMPRDDTA_DM_GE_SSH_ACTION
logFileName=$jobName"_"$batchId".log"
logFilePath=$PROJECTS_HOME$PROJ_TMP/$batchId/$logFileName
current_date=`date +%Y%m%d`

##############################################################################
#																			 #
# Inserting data in work tables	       								 		 #
#																			 #
##############################################################################	

	
fn_insert_work_tbl "$debug_mode" "${logFilePath}" "$batchId" "$WORK_CDMPRDDTA_DM_GE"

hadoop fs -chmod -R 777 $HDFS_PREFIX > /dev/null

exitCode="$?"

endTime=`date +"%T"`
   
fn_job_statistics "$startTime" "$endTime" "$exitCode" "$1" "$NOTAPPLICABLE" "$jobName" "$WORK_CDMPRDDTA_DM_GE" "$logFileName"

##############################################################################
#                                    End                                     #
##############################################################################