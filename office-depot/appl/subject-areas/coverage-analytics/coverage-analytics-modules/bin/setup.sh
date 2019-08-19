#!/bin/sh

##############################################################################
#                              General Details                               #
##############################################################################
#                                                                            #
# Name                                                                       #
#     : setup      								   	 						 #
# File                                                                       #
#     : setup.sh                                   	                         #
# Description                                                                #
#     : Creating required Local as well as HDFS Directories.              	 #
#                                                                            #
#                                                                            #
#                                                                            #
# Author                                                                     #
#     : Shweta Karwa                      			 						 #
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

MODULE_HOME=`dirname ${SCRIPT_HOME}`
MODULES_HOME=`dirname ${MODULE_HOME}`
SUBJECT_AREA_HOME=`dirname ${MODULES_HOME}`
PROJECT_HOME=`dirname ${SUBJECT_AREA_HOME}`
PROJECTS_HOME=`dirname ${PROJECT_HOME}`

source $MODULE_HOME/bin/coverageAnalytics_functions.sh

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

startTime=`date +"%T"`
currentDate=`date +%Y%m%d`
jobName=$SETUP_SSH_ACTION

##############################################################################
#																			 #
# Removing Last batchId if presents 						     			 #
#																			 #
##############################################################################

if [ -e $SUBJECT_AREA_HOME$BATCH_ID_FILE ]
then
   rm  $SUBJECT_AREA_HOME$BATCH_ID_FILE
   if [ $? == 0 ]
   then
      #fn_log_info "Deleting existing batchId !!!" ${logFilePath}
      echo "Deleting existing batchId !!!"
   else
      exit
   fi
fi

##############################################################################
#																			 #
# Generating new BatchId and setting some local Params						 #
#																			 #
##############################################################################
			
date +%Y%m%d%H%M%S > $SUBJECT_AREA_HOME$BATCH_ID_FILE
batchId=`cat $SUBJECT_AREA_HOME$BATCH_ID_FILE`
logFileName=$jobName"_"$batchId".log"
logFilePath=$PROJECTS_HOME$PROJ_TMP/$batchId/$logFileName

if [ $? == 0 ]
then
   ##fn_log_info "New BatchId [ $batchId ] created successfully" ${logFilePath}
   echo "New BatchId [ $batchId ] created successfully"
else
   exit
fi
chmod 600 $SUBJECT_AREA_HOME$BATCH_ID_FILE

##############################################################################
#																			 #
# Creating /tmp/{batchId}/staging directory if not exists 		             #
#																			 #
##############################################################################			

if [ ! -e $PROJECTS_HOME$PROJ_TMP/$batchId$STAGING ]
then
   fn_local_create_directory_if_not_exist "$PROJECTS_HOME$PROJ_TMP/$batchId$STAGING" "1" "${logFilePath}"
fi

##############################################################################
#																			 #
# Creating local outgoing directory if not exists 						     #
#																			 # 
##############################################################################			

if [ ! -e $PROJECTS_HOME$OUTGOING/$1/$batchId ]
then
   fn_local_create_directory_if_not_exist "$PROJECTS_HOME$OUTGOING/$1/$batchId" "1" "${logFilePath}"
fi

##############################################################################
#																			 #
# Creating /Audit_log/Job_Statistic directory on HDFS						 #
#																			 #
##############################################################################
			
fn_hadoop_create_directory_if_not_exists $AUDITLOG_HDFS/$JOB_STATISTIC 1 ${logFilePath}

##############################################################################
#																			 #
# Creating /Log/{CurrentDate} directory on HDFS 							 #
#																			 #
##############################################################################
			
fn_hadoop_create_directory_if_not_exists $LOG_HDFS/$currentDate 1 ${logFilePath}


exitCode="$?"

endTime=`date +"%T"`

##############################################################################
#																			 #
# Logging Job Audit Details						     						 #
#																			 # 
##############################################################################

fn_job_statistics "$startTime" "$endTime" "$exitCode" "$1" "$NOTAPPLICABLE" "$jobName" "$NOTAPPLICABLE" "$logFileName"

##############################################################################
#                                    End                                     #
##############################################################################
