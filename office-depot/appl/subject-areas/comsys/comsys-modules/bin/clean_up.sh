#!/bin/sh

#######################################################################################
#                              General Details                               		  #
#######################################################################################
#                                                                            		  #
# Name                                                                                #
#     : clean_up       								   	 		                      #
# File                                                                                #
#     : clean_up.sh                                   	                              #
# Description                                                                         #
#     : Deleting temporary files from batchId directory once data is processed 		  #
#                                                                                     #
#                                                                                     #
#                                                                                     #
# Author                                                                              #
#     : Sarfarazkhan                         			 				              #
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

source $MODULE_HOME/bin/comsys_functions.sh
source $MODULE_HOME/etc/comsys-modules.properties

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

startTime=`date +"%T"`
batchId=`cat $SUBJECT_AREA_HOME$BATCH_ID_FILE`
jobName=$CLEAN_UP_SSH_ACTION
logFileName=$jobName"_"$batchId".log"
logFilePath=$PROJECTS_HOME$PROJ_TMP$batchId/$logFileName

##############################################################################
#																			 #									
# Deleting all files from tmp/{batchId} directory							 #
#                                                                        	 #
##############################################################################

if [ "$(ls -A $PROJECTS_HOME$PROJ_TMP/$batchId$STAGING)" ]
then
	rm -r $PROJECTS_HOME$PROJ_TMP/$batchId$STAGING/*
fi
fn_log_info "Deleting file from stagging directory [$PROJECTS_HOME$PROJ_TMP/$batchId$STAGING/*]" "${logFilePath}"

if [ "$(ls -A $PROJECTS_HOME$PROJ_TMP/$batchId)" ]
then
	rm -r $PROJECTS_HOME$PROJ_TMP/$batchId
fi
fn_log_info "Deleting file from tmp directory [$PROJECTS_HOME$PROJ_TMP/$batchId/*]" "${logFilePath}"



##############################################################################
#																			 #									
# Deleting BATCH_ID file													 #
#                                                                        	 #
##############################################################################

if [ -e $SUBJECT_AREA_HOME$BATCH_ID_FILE ]
then
	rm $SUBJECT_AREA_HOME$BATCH_ID_FILE
fi
fn_log_info "Removing BatchId [$batchId]" "${logFilePath}"

##############################################################################
#																			 #									
# Creating empty file														 #
#                                                                        	 #
##############################################################################

touch $SUBJECT_AREA_HOME$BATCH_ID_FILE

exitCode="$?"

endTime=`date +"%T"`

fn_job_statistics "$startTime" "$endTime" "$exitCode" "$1" "$NOTAPPLICABLE" "$jobName" "$NOTAPPLICABLE" "$logFileName"



##############################################################################
#                                    End                                     #
##############################################################################
