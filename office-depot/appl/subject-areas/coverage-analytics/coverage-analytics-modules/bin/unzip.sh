#!/bin/bash

##############################################################################
#                              General Details                               #
##############################################################################
#                                                                            #
# Name                                                                       #
#     : unzip      								   	 						 #
# File                                                                       #
#     : unzip.sh                                   	                         #
# Description                                                                #
#     : Unzips the Data located on SFTP and store into a tmp directory       #
#                                                                            #
#                                                                            #
#                                                                            #
# Author                                                                     #
#     : Shweta Karwa                         			 					 #
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

source $MODULE_HOME/bin/coverageAnalytics_functions.sh

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

startTime=`date +"%T"`
batchId=`cat $SUBJECT_AREA_HOME$BATCH_ID_FILE`
jobName=$UNZIP_SSH_ACTION
logFileName=$jobName"_"$batchId".log"
logFilePath=$PROJECTS_HOME$PROJ_TMP/$batchId/$logFileName

##############################################################################
#																			 #
# Deleting files from tmp													 #
#																			 #
##############################################################################

fn_log_info "Deleting file from Directory [ $PROJECTS_HOME$PROJ_TMP/$batchId/* ]" "${logFilePath}"
if [ "$(ls -A $PROJECTS_HOME$PROJ_TMP/$batchId)" ]
then
	rm -r $PROJECTS_HOME$PROJ_TMP/$batchId/*
fi

fn_local_create_directory_if_not_exist "$PROJECTS_HOME$PROJ_TMP/$batchId$STAGING" "1" "${logFilePath}"

##############################################################################
#																			 #
# Changing pwd directory to tmp											     #
#																			 #
##############################################################################
   
fn_log_info "Moving to Directory [ $PROJECTS_HOME$PROJ_TMP/$batchId/ ]" "${logFilePath}"
cd $PROJECTS_HOME$PROJ_TMP/$batchId/

##############################################################################
#																			 #
# Connecting to SFTP and downloading latest file							 #
#																			 #
##############################################################################
	
   fn_log_info "Connecting to SFTP Server : $HOST:$SERVER_DIR" ${logFilePath}
   lftp -u $USER,$PASSWD sftp://$HOST:$SERVER_DIR << end
   fn_log_info "Downloading Files to Directory [ $PROJECTS_HOME$PROJ_TMP/$batchId/ ]" "${logFilePath}"
   mget "*"$REMOTE_FILE
end


##############################################################################
#																			 #
# Unzipping files in tmp directory										     #
#																			 #
##############################################################################

fn_log_info "Unzipping Files in directory [ $PROJECTS_HOME$PROJ_TMP/$batchId ]" "${logFilePath}"

ARRAY_ZIP_FILES=(*.zip)
for i in "${ARRAY_ZIP_FILES[@]}"
do
  # prefix=`echo $i | cut -d'_' -f1`
   unzip $PROJECTS_HOME$PROJ_TMP/$batchId/$i -d $PROJECTS_HOME$PROJ_TMP/$batchId/

   mv *.csv $PROJECTS_HOME$PROJ_TMP/$batchId$STAGING/
   
done
   
exitCode="$?"

endTime=`date +"%T"`
 
fn_job_statistics "$startTime" "$endTime" "$exitCode" "$1" "$NOTAPPLICABLE" "$UNZIP_SSH_ACTION" "$NOTAPPLICABLE" "$logFileName"
##############################################################################
#                                    End                                     #
##############################################################################