#!/bin/bash

##############################################################################
#                              General Details                               #
##############################################################################
#                                                                            #
# Name                                                                       #
#     : validate_hive_schema      								   	 		 #
# File                                                                       #
#     : validate_hive_schema.sh                                   	         #
# Description                                                                #
#     : Validate hive schema 												 #
#                                                                            #
#                                                                            #
#                                                                            #
# Author                                                                     #
#     : Kishan                         			 					         #
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
source $MODULE_HOME/etc/coremetrics-modules.properties

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

startTime=`date +"%T"`
batchId=`cat $SUBJECT_AREA_HOME$BATCH_ID_FILE`
jobName=$VALIDATE_SCHEMA_SSH_ACTION
logFileName=$jobName"_"$batchId".log"
logFilePath=$PROJECTS_HOME$PROJ_TMP/$batchId/$logFileName

################# ################# ##########################################
#                                                                            #  	
#  Validating hive database    									             #                                     
#                                                                            #
##############################################################################

hiveDatabase=$PROJECTS_HOME$PROJ_TMP/$batchId/hive_database_list.txt
mailContent=$PROJECTS_HOME$PROJ_TMP/$batchId/mail_body.txt

hive -e "show databases;"> $hiveDatabase
#hiveDatabaseList=$(cat $hiveDatabase | tr "\n" " ")
hiveDatabaseList=`grep -x "$DB_INCOMING\|$DB_WORK\|$DB_GOLD\|$DB_AUDIT" $hiveDatabase  | tr "\n" " "`
totalDatabaseList=""$DB_INCOMING" "$DB_WORK" "$DB_GOLD" "$DB_AUDIT""
listOfDatabase=$(echo ${hiveDatabaseList[*]} ${totalDatabaseList[*]} | tr ' ' '\n' | sort | uniq -u)
echo " DATABASE CREATED ON `date` :  $listOfDatabase" > $mailContent
fn_log_info "Successfully Validated Databases" "${logFilePath}"

#############################################################################
#                                                                           #
# Validating hive tables                                                    #        
#                                                                           #
#############################################################################

hiveTable=$PROJECTS_HOME$PROJ_TMP/$batchId/hive_table_list.txt

for var in $hiveDatabaseList
do 
	hive -e "use $var; show tables;" >> $hiveTable
done
fileItem=$(cat $hiveTable | tr "\n" " ")
hiveTableList=""${ARRAY_INCOMING_RAW_TBLS[@]}" "${ARRAY_GOLD_TBLS[@]}" "${ARRAY_WORK_TBLS[@]}" "${ARRAY_ADDITIONAL_TBLS[@]}"" 
#filePath='$PROJECTS_HOME$PROJ_TMP/$batchId/'
listOfTables=$(echo ${fileItem[*]} ${hiveTableList[@]} | tr ' ' '\n' | sort | uniq -u) 
echo "TABLES CREATED ON `date` : $listOfTables" >> $mailContent
numberOfWords=`echo $listOfTables | wc -m`
fn_log_info "Successfully Validated Tables" "${logFilePath}"

if [ $numberOfWords -gt 1 ]
	then
	mailx -s  "ALERT MESSAGE OF HIVE" $MAIL_TO < $mailContent
fi 

fn_log_info "Successfully send email to user[$MAIL_TO]" "${logFilePath}"
exitCode="$?"

endTime=`date +"%T"`
 
fn_job_statistics "$startTime" "$endTime" "$exitCode" "$1" "$NOTAPPLICABLE" "$jobName" "$NOTAPPLICABLE" "$logFileName"

##############################################################################
#                                    End                                     #
##############################################################################

