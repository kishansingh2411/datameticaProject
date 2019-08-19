##############################################################################
#                              General Details                               #
##############################################################################
#                                                                            #
# Name: functions                                                            #
#           								   	  							 #
# File: functions                                                            #
#                                        	   								 #
# Description: List of reusable functions defined.                           #
#                   	   				 									 #
#        1. fn_create_hive_tbl                                               #
#		 2. fn_move_to_incoming												 #
#		 3. fn_pig_wrapper													 #
#		 4. fn_job_statistics										         #
#                                                                   		 #
# Author: Sarfarazkhan / Harshwardhan                                        #
#                              			   									 #
#                                                                            #
##############################################################################

##############################################################################
#																			 #
# Importing properties files												 #
#																			 #
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

echo "MODULE_HOME :: $MODULE_HOME" 
echo "MODULES_HOME :: $MODULES_HOME"
echo "SUBJECT_AREA_HOME :: $SUBJECT_AREA_HOME"
echo "PROJECT_HOME :: $PROJECT_HOME"
echo "PROJECTS_HOME :: $PROJECTS_HOME"


source $PROJECT_HOME/etc/namespace.properties
source $MODULE_HOME/etc/comsys-modules.properties
source $PROJECT_HOME/bin/functions.sh

#echo "$LOCAL_PREFIX";
#exit;

###############################################################################                                                                                                                                                        #
# 5. Function moves incoming files to HDFS.                                   #
#                                                                             #
# Takes two parameters                                                        #
#       1. File-table mapping                                                 #
#   2. Record Type                                                            #
#                                                                             #
###############################################################################



function fn_move_to_incoming() {

   tableDetails=$1
   recordType=$2
   startTime=`date +"%T"`

   batchId=`cat $SUBJECT_AREA_HOME$BATCH_ID_FILE`
   table_name=`echo $tableDetails | cut -d',' -f1`

   jobName="COMSYS_"$table_name"_JOB"
   jobName=$(echo $jobName | tr 'a-z' 'A-Z')

   logFileName=$jobName"_"$batchId".log"
   logFilePath=$PROJECTS_HOME$PROJ_TMP/$batchId/$logFileName
   
   incoming_filename=`echo $tableDetails | cut -d',' -f2`
   echo "$PROJECTS_HOME" 
   echo "cd $PROJECTS_HOME/comsys"
 
   cd $PROJECTS_HOME/comsys
   cp *.csv $PROJECTS_HOME$PROJ_TMP/$batchId$STAGING
   fn_log_info "Successfully Copied all files to staging directory" "${logFilePath}"
   
        echo "for table $table_name"
   		fn_hadoop_create_directory_if_not_exists $INCOMING_HDFS/$table_name"/batch_id="$batchId 1
   		fn_hdp_put $PROJECTS_HOME$PROJ_TMP/$batchId$STAGING $INCOMING_HDFS/$table_name"/batch_id="$batchId $incoming_filename 1 
  		fn_log_info "Successfully file Copied to hdfs path of table [$table_name] from staging directory" "${logFilePath}"
   		hive -e " use $DB_INCOMING ; alter table $table_name add partition(batch_id='$batchId') "
    	exitCode="$?"
		
		successMessage="Altered table [$table_name]"
	    failureMessage="Failed to alter table [$table_name]"
	    failOnError=0
	
	    fn_handle_exit_code "${exitCode}" "${successMessage}" "${failureMessage}" "${failOnError}" "${logFilePath}"
	#    fn_log_info "Altered table [$table_name]" "${logFilePath}"
	#    exitCode="$?"
	#    successMessage="successfully added batch_id=$batchId partition to table $table_name"
	#    failureMessage="Failed to add batch_id=$batchId partition to table $table_name"
	#	failOnError=1
	#	fn_handle_exit_code "${exitCode}" "${successMessage}" "${failureMessage}" "${failOnError}" ${logFilePath}
	#	fn_log_info "Successfully added [batch_id=$batchId] partition for incoming table [$table_name]" "${logFilePath}"
	    
	    endTime=`date +"%T"`
		fn_job_statistics "$startTime" "$endTime" "$exitCode" "$recordType" "$NOTAPPLICABLE" "$jobName" "$table_name" "$logFileName"
  
}

function fn_create_hive_tbl() {
   logFilePath=$2
   ddl_path=$MODULE_HOME/schema/create_hive_tables
   batchId=`cat $SUBJECT_AREA_HOME$BATCH_ID_FILE`

  hive $1 -hiveconf DB_INCOMING=$DB_INCOMING -hiveconf DB_AUDIT=$DB_AUDIT \
  -hiveconf TBL_INCOMING_ODIDTA_GB_SIZBOX=$INCOMING_ODIDTA_GB_SIZBOX \
  -hiveconf TBL_INCOMING_ODIDTA_GB_SIZDTL=$INCOMING_ODIDTA_GB_SIZDTL -hiveconf TBL_INCOMING_ODIDTA_GB_SIZHED=$INCOMING_ODIDTA_GB_SIZHED \
  -hiveconf TBL_INCOMING_ODIDTA_GB_FILHED=$INCOMING_ODIDTA_GB_FILHED -hiveconf TBL_JOB_STATISTIC=$JOB_STATISTIC \
  -hiveconf INCOMING_HDFS=$INCOMING_HDFS -hiveconf AUDITLOG_HDFS=$AUDITLOG_HDFS -S -f $ddl_path".hql" >> ${logFilePath} 2>&1;

  fn_log_info "Successfully created hive schema" "${logFilePath}"
 }

function fn_job_statistics()
{
	startTime=$1
	endTime=$2
	exitCode=$3
	recordType=$4
	recordCount=$5
	jobName=$6
	tableName=$7
	logFileName=$8
	
	#echo "$exitCode" "$recordType" "$recordCount" "$jobName" "$tableName" "$logFileName"
	batchId=`cat $SUBJECT_AREA_HOME$BATCH_ID_FILE`
	currentDate=`date +%Y%m%d`
	
	let diff_dt=(`date +%s -d "$endTime" `-`date +%s -d "$startTime"`);
	timeTaken=`date -u -d @${diff_dt} +"%T"`
	
    auditLogFilePath=$PROJECTS_HOME$PROJ_TMP/$batchId/
    auditLogFileName="STATS_"$jobName"_"$batchId".log"
	
	if [ "$tableName" == "$NOTAPPLICABLE" ]
	then
    	successMessage="Job executed successfully [$jobName]"
    	failureMessage="Failed while executing job [$jobName]"
    else
    	successMessage="Data successfully loaded for table $tableName"
    	failureMessage="Failed to Load data for table $tableName"
    fi	
    failOnError=0

    hdfsLogPath=$LOG_HDFS/$currentDate/
    #echo "hdfsLogPath : $hdfsLogPath"
    logFilePath=$PROJECTS_HOME$PROJ_TMP/$batchId/
    #echo "logFilePath : $logFilePath"
    
    fn_handle_exit_code "${exitCode}" "${successMessage}" "${failureMessage}" "${failOnError}" "${logFilePath}${logFileName}"
    
    if [ $exitCode == 0 ]
    then
       statusCode="SUCCESS"
    else
       statusCode="FAILED"
    fi

	echo "$batchId|$jobName|$tableName|$USER_RAN|$startTime|$endTime|$timeTaken|$recordCount|$statusCode|$recordType|$hdfsLogPath$logFileName" > $auditLogFilePath$auditLogFileName
	
	fn_hadoop_create_directory_if_not_exists $AUDITLOG_HDFS/$JOB_STATISTIC"/current_date="$currentDate 1 "${logFilePath}${logFileName}"
	
	hdfsAuditLogPath=$AUDITLOG_HDFS/$JOB_STATISTIC"/current_date="$currentDate"/"
	#echo "hdfsAuditLogPath : $hdfsAuditLogPath"
    fn_hdp_put "${auditLogFilePath}" "${hdfsAuditLogPath}" "${auditLogFileName}" 1 "${logFilePath}${logFileName}"
    
     fn_hdp_put "${logFilePath}" "${hdfsLogPath}" "${logFileName}" 1 "${logFilePath}${logFileName}"
}
