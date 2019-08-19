##############################################################################
#                              General Details                               #
##############################################################################
#                                                                            #
# Name: functions                                                            #
#                                                                            #
# File: functions                                                            #
#                                                                            #
# Description: List of reusable functions defined.                           #
#                                                                            #
#                1. fn_create_hive_tbl                                       #
#                2. fn_insert_work_tbl										 #
#				 3.	fn_move_to_incoming 									 #
#				 4.	fn_alter_gold_table                                      #
#                5. fn_pig_wrapper                                           #
#                6. fn_job_statistics                                        #
#                															 #
# Author: Shweta and Sonali                                                  #                                                            #
#                                                                            #
##############################################################################

##############################################################################
#                                                                            #
# Importing properties files                                                 #
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

source $PROJECT_HOME/etc/namespace.properties
source $MODULE_HOME/etc/coverage-analytics-modules.properties
source $PROJECT_HOME/bin/functions.sh

##############################################################################
#                                                                            #
# 1.Function to create hive tables(incoming,work,gold)                       #
#                                                                            #
# Taking two parameter                                                       #
#       1. Debug mode                                                        #
#       2. Log File path                                                     #
#                                                                            #
##############################################################################

function fn_create_hive_tbl() {
   logFilePath=$2

   ddl_path=$MODULE_HOME/schema/create_hive_tables

   hive $1 -hiveconf DB_INCOMING=$DB_INCOMING -hiveconf DB_GOLD=$DB_GOLD -hiveconf DB_AUDIT=$DB_AUDIT -hiveconf DB_WORK=$DB_WORK \
  -hiveconf TBL_INCOMING_NL_UNIV_VIKBASE=$INCOMING_NL_UNIV_VIKBASE -hiveconf TBL_INCOMING_BE_UNIV_SCHOBER=$INCOMING_BE_UNIV_SCHOBER \
  -hiveconf TBL_INCOMING_GE_UNIV_SCHOBER=$INCOMING_GE_UNIV_SCHOBER -hiveconf TBL_INCOMING_IT_UNIV_DBITALY=$INCOMING_IT_UNIV_DBITALY \
  -hiveconf TBL_INCOMING_AT_UNIV_KSV=$INCOMING_AT_UNIV_KSV -hiveconf TBL_INCOMING_AT_UNIV_SCHOBER=$INCOMING_AT_UNIV_SCHOBER \
  -hiveconf TBL_INCOMING_UK_UNIV_LBMDATA=$INCOMING_UK_UNIV_LBMDATA -hiveconf TBL_INCOMING_IR_UNIV_BILLMOSS=$INCOMING_IR_UNIV_BILLMOSS \
  -hiveconf TBL_INCOMING_FR_MDUGAST_N80FIN=$INCOMING_FR_MDUGAST_N80FIN -hiveconf TBL_INCOMING_CDMPRDDTA_DM_ASSIGNED_CUSTOMER=$INCOMING_CDMPRDDTA_DM_ASSIGNED_CUSTOMER \
  -hiveconf TBL_INCOMING_CDMPRDDTA_DM_TRANSACTION_DTL=$INCOMING_CDMPRDDTA_DM_TRANSACTION_DTL -hiveconf TBL_INCOMING_CDMPRDDTA_DM_ASSOCIATE=$INCOMING_CDMPRDDTA_DM_ASSOCIATE \
  -hiveconf TBL_INCOMING_CDMPRDDTA_DM_COUNTRY=$INCOMING_CDMPRDDTA_DM_COUNTRY -hiveconf TBL_JOB_STATISTIC=$JOB_STATISTIC \
  -hiveconf TBL_INCOMING_CDMPRDDTA_DM_CUSTOMER_ACCOUNT=$INCOMING_CDMPRDDTA_DM_CUSTOMER_ACCOUNT -hiveconf TBL_INCOMING_CDMPRDDTA_DM_CALENDAR=$INCOMING_CDMPRDDTA_DM_CALENDAR \
  -hiveconf TBL_WORK_NL_UNIV_VIKBASE=$WORK_NL_UNIV_VIKBASE -hiveconf TBL_WORK_GE_UNIV_SCHOBER=$WORK_GE_UNIV_SCHOBER \
  -hiveconf TBL_WORK_BE_UNIV_SCHOBER=$WORK_BE_UNIV_SCHOBER -hiveconf TBL_WORK_IT_UNIV_DBITALY=$WORK_IT_UNIV_DBITALY \
  -hiveconf TBL_WORK_AT_UNIV_KSV=$WORK_AT_UNIV_KSV -hiveconf TBL_WORK_AT_UNIV_SCHOBER=$WORK_AT_UNIV_SCHOBER \
  -hiveconf TBL_WORK_UK_UNIV_LBMDATA=$WORK_UK_UNIV_LBMDATA -hiveconf TBL_WORK_IR_UNIV_BILLMOSS=$WORK_IR_UNIV_BILLMOSS \
  -hiveconf TBL_WORK_FR_MDUGAST_N80FIN=$WORK_FR_MDUGAST_N80FIN -hiveconf TBL_WORK_CDMPRDDTA_DM_ASSIGNED_CUSTOMER=$WORK_CDMPRDDTA_DM_ASSIGNED_CUSTOMER \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_TRANSACTION_DTL=$WORK_CDMPRDDTA_DM_TRANSACTION_DTL -hiveconf TBL_WORK_CDMPRDDTA_DM_ASSOCIATE=$WORK_CDMPRDDTA_DM_ASSOCIATE \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_COUNTRY=$WORK_CDMPRDDTA_DM_COUNTRY -hiveconf TBL_WORK_CDMPRDDTA_DM_CALENDAR=$WORK_CDMPRDDTA_DM_CALENDAR \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_CUSTOMER_ACCOUNT=$WORK_CDMPRDDTA_DM_CUSTOMER_ACCOUNT \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_NL=$WORK_CDMPRDDTA_DM_NL -hiveconf TBL_WORK_CDMPRDDTA_DM_BE=$WORK_CDMPRDDTA_DM_BE \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_GE=$WORK_CDMPRDDTA_DM_GE -hiveconf TBL_WORK_CDMPRDDTA_DM_AT=$WORK_CDMPRDDTA_DM_AT \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_UK=$WORK_CDMPRDDTA_DM_UK -hiveconf TBL_WORK_CDMPRDDTA_DM_FR=$WORK_CDMPRDDTA_DM_FR \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_IR=$WORK_CDMPRDDTA_DM_IR -hiveconf TBL_WORK_CDMPRDDTA_DM_IT=$WORK_CDMPRDDTA_DM_IT \
  -hiveconf TBL_GOLD_CDMPRDDTA_DM=$GOLD_CDMPRDDTA_DM -hiveconf TBL_GOLD_UNIV=$GOLD_UNIV \
  -hiveconf TBL_INCOMING_DIM_NACE=$INCOMING_DIM_NACE \
  -hiveconf OPENCSV_JAR=$OPENCSV_JAR \
  -hiveconf JOB_CREATE_SCHEMA_SSH_ACTION=$CREATE_SCHEMA_SSH_ACTION \
  -hiveconf GOLD_HDFS=$GOLD_HDFS -hiveconf INCOMING_HDFS=$INCOMING_HDFS -hiveconf AUDITLOG_HDFS=$AUDITLOG_HDFS -hiveconf WORK_HDFS=$WORK_HDFS -S -f $ddl_path".hql"  >> ${logFilePath} 2>&1;
     #echo "Hive command completed"
     fn_log_info "Successfully created hive schema" "${logFilePath}"
 }


##############################################################################
#                                                                            #
# 2.Function to insert into work table                                       #
#                                                                            #
# Taking two parameter                                                       #
#       1. Debug mode                                                        #
#       2. Log File path                                                     #	
#                                                                            #
##############################################################################

function fn_insert_work_tbl() {
   logFilePath=$2
   batch_id=$3
   table_name=$4

   ddl_path=$MODULE_HOME/$table_name"_tbl"/hive/$table_name

   hive $1 -hiveconf DB_INCOMING=$DB_INCOMING -hiveconf DB_WORK=$DB_WORK -hiveconf DB_AUDIT=$DB_AUDIT \
  -hiveconf batch_id=$batch_id \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_TRANSACTION_DTL=$WORK_CDMPRDDTA_DM_TRANSACTION_DTL \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_CALENDAR=$WORK_CDMPRDDTA_DM_CALENDAR \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_ASSIGNED_CUSTOMER=$WORK_CDMPRDDTA_DM_ASSIGNED_CUSTOMER \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_ASSOCIATE=$WORK_CDMPRDDTA_DM_ASSOCIATE \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_CUSTOMER_ACCOUNT=$WORK_CDMPRDDTA_DM_CUSTOMER_ACCOUNT \
  -hiveconf JOB_WORK_CDMPRDDTA_DM_AT_SSH_ACTION=$WORK_CDMPRDDTA_DM_AT_SSH_ACTION \
  -hiveconf JOB_WORK_CDMPRDDTA_DM_BE_SSH_ACTION=$WORK_CDMPRDDTA_DM_BE_SSH_ACTION \
  -hiveconf JOB_WORK_CDMPRDDTA_DM_FR_SSH_ACTION=$WORK_CDMPRDDTA_DM_FR_SSH_ACTION \
  -hiveconf JOB_WORK_CDMPRDDTA_DM_GE_SSH_ACTION=$WORK_CDMPRDDTA_DM_GE_SSH_ACTION \
  -hiveconf JOB_WORK_CDMPRDDTA_DM_IR_SSH_ACTION=$WORK_CDMPRDDTA_DM_IR_SSH_ACTION \
  -hiveconf JOB_WORK_CDMPRDDTA_DM_IT_SSH_ACTION=$WORK_CDMPRDDTA_DM_IT_SSH_ACTION \
  -hiveconf JOB_WORK_CDMPRDDTA_DM_NL_SSH_ACTION=$WORK_CDMPRDDTA_DM_NL_SSH_ACTION \
  -hiveconf JOB_WORK_CDMPRDDTA_DM_UK_SSH_ACTION=$WORK_CDMPRDDTA_DM_UK_SSH_ACTION \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_NL=$WORK_CDMPRDDTA_DM_NL -hiveconf TBL_WORK_CDMPRDDTA_DM_BE=$WORK_CDMPRDDTA_DM_BE \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_GE=$WORK_CDMPRDDTA_DM_GE -hiveconf TBL_WORK_CDMPRDDTA_DM_AT=$WORK_CDMPRDDTA_DM_AT \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_UK=$WORK_CDMPRDDTA_DM_UK -hiveconf TBL_WORK_CDMPRDDTA_DM_FR=$WORK_CDMPRDDTA_DM_FR \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_IR=$WORK_CDMPRDDTA_DM_IR -hiveconf TBL_WORK_CDMPRDDTA_DM_IT=$WORK_CDMPRDDTA_DM_IT \
  -hiveconf OPENCSV_JAR=$OPENCSV_JAR -hiveconf VARIABLE_UNAVAILABLE=$UNAVAILABLE \
  -hiveconf INCOMING_HDFS=$INCOMING_HDFS WORK_HDFS=$WORK_HDFS -hiveconf AUDITLOG_HDFS=$AUDITLOG_HDFS -S -f $ddl_path".hql"  >> ${logFilePath} 2>&1;
   #echo "Hive command completed"
   #fn_log_info "Successfully created work schema" "${logFilePath}"

 }

###############################################################################
#                                                                             #
# 3. Function to alter gold hive tables                                       #
#                                                                             #
###############################################################################

function fn_alter_gold_table() {
   echo "in function alter table"
   batch_id=$1
   table_name=$2
   country_code=$3

  if [[ -z "${country_code}"  ]]
   then
        echo "altering table $table_name"
        hive -e " use $DB_GOLD; alter table $table_name add partition(batch_id='$batch_id') "
#   elif [ $country_code == 'DM' ]
#   then
#        echo "altering table $GOLD_DM_TRANSACTION_DTL"
#        hive -e " use $DB_GOLD; alter table $GOLD_DM_TRANSACTION_DTL add partition(batch_id='$batch_id') "
   else
        echo "altering table $GOLD_UK_IR_FR_DATA"
        hive -e " use $DB_GOLD; alter table $GOLD_UK_IR_FR_DATA add partition(batch_id='$batch_id', country_code='$country_code') "
   fi
}
###############################################################################                                                                                                                                                        #
# 4. Wrapper Function to execute pig script and generate auditing logs        #
#                                                                             #
# Takes two parameters                                                        #
#   1. File-table mapping                                                     #
#   2. Record Type                                                            #
#                                                                             #
###############################################################################

function fn_pig_wrapper()
{
   table_name=$1
   recordType=$2
   startTime=`date +"%T"`
   batch_id=`cat $SUBJECT_AREA_HOME$BATCH_ID_FILE`
   jobName="COVERAGE_ANALYTICS_"$table_name"_JOB"
   jobName=$(echo $jobName | tr 'a-z' 'A-Z')
   logFileName=$jobName"_"$batch_id".log"
   logFilePath=$PROJECTS_HOME$PROJ_TMP/$batch_id/$logFileName
   pig_script_path=$MODULE_HOME"/"$table_name"_tbl/pig/"$table_name".pig"


   pig -useHCatalog -param batch_id=$batch_id -param record_type=$recordType -m "$PROJECT_HOME/etc/namespace.properties" -m "$MODULE_HOME/etc/coverage-analytics-modules.properties" -f $pig_script_path 2> ${logFilePath};
  
   exitCode="$?"
   successMessage="Data successfully loaded for table $table_name"
   failureMessage="Failed to Load data for table $table_name"
   failOnError=0
   fn_handle_exit_code "${exitCode}" "${successMessage}" "${failureMessage}" "${failOnError}" ${logFilePath}
   recordCount=`cat $logFilePath | grep "Total records written"| cut -d" " -f5`
   statusCount=`cat $logFilePath | grep "Success!" | wc -l`
   if [ $statusCount == 0 ]
   then
      status_code="Failed"
   else
      status_code="Success"
   fi
    endTime=`date +"%T"`
   fn_job_statistics "$startTime" "$endTime" "$exitCode" "$recordType" "$recordCount" "$jobName" "$table_name" "$logFileName"
}

##############################################################################
#																			 #
# Function to manage hive tables 											 #
#																			 #
# Taking two parameter													     #
# 	1. Debug mode												             #
# 	2. Log File path												         #
#																			 #
##############################################################################

function fn_manage_hive_tbl() {
   logFilePath=$2
   fileName=$3
   	
   ddl_path=$MODULE_HOME/schema/$fileName
   batchId=`cat $SUBJECT_AREA_HOME$BATCH_ID_FILE`
			
    hive $1 -hiveconf DB_GOLD=$DB_GOLD -hiveconf DB_WORK=$DB_WORK \
  -hiveconf batchId=$batchId \
  -hiveconf CC_country_code_at=$country_code_at \
  -hiveconf CC_country_code_be=$country_code_be -hiveconf CC_country_code_fr=$country_code_fr \
  -hiveconf CC_country_code_ge=$country_code_ge \
  -hiveconf CC_country_code_it=$country_code_it -hiveconf CC_country_code_ir=$country_code_ir \
  -hiveconf CC_country_code_nl=$country_code_nl -hiveconf CC_country_code_uk=$country_code_uk \
  -hiveconf TBL_GOLD_UNIV=$GOLD_UNIV -hiveconf TBL_GOLD_CDMPRDDTA_DM=$GOLD_CDMPRDDTA_DM \
  -hiveconf TBL_WORK_NL_UNIV_VIKBASE=$WORK_NL_UNIV_VIKBASE -hiveconf TBL_WORK_GE_UNIV_SCHOBER=$WORK_GE_UNIV_SCHOBER \
  -hiveconf TBL_WORK_BE_UNIV_SCHOBER=$WORK_BE_UNIV_SCHOBER -hiveconf TBL_WORK_IT_UNIV_DBITALY=$WORK_IT_UNIV_DBITALY \
  -hiveconf TBL_WORK_AT_UNIV_KSV=$WORK_AT_UNIV_KSV -hiveconf TBL_WORK_AT_UNIV_SCHOBER=$WORK_AT_UNIV_SCHOBER \
  -hiveconf TBL_WORK_UK_UNIV_LBMDATA=$WORK_UK_UNIV_LBMDATA -hiveconf TBL_WORK_IR_UNIV_BILLMOSS=$WORK_IR_UNIV_BILLMOSS \
  -hiveconf TBL_WORK_FR_MDUGAST_N80FIN=$WORK_FR_MDUGAST_N80FIN -hiveconf TBL_WORK_CDMPRDDTA_DM_ASSIGNED_CUSTOMER=$WORK_CDMPRDDTA_DM_ASSIGNED_CUSTOMER \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_TRANSACTION_DTL=$WORK_CDMPRDDTA_DM_TRANSACTION_DTL -hiveconf TBL_WORK_CDMPRDDTA_DM_ASSOCIATE=$WORK_CDMPRDDTA_DM_ASSOCIATE \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_COUNTRY=$WORK_CDMPRDDTA_DM_COUNTRY -hiveconf TBL_WORK_CDMPRDDTA_DM_CALENDAR=$WORK_CDMPRDDTA_DM_CALENDAR \
  -hiveconf TBL_WORK_CDMPRDDTA_DM_CUSTOMER_ACCOUNT=$WORK_CDMPRDDTA_DM_CUSTOMER_ACCOUNT \
  -hiveconf JOB_MANAGE_GOLD_SCHEMA_SSH_ACTION=$MANAGE_GOLD_SCHEMA_SSH_ACTION \
  -hiveconf JOB_MANAGE_WORK_SCHEMA_SSH_ACTION=$MANAGE_WORK_SCHEMA_SSH_ACTION \
    -hiveconf GOLD_HDFS=$GOLD_HDFS -hiveconf WORK_HDFS=$WORK_HDFS -S -f $ddl_path".hql"  >> ${logFilePath} 2>&1;
     #echo "Hive command completed"
     fn_log_info "Successfully alter work tables" "${logFilePath}"
 }

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

   jobName="COVERAGEANALYTICS_"$table_name"_JOB"
   jobName=$(echo $jobName | tr 'a-z' 'A-Z')

   logFileName=$jobName"_"$batchId".log"
   logFilePath=$PROJECTS_HOME$PROJ_TMP/$batchId/$logFileName

   incoming_filename=`echo $tableDetails | cut -d',' -f2`
   country_code=`echo $incoming_filename | cut -d'_' -f1`
   country_code=$(echo $country_code | tr 'a-z' 'A-Z')
   
     if [ $country_code == 'DM' ]
   then
        echo "altering table $table_name"
   		fn_hadoop_create_directory_if_not_exists $INCOMING_HDFS/$table_name"/batch_id="$batchId 1 ${logFilePath}
   		fn_hdp_put $PROJECTS_HOME$PROJ_TMP/$batchId$STAGING $INCOMING_HDFS/$table_name"/batch_id="$batchId $incoming_filename 1 ${logFilePath}
  		fn_log_info "Successfully Copied to table [$table_name] from staging directory" "${logFilePath}"
   		hive -e " use $DB_INCOMING ; alter table $table_name add partition(batch_id='$batchId') "
   		exitCode="$?"
		successMessage="Altered table [$table_name]"
	    failureMessage="Failed to alter table [$table_name]"
		failOnError=0
		fn_handle_exit_code "${exitCode}" "${successMessage}" "${failureMessage}" "${failOnError}" ${logFilePath}
		fn_log_info "Altered table [$table_name]" "${logFilePath}"
	    exitCode="$?"
	    successMessage="successfully added batch_id=$batchId partition to table $table_name"
	    failureMessage="Failed to add batch_id=$batchId partition to table $table_name"
		failOnError=1
		fn_handle_exit_code "${exitCode}" "${successMessage}" "${failureMessage}" "${failOnError}" ${logFilePath}
		fn_log_info "Successfully added [batch_id=$batchId] partition for incoming table [$table_name]" "${logFilePath}"
		endTime=`date +"%T"`
		fn_job_statistics "$startTime" "$endTime" "$exitCode" "$recordType" "$NOTAPPLICABLE" "$jobName" "$table_name" "$logFileName"
   else
   		fn_hadoop_create_directory_if_not_exists $INCOMING_HDFS/$table_name"/batch_id="$batchId"/country_code="$country_code 1 ${logFilePath}
		fn_hdp_put $PROJECTS_HOME$PROJ_TMP/$batchId$STAGING $INCOMING_HDFS/$table_name"/batch_id="$batchId"/country_code="$country_code $incoming_filename 1 ${logFilePath}
   		fn_log_info "Successfully Copied to table [$table_name] from staging directory" "${logFilePath}"
   		hive -e " use $DB_INCOMING ; alter table $table_name add partition(batch_id=$batchId,country_code='$country_code') "
			exitCode="$?"
		successMessage="Altered table [$table_name]"
	    failureMessage="Failed to alter table [$table_name]"
	    failOnError=0
	    fn_handle_exit_code "${exitCode}" "${successMessage}" "${failureMessage}" "${failOnError}" ${logFilePath}
		fn_log_info "Altered table [$table_name]" "${logFilePath}"
	    exitCode="$?"
	    successMessage="successfully added batch_id=$batchId , country_code=$country_code partition to table $table_name"
	    failureMessage="Failed to add batch_id=$batchId , country_code=$country_code partition to table $table_name"
	    failOnError=1
	    fn_handle_exit_code "${exitCode}" "${successMessage}" "${failureMessage}" "${failOnError}" ${logFilePath}
	    fn_log_info "Successfully added [batch_id=$batchId,country_code=$country_code] partition for incoming table [$table_name]" "${logFilePath}"
		endTime=`date +"%T"`
		fn_job_statistics "$startTime" "$endTime" "$exitCode" "$recordType" "$NOTAPPLICABLE" "$jobName" "$table_name" "$logFileName"
   fi
}

###############################################################################
# 6. Function to log job statistics.                                          #
#                                                                             #
# Takes following parameters                                                  #
#       1. Start Time of Job                                                  #
#       2. End Time of Job                                                    #
#       3. Exit Code                                                          #
#       4. Record Type (Daily/Weekly/Monthly/Quaterly/Yearly)                 #
#       5. Total Record Count                                                 #
#       6. JobName                                                            #
#       7. TableName                                                          #
#       8. LogFileName                                                        #
#                                                                             #
##############################################################################

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
       statusCode="Success"
    else
       statusCode="Failed"
    fi

        echo "$batchId|$jobName|$tableName|$USER_RAN|$startTime|$endTime|$timeTaken|$recordCount|$statusCode|$recordType|$hdfsLogPath$logFileName" > $auditLogFilePath$auditLogFileName

        fn_hadoop_create_directory_if_not_exists $AUDITLOG_HDFS/$JOB_STATISTIC"/current_date="$currentDate 1 "${logFilePath}${logFileName}"

        hdfsAuditLogPath=$AUDITLOG_HDFS/$JOB_STATISTIC"/current_date="$currentDate"/"
        #echo "hdfsAuditLogPath : $hdfsAuditLogPath"
    fn_hdp_put "${auditLogFilePath}" "${hdfsAuditLogPath}" "${auditLogFileName}" 1 "${logFilePath}${logFileName}"

     fn_hdp_put "${logFilePath}" "${hdfsLogPath}" "${logFileName}" 1 "${logFilePath}${logFileName}"
}
