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
# Author: Sarfarazkhan / Deepanshu / Shweta / Sonali                         #
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
COREMETRICS_PROPERTIES_MODULE_HOME=`dirname ${SCRIPT_HOME}`
COREMETRICS_PROPERTIES_MODULES_HOME=`dirname ${COREMETRICS_PROPERTIES_MODULE_HOME}`
COREMETRICS_PROPERTIES_SUBJECT_AREA_HOME=`dirname ${COREMETRICS_PROPERTIES_MODULES_HOME}`
COREMETRICS_PROPERTIES_PROJECT_HOME=`dirname ${COREMETRICS_PROPERTIES_SUBJECT_AREA_HOME}`
COREMETRICS_PROPERTIES_PROJECTS_HOME=`dirname ${COREMETRICS_PROPERTIES_PROJECT_HOME}`

source $COREMETRICS_PROPERTIES_PROJECT_HOME/etc/namespace.properties
source $COREMETRICS_PROPERTIES_MODULE_HOME/etc/coremetrics-modules.properties
source $COREMETRICS_PROPERTIES_PROJECT_HOME/bin/functions.sh

##############################################################################
#																			 #
# Function to create hive tables 											 #
#																			 #
# Taking two parameter													     #
# 	1. Debug mode												             #
# 	2. Log File path												         #
#																			 #
##############################################################################

function fn_create_hive_tbl() {
   logFilePath=$2
	
   ddl_path=$COREMETRICS_PROPERTIES_MODULE_HOME/schema/create_hive_tables
   batchId=`cat $SUBJECT_AREA_HOME$BATCH_ID_FILE`

  hive $1 -hiveconf DB_INCOMING=$DB_INCOMING -hiveconf DB_GOLD=$DB_GOLD -hiveconf DB_WORK=$DB_WORK -hiveconf DB_AUDIT=$DB_AUDIT \
  -hiveconf TBL_INCOMING_DIM_COOKIES=$INCOMING_DIM_COOKIES -hiveconf TBL_INCOMING_DIM_COUNTRY=$INCOMING_DIM_COUNTRY \
  -hiveconf TBL_INCOMING_ABANDONMENT=$INCOMING_CART_ITEM_ABANDONMENT -hiveconf TBL_INCOMING_ADDITION=$INCOMING_CART_ITEM_ADDITION \
  -hiveconf TBL_INCOMING_CONVERSION=$INCOMING_CONVERSION_EVENT -hiveconf TBL_INCOMING_ELEMENT=$INCOMING_ELEMENT \
  -hiveconf TBL_INCOMING_GEOGRAPHY=$INCOMING_GEOGRAPHY -hiveconf TBL_INCOMING_MMC_CLICK=$INCOMING_MMC_CLICK \
  -hiveconf TBL_INCOMING_ORDER=$INCOMING_ORDER -hiveconf TBL_INCOMING_PAGE_VIEW=$INCOMING_PAGE_VIEW \
  -hiveconf TBL_INCOMING_PURCHASE=$INCOMING_CART_ITEM_PURCHASE -hiveconf TBL_INCOMING_PRODUCT_VIEW=$INCOMING_PRODUCT_VIEW \
  -hiveconf TBL_INCOMING_REAL_ESTATE=$INCOMING_REAL_ESTATE_CLICK  -hiveconf TBL_INCOMING_TECHNICAL_PROPERTIES=$INCOMING_TECHNICAL_PROPERTIES \
  -hiveconf TBL_INCOMING_SITE_PROMOTION=$INCOMING_SITE_PROMOTION_CLICK -hiveconf TBL_INCOMING_REGISTRATION=$INCOMING_REGISTRATION \
  -hiveconf TBL_INCOMING_VALIDATION=$INCOMING_VALIDATION -hiveconf TBL_INCOMING_SESSION_FIRST=$INCOMING_SESSION_FIRST_PAGE_VIEW \
  -hiveconf TBL_GOLD_PURCHASE=$GOLD_CART_ITEM_PURCHASE -hiveconf TBL_GOLD_TECHNICAL_PROPERTIES=$GOLD_TECHNICAL_PROPERTIES \
  -hiveconf TBL_GOLD_ABANDONMENT=$GOLD_CART_ITEM_ABANDONMENT -hiveconf TBL_GOLD_SESSION_FIRST=$GOLD_SESSION_FIRST_PAGE_VIEW \
  -hiveconf TBL_GOLD_CONVERSION=$GOLD_CONVERSION_EVENT -hiveconf TBL_GOLD_ELEMENT=$GOLD_ELEMENT \
  -hiveconf TBL_GOLD_MMC_CLICK=$GOLD_MMC_CLICK -hiveconf TBL_GOLD_ORDER=$GOLD_ORDER  \
  -hiveconf TBL_GOLD_PAGE_VIEW=$GOLD_PAGE_VIEW -hiveconf TBL_GOLD_PRODUCT_VIEW=$GOLD_PRODUCT_VIEW \
  -hiveconf TBL_GOLD_REAL_ESTATE=$GOLD_REAL_ESTATE_CLICK -hiveconf TBL_GOLD_REGISTRATION=$GOLD_REGISTRATION \
  -hiveconf TBL_GOLD_SITE_PROMOTION=$GOLD_SITE_PROMOTION_CLICK -hiveconf TBL_GOLD_VALIDATION=$GOLD_VALIDATION \
  -hiveconf TBL_GOLD_GEOGRAPHY=$GOLD_GEOGRAPHY -hiveconf TBL_GOLD_ADDITION=$GOLD_CART_ITEM_ADDITION \
  -hiveconf TBL_WORK_ORDER=$WORK_ORDER -hiveconf TBL_WORK_PAGE_VIEW=$WORK_PAGE_VIEW \
  -hiveconf TBL_WORK_GEOGRAPHY_COUNTRY_COOKIE=$WORK_GEOGRAPHY_COUNTRY_COOKIE -hiveconf TBL_WORK_ABANDONMENT=$WORK_CART_ITEM_ABANDONMENT \
  -hiveconf TBL_WORK_ADDITION=$WORK_CART_ITEM_ADDITION -hiveconf TBL_WORK_PRODUCT_VIEW=$WORK_PRODUCT_VIEW \
  -hiveconf TBL_JOB_STATISTIC=$JOB_STATISTIC \
  -hiveconf TBL_VALIDATION=$VALIDATION \
  -hiveconf GOLD_HDFS=$GOLD_HDFS -hiveconf INCOMING_HDFS=$INCOMING_HDFS -hiveconf AUDITLOG_HDFS=$AUDITLOG_HDFS -hiveconf WORK_HDFS=$WORK_HDFS -S -f $ddl_path".hql"  >> ${logFilePath} 2>&1;
     #echo "Hive command completed"
     fn_log_info "Successfully created hive schema" "${logFilePath}"
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
	
   ddl_path=$COREMETRICS_PROPERTIES_MODULE_HOME/schema/manage_hive_tables
   batchId=`cat $COREMETRICS_PROPERTIES_SUBJECT_AREA_HOME$BATCH_ID_FILE`
  
  hive $1 -hiveconf DB_GOLD=$DB_GOLD \
  -hiveconf TBL_GOLD_PURCHASE=$GOLD_CART_ITEM_PURCHASE -hiveconf TBL_GOLD_TECHNICAL_PROPERTIES=$GOLD_TECHNICAL_PROPERTIES \
  -hiveconf TBL_GOLD_ABANDONMENT=$GOLD_CART_ITEM_ABANDONMENT -hiveconf TBL_GOLD_SESSION_FIRST=$GOLD_SESSION_FIRST_PAGE_VIEW \
  -hiveconf TBL_GOLD_CONVERSION=$GOLD_CONVERSION_EVENT -hiveconf TBL_GOLD_ELEMENT=$GOLD_ELEMENT \
  -hiveconf TBL_GOLD_MMC_CLICK=$GOLD_MMC_CLICK -hiveconf TBL_GOLD_ORDER=$GOLD_ORDER  \
  -hiveconf TBL_GOLD_PAGE_VIEW=$GOLD_PAGE_VIEW -hiveconf TBL_GOLD_PRODUCT_VIEW=$GOLD_PRODUCT_VIEW \
  -hiveconf TBL_GOLD_REAL_ESTATE=$GOLD_REAL_ESTATE_CLICK -hiveconf TBL_GOLD_REGISTRATION=$GOLD_REGISTRATION \
  -hiveconf TBL_GOLD_SITE_PROMOTION=$GOLD_SITE_PROMOTION_CLICK -hiveconf TBL_GOLD_VALIDATION=$GOLD_VALIDATION \
  -hiveconf TBL_GOLD_GEOGRAPHY=$GOLD_GEOGRAPHY -hiveconf TBL_GOLD_ADDITION=$GOLD_CART_ITEM_ADDITION \
  -hiveconf batchId=$batchId \
  -hiveconf GOLD_HDFS=$GOLD_HDFS -S -f $ddl_path".hql"  >> ${logFilePath} 2>&1;
     #echo "Hive command completed"
     fn_log_info "Successfully alter Gold tables" "${logFilePath}"
 }

##############################################################################
#																			 #
# Wrapper Function to execute pig script and generate auditing logs			 #
#																			 #
# Takes two parameters														 #
# 	1. File-table mapping 													 #
#   2. Record Type	  									 					 #
#																			 #
##############################################################################


function fn_pig_wrapper()
{

   table_name=$1
   recordType=$2
   #echo " i m in function file $table_name"
   startTime=`date +"%T"`
   
   batch_id=`cat $COREMETRICS_PROPERTIES_SUBJECT_AREA_HOME$BATCH_ID_FILE`
   
   jobName="COREMETRICS_"$table_name"_JOB"
   jobName=$(echo $jobName | tr 'a-z' 'A-Z')
   
   logFileName=$jobName"_"$batch_id".log"
   logFilePath=$COREMETRICS_PROPERTIES_PROJECTS_HOME$PROJ_TMP/$batch_id/$logFileName
                
   pig_script_path=$COREMETRICS_PROPERTIES_MODULE_HOME"/"$table_name"_tbl/pig/"$table_name".pig"
   
   
   #pig -useHCatalog -param batch_id=$batch_id -param record_type=$recordType -param_file $source_home$COREMETRICS_PROPERTIES_MODULE_HOME"/etc/coremetrics-modules.properties" -f $pig_script_path  2> $pig_log_file;        
   pig -useHCatalog -param batch_id=$batch_id -param record_type=$recordType -m "$COREMETRICS_PROPERTIES_PROJECT_HOME/etc/namespace.properties" -m "$COREMETRICS_PROPERTIES_MODULE_HOME/etc/coremetrics-modules.properties" -f $pig_script_path 2> ${logFilePath};
  
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
# Function moves incoming files to HDFS.									 #
#																			 #
# Takes two parameters														 #
# 	1. File-table mapping 													 #
#   2. Record Type	  									 					 #
#																			 #
##############################################################################
 
 
function fn_move_to_incoming() {
   tableDetails=$1
   recordType=$2
   
   startTime=`date +"%T"`
   
   batchId=`cat $COREMETRICS_PROPERTIES_SUBJECT_AREA_HOME$BATCH_ID_FILE`
     
   table_name=`echo $tableDetails | cut -d',' -f1`
   
   jobName="COREMETRICS_"$table_name"_JOB"
   jobName=$(echo $jobName | tr 'a-z' 'A-Z')		  
   
   logFileName=$jobName"_"$batchId".log"
   logFilePath=$COREMETRICS_PROPERTIES_PROJECTS_HOME$PROJ_TMP/$batchId/$logFileName
   client_name=$(ls $COREMETRICS_PROPERTIES_PROJECTS_HOME$PROJ_TMP/$batchId/ | grep -E '.zip'| cut -d '_' -f1)
   incoming_filename=`echo $tableDetails | cut -d',' -f2`
   

 ####   search for geography table ######
 
  if [ $incoming_filename == "Geography.txt" ]; then
    for i in $client_name
	  do 
		  incoming_filename_concat=$i"_"$incoming_filename
		  fn_hadoop_create_directory_if_not_exists $INCOMING_HDFS/$table_name"/batch_id="$batchId"/client_name="$i 1 ${logFilePath}
		  fn_hdp_put $COREMETRICS_PROPERTIES_PROJECTS_HOME$PROJ_TMP/$batchId$STAGING $INCOMING_HDFS/$table_name"/batch_id="$batchId"/client_name="$i  $incoming_filename_concat 1 ${logFilePath} 
		  hive -e " use $DB_INCOMING ; alter table $table_name add partition(batch_id=$batchId,client_name=$i) " 
		    
	  done  
       exitCode="$?"
	   successMessage="Altered table [$table_name]"
	   failureMessage="Failed to alter table [$table_name]"
	   failOnError=1
	   fn_handle_exit_code "${exitCode}" "${successMessage}" "${failureMessage}" "${failOnError}" ${logFilePath}
	   fn_log_info "Altered table [$table_name]" "${logFilePath}"
	   
	   endTime=`date +"%T"`  
       fn_job_statistics "$startTime" "$endTime" "$exitCode" "$recordType" "$NOTAPPLICABLE" "$jobName" "$table_name" "$logFileName"
 else 
       incoming_filename="*_"$incoming_filename
       fn_hadoop_create_directory_if_not_exists $INCOMING_HDFS/$table_name"/batch_id="$batchId 1 ${logFilePath}
       fn_hdp_put $COREMETRICS_PROPERTIES_PROJECTS_HOME$PROJ_TMP/$batchId$STAGING $INCOMING_HDFS/$table_name"/batch_id="$batchId"/"  $incoming_filename 1 ${logFilePath}
	   hive -e " use $DB_INCOMING ; alter table $table_name add partition(batch_id=$batchId) "
	   
       exitCode="$?" 
	   
       successMessage="Altered table [$table_name]"
       failureMessage="Failed to alter table [$table_name]"
       failOnError=1
       fn_handle_exit_code "${exitCode}" "${successMessage}" "${failureMessage}" "${failOnError}" ${logFilePath}
       fn_log_info "Successfully added [batch_id=$batchId] partition for incoming table [$table_name]" "${logFilePath}"
 
	   endTime=`date +"%T"`  
       fn_job_statistics "$startTime" "$endTime" "$exitCode" "$recordType" "$NOTAPPLICABLE" "$jobName" "$table_name" "$logFileName"
  fi
   
} 

##############################################################################
#																			 #
# Function to log job statistics.									         #
#																			 #
# Takes following parameters												 #
# 	1. Start Time of Job													 #
#	2. End Time of Job														 #
#	3. Exit Code															 #
#	4. Record Type (Daily/Weekly/Monthly/Quaterly/Yearly)					 #
#	5. Total Record Count													 #
#	6. JobName																 #
#	7. TableName															 #
#	8. LogFileName															 #
#																			 #
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
	batchId=`cat $COREMETRICS_PROPERTIES_SUBJECT_AREA_HOME$BATCH_ID_FILE`
	currentDate=`date +%Y%m%d`
	
	let diff_dt=(`date +%s -d "$endTime" `-`date +%s -d "$startTime"`);
	timeTaken=`date -u -d @${diff_dt} +"%T"`
	
    auditLogFilePath=$COREMETRICS_PROPERTIES_PROJECTS_HOME$PROJ_TMP/$batchId/
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
    logFilePath=$COREMETRICS_PROPERTIES_PROJECTS_HOME$PROJ_TMP/$batchId/
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
