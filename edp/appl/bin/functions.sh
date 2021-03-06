#!/bin/bash

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: function.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This script consists of all the utility functions that are used by
#			      module scripts. Modules shall not call and hadoop related commands
#       		  directly but instead write a wrapper function in this script with
#       		  proper handling of exit codes of the commands.
#					1) Variables defined as final in the document must not be modified   
#			           by the module script code.                                         
#			        2) If function argument ends with * then it means that required       
#			           argument.                                                          
#			        3) If function argument ends with ? then it means that optional       
#			           argument.                                            
#   Author(s)   : DataMetica Team
#   Usage       : 
#   Date        : 05/02/2016
#   Log File    : 
#   SQL File    : 
#   Error File  : 
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          05/02/2016       Initial version
#
#
#####################################################################################################################

###############################################################################
#                     Global Environment Base Properties                      #
###############################################################################

###                                                                           
# Integer representation of boolean true
# @Type  :  Integer
# @Final :  true
BOOLEAN_TRUE=1

###                                                                           
# Integer representation of boolean false
# @Type  :  Integer
# @Final :  true
BOOLEAN_FALSE=0

###                                                                           
# Integer representation of success exit code
# @Type  :  Integer
# @Final :  true
EXIT_CODE_SUCCESS=0

###                                                                           
# Integer representation of fail exit code
# @Type  :  Integer
# @Final :  true
EXIT_CODE_FAIL=1

###                                                                           
# Integer representation of exit code that will be returned when value for
# a required variable is not set
# @Type  :  Integer
# @Final :  true
EXIT_CODE_VARIABLE_NOT_SET=-10

###
# Holds current batch id. 
# Do not modify this in the module scripts. BATCH_ID could be set by the 
# workflow schedulers and passed into module scripts environment and can
# be accessed in this file.
# @Type   :  String
# @Format :  YYYYMMDDHHMMSS 
# @Final  :  true
BATCH_ID="${BATCH_ID}"

###
# Flat to check if the log directory for this module is setup or not.
# @Type  :  Boolean
# @Final :  flase
LOG_DIR_MODULE_SET="false"

###
# Log file name used to form log file names for
# the same module. Module log dir is set in the module's env properties file.
# @Type  :  String
# @Final :  true
LOG_FILE_NAME_MODULE=""

###
# Hive provides option to only pass single initialization file. To support
# overriding of hive variables at namespace, project, module level, a single
# file needs to be created out of the individual files provided at each level.
# This variable holds name of that file.
# @Type  :  Path
# @Final :  true
HIVE_INITIALISATION_FILE=""

###
# Path to repository directory which contains all the dependencies seperated
# by version. This allows multiple framework versions to co-exist across multiple
# projects or modules.
# @Type  :  Path
# @Final :  true
REPOSITORY_DIR="${HOME}/repository"

################################################################################
#                             Function Definitions                             #
################################################################################

################################################################################
#                                                                              #
# Function to check mandatory params                                           #
#                                                                              #
################################################################################

function fn_check_mandatory_params(){
key=`echo "$1"|awk '{split($0,a,","); print a[1]}'`
value=`echo "$1"|awk '{split($0,a,","); print a[2]}'`

param=$value
  if [ -z "$param" ]
  then
     echo "Missing mandatory parameters $key"
     exit
  fi
}

################################################################################
#                                                                              #
# Function to check period Name                                                #
#                                                                              #
################################################################################

function fn_check_period_name(){
period_name=$1

   if [[ $period_name == daily ]] || 
      [[ $period_name == weekly ]] || 
      [[ $period_name == monthly ]] || 
      [[ $period_name == quarterly ]] || 
      [[ $period_name == yearly ]]
    then
          echo "Correct period name '$period_name' provided"
    else
         echo "Period name '$period_name' is incorrect."
         echo " Period name must be one of [daily or weekly or monthly or quarterly or yearly]"
    exit
   fi
}

################################################################################
#                                                                              #
# Function to check date format		                                           #
#                                                                              #
################################################################################

function fn_check_date_format(){
 usrdate=$1

 if [[ $usrdate =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] 
 then
    date -d "$usrdate"  > /dev/null 2>&1
	if [ $? -eq 0 ]
	then
       echo ""
	else
       echo "Date $usrdate is in invalid format. Correct format is YYYY-MM-DD"
       exit -1	
	fi
 else 
    echo "Date $usrdate is in an invalid format (not YYYY-MM-DD)"
    exit -1
 fi
}

################################################################################
#                                                                              #
# Function to insert into job statistic table                                  #
#                                                                              #
# Parameter list                                                               #
# 1. batch id												                   #
# 2. Job name												                   #
# 3. start time of job												           #
# 4. job status												                   #
# 5. log file path												               # 
# 																			   #
################################################################################

function fn_insert_job_statistics(){
  
  batch_id=$1
  jobname=$2
  starttime=$3
  status=$4
  log_file_path=$5

export PGPASSWORD=$POSTGRES_PASSWORD;
export PGHOST=$POSTGRES_HOST;
export PGPORT=$POSTGRES_PORT;
psql $POSTGRES_DATABASE_NAME $POSTGRES_USERNAME 1>> ${log_file_path} 2>> ${log_file_path} << EOF
        insert into job_statistics(batch_id, job_name, start_time, status) values ('$batch_id','$jobname','$starttime','$status');
EOF
exit_code="$?"

fail_on_error=${BOOLEAN_TRUE}
success_message="Capture statistic for job : $jobname"
failure_message="Failed to Capture statistic for job : $jobname"
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file_path}"
}


################################################################################
#                                                                              #
# Function to insert into job cdc process log table                            #
#                                                                              #
# Parameter list                                                               #
# 1.DATASET_NAME												               #
# 2.TABLE_NAME												                   #
# 3.RUNDATE											                           #
# 4.DEDUP_CNT												                   #
# 5.INSERT_CNT                                                                 #
# 6.UPDATE_CNT																   #
# 7.DELETE_CNT														           #
# 8.TOTAL_CNT                                                                  #
# 9.log_file_path											                   # 
# 																			   #
################################################################################

function fn_insert_cdc_process(){
  
   dataset_name=$1     
   table_name=$2  
   rundate=$3 
   duplicate_cnt=$4        
   dedup_cnt=$5    
   insert_cnt=$6     
   update_cnt=$7    
   delete_cnt=$8     
   total_cnt=$9
   log_file_path="${10}"  

export PGPASSWORD=$POSTGRES_PASSWORD;
export PGHOST=$POSTGRES_HOST;
export PGPORT=$POSTGRES_PORT;
psql $POSTGRES_DATABASE_NAME $POSTGRES_USERNAME 1>> ${log_file_path} 2>> ${log_file_path} << EOF
   
   DELETE FROM cdc_process_log WHERE dataset_name = '${dataset_name}' and table_name = '${table_name}' and rundate = '${rundate}';
   
   insert into cdc_process_log(dataset_name,table_name,rundate,duplicate_cnt,dedup_cnt,insert_cnt,update_cnt,delete_cnt,total_cnt) values ('$dataset_name','$table_name','$rundate','$duplicate_cnt','$dedup_cnt','$insert_cnt','$update_cnt','$delete_cnt','$total_cnt');
EOF
exit_code="$?"

fail_on_error=${BOOLEAN_TRUE}
success_message="Capture statistic for job : $jobname"
failure_message="Failed to Capture statistic for job : $jobname"
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file_path}"
}

################################################################################
# 																			   #
# Function to update job statistic table 									   #
# 																			   #
# Parameter list                                                               #
# 1. batch id												                   #
# 2. Job name												                   #
# 3. start time of job												           #
# 4. number of records												           #
# 5. exit code of command												       #
# 6. source data timestamp												       #
# 7. log file path                                             				   #
# 8. source file name                                             			   #
# 9. args column value                                             			   #
# 																			   #
################################################################################

function fn_update_job_statistics(){

  batch_id=$1
  job_name=$2
  start_time=$3
  no_of_records=$4
  exit_code=$5
  source_data_timestamp=$6
  log_file_path=$7
  source_file_name=$8
  args=$9

  end_time=`date +"%Y-%m-%d %H:%M:%S"`
  log_file_name=`basename $log_file_path`
 
  let diff_dt=(`date +%s -d "$end_time" `-`date +%s -d "$start_time"`);
  totaltime=`date -u -d @${diff_dt} +"%T"`

  if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
  then
     status="FAILED" 
  else
     status="SUCCEEDED" 
  fi

  if [ "$#" == "10" ]
  then
     status="${10}"
  fi

export PGPASSWORD=$POSTGRES_PASSWORD;
export PGHOST=$POSTGRES_HOST;
export PGPORT=$POSTGRES_PORT;
psql $POSTGRES_DATABASE_NAME $POSTGRES_USERNAME 1>> ${log_file_path} 2>> ${log_file_path} << EOF
	update job_statistics set end_time='$end_time', total_time='$totaltime', 
	no_of_records='$no_of_records', status='$status', source_data_timestamp='$source_data_timestamp',
	log_file_path='$log_file_name', source_file_name='$source_file_name', args='$args' 
	where (batch_id='$batch_id' and job_name='$job_name' and start_time='$start_time');
EOF
  
  if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
  then
      exit "${exit_code}"
  fi
}

############################################
#									       #
#Function hadoop delete file               #
#                                          # 
############################################

function fn_hadoop_delete_file(){
  file="$1"
  fail_on_error="$2"
  log_file=$3	

  assert_variable_is_set "file" "${file}"

  hadoop fs -rm -r -f "${file}"
  exit_code="$?"

  success_message="Deleted file ${file}"
  failure_message="Failed to delete file ${file}"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
  return $exit_code
}

#########################################################
#  alter table with one partition and location			#
#														#
#########################################################

function fn_alter_table_with_one_partitions_with_location(){
  database="$1"
  table_name="${HIVE_TABLE_PREFIX}$2"
  partition_col1=$3
  location=$4
  fail_on_error=$5
  hiveserver2_url=$6
  hive_username=$7
  hive_password=$8
  log_file_path=$9
  
  beeline -u ${hiveserver2_url} -n ${hive_username} -p ${hive_password} --showHeader=false --silent=true -e \
  "use $database; alter table $table_name add IF NOT EXISTS partition($partition_col1) location '$location';" 1>> "${log_file_path}" 2>> "${log_file_path}"
  exit_code="$?"

  success_message="Alter table $table_name, added partition $partition_col1"
  failure_message="Failed to Alter table $table_name, while adding partition $partition_col1"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file_path}"
  return $exit_code
}

############################################
#									       #
#Function to add 1 partition to table      #
#                                          # 
############################################

function fn_alter_table_with_one_partitions(){
  database="$1"
  table_name="${HIVE_TABLE_PREFIX}$2"
  partition_col1=$3
  fail_on_error=$4
  hiveserver2_url=$5
  hive_username=$6
  hive_password=$7
  log_file_path=$8	

  beeline -u ${hiveserver2_url} -n ${hive_username} -p ${hive_password} --showHeader=false --silent=true -e \
  "use $database; alter table $table_name add IF NOT EXISTS partition($partition_col1);" 1>> "${log_file_path}" 2>> "${log_file_path}"
  exit_code="$?"

  success_message="Alter table $table_name, added partition $partition_col1"
  failure_message="Failed to Alter table $table_name, while adding partition $partition_col1"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file_path}"
  return $exit_code
}

############################################
#									       #
#Function to add 2 partition to table      #
#                                          # 
############################################

function fn_alter_table_with_two_partitions(){
  database=$1
  table_name="${HIVE_TABLE_PREFIX}$2"
  partition_col1=$3
  partition_col2=$4
  fail_on_error=$5
  hiveserver2_url=$6
  hive_username=$7
  hive_password=$8
  log_file_path=$9

  beeline -u ${hiveserver2_url} -n ${hive_username} -p ${hive_password} --showHeader=false --silent=true -e \
  "USE $database; ALTER TABLE $table_name ADD IF NOT EXISTS PARTITION ($partition_col1 , $partition_col2); " 1>> "${log_file_path}" 2>> "${log_file_path}"
  exit_code="$?"

  success_message="Alter table $table_name, added partition $partition_col1 and $partition_col2"
  failure_message="Failed to Alter table $table_name, while adding partition $partition_col1 and $partition_col2"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file_path}"
  return $exit_code
  
}

############################################
#									       #
#Function to drop 1 partition to table     #
#                                          # 
############################################

function fn_alter_drop_table_with_one_partitions(){
  database="$1"
  table_name="${HIVE_TABLE_PREFIX}$2"
  partition_col1=$3
  fail_on_error=$4
  hiveserver2_url=$5
  hive_username=$6
  hive_password=$7
  log_file_path=$8

  beeline -u ${hiveserver2_url} -n ${hive_username} -p ${hive_password} --showHeader=false --silent=true -e \
  "use $database; alter table $table_name drop partition($partition_col1);" 1>> "${log_file_path}" 2>> "${log_file_path}"
  exit_code="$?"

  success_message="Alter table $table_name, dropped partition $partition_col1"
  failure_message="Failed to Alter table $table_name, while dropping partition $$partition_col1"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file_path}"
  return $exit_code
}

############################################
#									       #
#Function to drop 2 partition to table     #
#                                          # 
############################################

function fn_alter_table_drop_two_partitions(){
  database="$1"
  table_name="${HIVE_TABLE_PREFIX}$2"
  partition_col1=$3
  partition_col2=$4
  fail_on_error=$5
  hiveserver2_url=$6
  hive_username=$7
  hive_password=$8
  log_file_path=$9

  beeline -u ${hiveserver2_url} -n ${hive_username} -p ${hive_password} --showHeader=false --silent=true -e \
  "use $database; alter table $table_name drop partition($partition_col1 , $partition_col2);" 1>> "${log_file_path}" 2>> "${log_file_path}"
  exit_code="$?"

  success_message="Alter table $table_name, dropped partition $partition_col1"
  failure_message="Failed to Alter table $table_name, while dropping partition $partition_col1 and $partition_col2"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file_path}"
  return $exit_code
}

############################################
#									       #
#Function to drop 3 partition to table     #
#                                          # 
############################################

function fn_alter_table_drop_three_partitions(){
  database="$1"
  table_name="${HIVE_TABLE_PREFIX}$2"
  partition_col1=$3
  partition_col2=$4
  partition_col3=$5
  fail_on_error=$6
  hiveserver2_url=$7
  hive_username=$8
  hive_password=$9
  log_file_path=${10}

  beeline -u ${hiveserver2_url} -n ${hive_username} -p ${hive_password} --showHeader=false --silent=true -e \
  "use $database; alter table $table_name drop partition($partition_col1 , $partition_col2, $partition_col3);" 1>> "${log_file_path}" 2>> "${log_file_path}"
  exit_code="$?"

  success_message="Alter table $table_name, dropped partition $partition_col1"
  failure_message="Failed to Alter table $table_name, while dropping partition $partition_col1 and $partition_col2 and $partition_col3"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file_path}"
  return $exit_code
}

########################################################################################
#																			 		   #
# Log any message                                                                      #
#                                                                                      #
# Arguments: message* - string message                                                 #
# 			 log_file* - path of log file                                              #   
#																			 		   # 
######################################################################################## 

function fn_log(){
  message=$1
  log_file=$2
  echo "${message}"  >> ${log_file}
}

########################################################################################
#																			 		   #
# Log info message                                                                     #  
#                                                                                      #  
# Arguments: message* - string message                                                 #
#		     log_file* - path of log file                                              #
#																			 		   # 
######################################################################################## 

function fn_log_info(){
  message=$1
  log_file=$2
  fn_log "INFO ${message}" "${log_file}"
}


########################################################################################
#																			 		   #
# Log warning message                                                                  #
#                                                                                      # 
# Arguments: message* - string message                                                 #
#		     log_file* - path of log file                                              #
#																			 		   # 
######################################################################################## 

function fn_log_warn(){
  message=$1
  log_file=$2
  fn_log "WARN ${message}" "${log_file}"
}


########################################################################################
#																			 		   #
# Log error message                                                                    #  
#                                                                                      #
# Arguments: message* - string message                                                 #
#		     log_file* - path of log file                                              #         
#																			 		   # 
######################################################################################## 

function fn_log_error(){
  message=$1
  log_file=$2
  fn_log "ERROR ${message}" "${log_file}"
}


########################################################################################
#																			 		   #
# Log given error message and exit the script with given exit code                     #
#                                                                                      #
# Arguments: exit_code* - exit code to be checked and exited with in case its not zero #
#   		 failure_message* - message to log if the exit code is non zero            #
#			 log_file* - path of log file                                              #
#																			 		   # 
######################################################################################## 

function fn_exit_with_failure_message(){
  exit_code=$1
  failure_message=$2
  log_file=$3

  fn_log_error "${failure_message}" "${log_file}"
  exit "${exit_code}"
}


########################################################################################
#																			 		   #
# Check the exit code and then log the message for success or failure based on         #
# exit code. If fail_on_error flag value is non zero and the exit code is non          #
# zero then this process exits with that exit code.                                    # 
#                                                                                      # 
# Arguments: exit_code* - exit code to be checked                                      #   
#   	     success_message* - message to log if the exit code is zero                #
#            failure_message* - message to log if the exit code is non-zero            #  
#            fail_on_error? - Flag to decide, in case of failure of this operation,    #
#							wheather to exit the process with error code or            #  
#							just write error message and return.                       #
#            log_file* - path of log file                                              #
#																			 		   # 
######################################################################################## 

function fn_handle_exit_code(){
  exit_code=$1
  success_message=$2
  failure_message=$3
  fail_on_error=$4
  log_file=$5 

  if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
  then
    fn_log_error "${failure_message}" "${log_file}"

    if [ ${fail_on_error} -ne $EXIT_CODE_SUCCESS ]
    then
      exit "${exit_code}"
    fi
  else
    fn_log_info "${success_message}" "${log_file}"
  fi
}


########################################################################################
#																			 		   #
# Verify if the variable is not-set/empty or not. In case it is non-set or empty,      #
# exit with failure message.                                                           #
#                                                                                      #
# Arguments: variable_name* - name of the variable to be check for being not-set/empty #
#   	     variable_value* - value of the variable                                   #   
#																			 		   # 
######################################################################################## 

function assert_variable_is_set(){
  variable_name=$1
  variable_value=$2

  if [ "${variable_value}" == "" ]
  then
    exit_code=${EXIT_CODE_VARIABLE_NOT_SET}

    failure_messages="Value for ${variable_name} variable is not set"
    fn_exit_with_failure_message "${exit_code}" "${failure_messages}" "${log_file}"
  fi
}


########################################################################################
#																			 		   #
# Delete local file                                                                    #
#                                                                                      #   
# Arguments: file* - path of the file to be deleted                                    #
#            fail_on_error? - Flag to decide, in case of failure of this operation,    #
#						wheather to exit the process with error code or                #
#						just write error message and return.                           #  
#																			 		   # 
######################################################################################## 

function fn_local_delete_file(){
  file="$1"
  fail_on_error="$2"
  log_file=$3	

  assert_variable_is_set "file" "${file}"

  rm -f "${file}"
  exit_code="$?"

  success_message="Deleted file ${file}"
  failure_message="Failed to delete file ${file}"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
  return $exit_code
}


########################################################################################
#																			 		   #
# Create new local directory if it does not already exist                              #
#                                                                                      #
# Arguments: directory* - path of the directory to be created                          #
#   		 fail_on_error? - Flag to decide, in case of failure of this operation,    #
#							wheather to exit the process with error code or            #  
#							just write error message and return.                       #
#																			 		   # 
########################################################################################

function fn_local_create_directory_if_not_exists(){
  directory=$1
  fail_on_error=$2
  log_file=$3

  assert_variable_is_set "directory" "${directory}"

  if [ ! -d "${directory}" ]
  then
    mkdir -p ${directory}
    exit_code=$?

    success_message="Created local directory ${directory}"
    failure_message="Failed to create local directory ${directory}"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
    return $exit_code
  else
    fn_log_info "Local directory ${directory} already exists" "${log_file}"
  fi
}


########################################################################################
#																			 		   #
# Delete hadoop directory if it already exists                                         #
#                                                                                      #
# Arguments: directory* - path of the directory to be created                          #
#            fail_on_error? - Flag to decide, in case of failure of this operation,    #
#								wheather to exit the process with error code or        # 
#								just write error message and return.                   #
#																			 		   # 
########################################################################################

function fn_hadoop_delete_directory_if_exists(){
  directory=$1
  fail_on_error=$2
  log_file=$3
 
  assert_variable_is_set "directory" "${directory}"

  hdfs dfs -test -e "${directory}"

  exit_code=$?

  if [ ${exit_code} == $EXIT_CODE_SUCCESS ]
  then
    hdfs dfs -rm -r "${directory}"
    exit_code=$?

    success_message="Deleted hadoop directory ${directory}"
    failure_message="Failed to delete hadoop directory ${directory}"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
    return ${exit_code} 
  else
    fn_log_info "Hadoop directory ${directory} does not exist" "${log_file}"
  fi
}


########################################################################################
#																			 		   #
# Create hadoop directory if it does not already exist                                 #  
#                                                                                      #
# Arguments: directory* - path of the directory to be created                          #
#            fail_on_error? - Flag to decide, in case of failure of this operation,    # 
#							wheather to exit the process with error code or            #  
#							just write error message and return.                       # 
#																			 		   # 
######################################################################################## 
 
function fn_hadoop_create_directory_if_not_exists(){
  directory=$1
  fail_on_error=$2
  log_file=$3

#  assert_variable_is_set "directory" "${directory}"

  hdfs dfs -test -e "${directory}"
  exit_code=$?

  if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
  then
    hdfs dfs -mkdir -p "${directory}"
    exit_code=$?
    
    success_message="Created hadoop directory ${directory}"
    failure_message="Failed to create hadoop directory ${directory}"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
    return $exit_code
  else
    fn_log_info "Hadoop directory ${directory} already exists" "${log_file}"
  fi
}

########################################################################################
#																			 		   #
# Moves a hadoop file or folder to another hadoop folder                               #
#                                                                                      #
# Arguments: file_or_directory* - path of the file or directory to be moved            # 
#   	     to_directory* - path of the target directory                              #
#            skip_tests? - skip all the path checks. This is useful if this            #
#							function is called in a loop.                              #
#            fail_on_error? - flag to decide, in case of failure of this operation,    # 
#							wheather to exit the process with error code or            #  
#							just write error message and return.                       #
#																			 		   # 
######################################################################################## 
 
function fn_hadoop_move_file_or_directory(){
  file_or_directory=$1
  to_directory=$2
  skip_tests=$3
  fail_on_error=$4
  log_file=$5

  if [ "${skip_tests}" != "${BOOLEAN_TRUE}" ]
  then
    assert_variable_is_set "file_or_directory" "${file_or_directory}"

    assert_variable_is_set "to_directory" "${to_directory}"

    hdfs dfs -test -e "${file_or_directory}"
    exit_code=$?

    success_message="Source hadoop file/directory ${file_or_directory} exists"
    failure_message="Source hadoop file/directory does not exist"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"

    fn_hadoop_delete_file "${to_directory}/*" "${fail_on_error}" "${log_file}"

  fi
  
  fn_hadoop_create_directory_if_not_exists "${to_directory}" "${fail_on_error}" "${log_file}"
  
  hadoop fs -mv "${file_or_directory}" "${to_directory}"
  exit_code=$?
  
  success_message="Successfully moved file or directory ${file_or_directory} to directory ${to_directory}"
  failure_message="Failed to move file or directory ${file_or_directory} to directory ${to_directory}"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
  return $exit_code
}


########################################################################################
#																			 		   #
# Moves a hadoop file or folder to another hadoop folder                               #
#                                                                                      #
# Arguments: file_or_directory* - path of the file or directory to be moved            #
#   	     to_directory* - path of the target directory                              #
#            skip_tests? - skip all the path checks. This is useful if this function   #
#							is called in a loop.                                       #
#            fail_on_error? - flag to decide, in case of failure of this operation,    #
#							wheather to exit the process with error code or just       #
#							write error message and return.                            #
#																			 		   # 
########################################################################################  

function fn_hadoop_copy_file_or_directory(){
  file_or_directory=$1
  to_directory=$2
  skip_tests=$3
  fail_on_error=$4
  log_file=$5

  if [ "${skip_tests}" != "${BOOLEAN_TRUE}" ]
  then
    assert_variable_is_set "file_or_directory" "${file_or_directory}"

    assert_variable_is_set "to_directory" "${to_directory}"

    hdfs dfs -test -e "${file_or_directory}"
    exit_code=$?

    success_message="Source hadoop file/directory ${file_or_directory} exists"
    failure_message="Source hadoop file/directory does not exist"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"

    fn_hadoop_delete_file "${to_directory}/*" "${fail_on_error}" "${log_file}"

    fn_hadoop_create_directory_if_not_exists "${to_directory}" "${fail_on_error}" "${log_file}"
  fi
  hadoop fs -cp "${file_or_directory}" "${to_directory}"
  exit_code=$?

  success_message="Successfully moved file or directory ${file_or_directory} to directory ${to_directory}"
  failure_message="Failed to move file or directory ${file_or_directory} to directory ${to_directory}"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
  return $exit_code
}


########################################################################################
#																			 		   #
# Generate new batch id.                                                               #
# This function implementation is based on generating timestamp as batch id.           #  
# But it may be used to simple test purposes. In case of enterprise scenarios,         # 
# enterprise workflow scheduler should be used which has capability to run             # 
# multiple batches in parallel. This implementation stores the newly generated         #
# batch id into a file named as current into the directory configured using            #
# BATCH_ID_DIR environment variable. For this first time this file wont be             #
# present hence it will be create. Every next time the contents of the current         #
# file are moved into a file named after the batch id store into current file.         #
#																			 		   # 
######################################################################################## 
 
function fn_generate_batch_id(){
  log_file=$1	

  fn_log_info "Generating new batch id" "${log_file}"

  fail_on_error=$BOOLEAN_TRUE

  assert_variable_is_set "BATCH_ID_DIR" "${BATCH_ID_DIR}"

  fn_local_create_directory_if_not_exists "${BATCH_ID_DIR}" "${fail_on_error}" "${log_file}"

  new_batch_id=`date +"%Y%d%m%H%M%S"`

  current_batch_id_file="${BATCH_ID_DIR}/current"

  if [ ! -f "${current_batch_id_file}" ]
  then
    fn_log_info "Current batch id file ${current_batch_id_file} does not exists." "${log_file}"
  else
    current_batch_id=`cat ${current_batch_id_file}`

    mv "${current_batch_id_file}" "${BATCH_ID_DIR}/${current_batch_id}"

    exit_code=$?

    dont_fail_on_error=0

    success_message="Moved current batch id file to ${BATCH_ID_DIR}/${current_batch_id}"

    failure_messages="Failed to move current file to ${BATCH_ID_DIR}/${current_batch_id} file"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_messages}" "${dont_fail_on_error}" "${log_file}"       
  fi 
  touch "${current_batch_id_file}"

  echo "${new_batch_id}" > "${current_batch_id_file}"
  exit_code=$?

  fail_on_error=1
  success_message="Generated new batch id is ${new_batch_id} and wrote it to ${current_batch_id_file} file "
  failure_messages="Failed to write new batch id ${new_batch_id} to ${current_batch_id_file} file"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_messages}" "${fail_on_error}" "${log_file}"
}


########################################################################################
#																			 		   #
# Set the global BATCH_ID with current batch id if it not already set                  #
#																			 		   # 
######################################################################################## 

function fn_get_current_batch_id(){

  if [ -z "${BATCH_ID}" ];
  then
    current_batch_id_file="${BATCH_ID_DIR_SUBJECT_AREA}/current"

    BATCH_ID=`cat ${current_batch_id_file}`
  fi
  assert_variable_is_set "BATCH_ID" "${BATCH_ID}"
}

########################################################################################
#																			 		   #
# Create the log directory for given module if already not ceated with                 #
# current batch id                                                                     #
#																			 		   # 
########################################################################################

function fn_get_module_log_dir(){

  fn_get_current_batch_id

  if [ ! -d "${LOG_DIR}/${SUBJECT_AREA_NAME}/${BATCH_ID}" ]
  then
    mkdir -p "${LOG_DIR}/${SUBJECT_AREA_NAME}/${BATCH_ID}"
	exit_code=$?
  fi

  return ${exit_code}
}

########################################################################################
#																			 		   #
# Set the global LOG_FILE_NAME_MODULE variable if it is not already set with           #
# current batch id                                                                     #
#																			 		   # 
######################################################################################## 

function fn_get_module_log_dir_and_file(){
  log_file=$1

  if [ "${LOG_DIR_MODULE_SET}" != "true" ]
  then
    fn_get_current_batch_id

    assert_variable_is_set "BATCH_ID" "${BATCH_ID}"

    assert_variable_is_set "LOG_DIR_MODULE" "${LOG_DIR_MODULE}"

    TEMP_LOG_DIR_MODULE="${LOG_DIR_MODULE}/batch-${BATCH_ID}"

    if [ ! -d "${TEMP_LOG_DIR_MODULE}" ]
    then
      mkdir -p ${TEMP_LOG_DIR_MODULE}
      exit_code=$?
    
      fail_on_error=${BOOLEAN_TRUE}
      success_message="Created log directory ${TEMP_LOG_DIR_MODULE}"
      failure_message="Failed to create log directory ${LOG_DIR_MODULE}"
      fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
    fi
    
    log_file_name=`date +"%Y%d%m%H%M%S"`
    
    LOG_FILE_NAME_MODULE="${log_file_name}.log"

    LOG_DIR_MODULE="${TEMP_LOG_DIR_MODULE}"

    LOG_DIR_MODULE_SET="true"
  fi
}


########################################################################################
#																			 		   #
# Creates directory structure for the hive table                                       #
#                                                                                      #
# Arguments: data_layer_dir* - data layer where this table has to be created           #
#            hive_table_name* - name of the hive table to be used to create folder     #
#            create_landing_partition? - wheater to create landing partition or not    # 
#                               landing partition is only applicable for incoming or   # 
#                               security layers as input marks needs to be supported.  #
#																			 		   # 
########################################################################################  

function fn_module_create_hive_table_data_layer_directory_structure(){
  data_layer_dir="$1"
  hive_table_name="$2"
  create_landing_partition="$3"
  log_file="$4"

  assert_variable_is_set "data_layer_dir" "${data_layer_dir}"

  assert_variable_is_set "hive_table_name" "${hive_table_name}"

  fn_log_info "Creating module table ${hive_table_name} in data layer ${data_layer_dir}" "${log_file}"
  fail_on_error="${BOOLEAN_TRUE}"

  hive_table_data_layer_dir="${data_layer_dir}/${hive_table_name}"

  fn_hadoop_create_directory_if_not_exists "${hive_table_data_layer_dir}" "${fail_on_error}" "${log_file}"

  fn_log_info "Successfully created module table ${hive_table_name} directory in data layer ${data_layer_dir}" "${log_file}"

  if [ "${create_landing_partition}" == "${BOOLEAN_TRUE}" ];
  then
    assert_variable_is_set "LANDING_PARTITION_BATCH_ID" "${LANDING_PARTITION_BATCH_ID}" 

    hive_table_data_layer_landing_partition_dir="${hive_table_data_layer_dir}/batch_id=${LANDING_PARTITION_BATCH_ID}"

    fn_hadoop_create_directory_if_not_exists "${hive_table_data_layer_landing_partition_dir}" "${fail_on_error}"

    fn_log_info "Successfully created module table ${hive_table_name} directory in data layer ${data_layer_dir} with landing partition id" "${log_file}"
  fi
}


########################################################################################
#																			 		   #
# Mark current batch input                                                             # 
#                                                                                      #
# Arguments: module_landing_partition_dir - path to the landing partition directory    #
#            module_marked_batch_id_dir - path to the current batch id directory       #
#																			 		   # 
######################################################################################## 
 
function fn_mark_batch_input(){
	module_landing_partition_dir=$1
	module_marked_batch_id_dir=$2
    log_file=$3

    assert_variable_is_set "module_landing_partition_dir" "${module_landing_partition_dir}"

    assert_variable_is_set "module_marked_batch_id_dir" "${module_marked_batch_id_dir}"  

    fail_on_error="${BOOLEAN_TRUE}"

    fn_hadoop_create_directory_if_not_exists "${module_marked_batch_id_dir}" "${fail_on_error}"

	module_marked_batch_id_dir_complete_file="${module_marked_batch_id_dir}/_COMPLETE"

	hadoop fs -test -e "${module_marked_batch_id_dir_complete_file}"
	exit_code=$?

	if [ ${exit_code} != $EXIT_CODE_SUCCESS ]
	then
		#if input path ends with forward slash
		if [[ "${module_landing_partition_dir}" == */ ]]
		then
			calculated_hidden_file_or_folder_prefix="${module_landing_partition_dir}${HIDDEN_FILE_OR_DIRECTORY_PREFIX}"
		else
			#add forward slash
			calculated_hidden_file_or_folder_prefix="${module_landing_partition_dir}/${HIDDEN_FILE_OR_DIRECTORY_PREFIX}"
		fi

    #find all the files from the landing partition directory
		list_of_all_files=$(hadoop fs -ls ${module_landing_partition_dir} | awk '{print $8}')

		skip_tests="${BOOLEAN_FALSE}"

    atleast_one_file_if_moved="${BOOLEAN_FALSE}"

		for each_file in ${list_of_all_files}
		do
		 file_name=${each_file}
		 
		 if [ "${file_name}" != "" ]
		 then
           #If the file does not start with underscore character which is used for hidden files
		   if [ "${file_name:0:${#calculated_hidden_file_or_folder_prefix}}" != "${calculated_hidden_file_or_folder_prefix}"  ]
		   then
		     fail_on_error="${BOOLEAN_TRUE}"

		     fn_hadoop_move_file_or_directory  "${file_name}" "${module_marked_batch_id_dir}" "${skip_tests}" "${fail_on_error}"

		     skip_tests="${BOOLEAN_TRUE}"

         atleast_one_file_if_moved="${BOOLEAN_TRUE}"
		     
		   fi
		 fi
		done

    if "${atleast_one_file_if_moved}" == "${BOOLEAN_TRUE}"
    then
      #all the files are successfullt moved from landing partition to current batch id partition. 
      #mark the success by creating _COMPLETE file
  		hadoop fs -touchz "${module_marked_batch_id_dir_complete_file}"
  		exit_code=$?
  	  
       fail_on_error=$BOOLEAN_TRUE
       success_message="Successfully created ${module_marked_batch_id_dir_complete_file} file"
       failure_message="Failed to create ${module_marked_batch_id_dir_complete_file} file"
       fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
    fi

	else
		fn_log_info "Input for this batch is already marked at directory ${module_marked_batch_id_dir}. Skipping marking again." "${log_file}"
	fi 
}


########################################################################################
#																			 		   #
# Generate hive initialization script                                                  #
#																			 		   # 
########################################################################################
  
function fn_generate_hive_initialization_file(){

  fail_on_error="${BOOLEAN_TRUE}"

  fn_local_delete_file "${HIVE_INITIALISATION_FILE}" "${fail_on_error}" "${log_file}"

  for line in `grep -v '^#' ${HOME}/etc/namespace.properties`
  do
    echo "set ${line};" >> "${HIVE_INITIALISATION_FILE}"
  done

  if [ -f "${USER_NAMESPACE_PROPERTIES_FILE}" ];                                                             
  then
    for line in `grep -v '^#' ${USER_NAMESPACE_PROPERTIES_FILE}`
    do
      echo "set ${line};" >> "${HIVE_INITIALISATION_FILE}"
    done
  fi

  echo "" >> "${HIVE_INITIALISATION_FILE}"

  cat "${HOME}/etc/default.hive.properties" >> "${HIVE_INITIALISATION_FILE}"

  echo "" >> "${HIVE_INITIALISATION_FILE}"

  cat "${SUBJECT_AREA_HOME}/etc/subject-area.hive.properties" >> "${HIVE_INITIALISATION_FILE}"

  echo "" >> "${HIVE_INITIALISATION_FILE}"

  cat "${PROJECT_HOME}/etc/project.hive.properties" >> "${HIVE_INITIALISATION_FILE}"

  echo "" >> "${HIVE_INITIALISATION_FILE}"

  cat "${MODULE_HOME}/etc/module.hive.properties" >> "${HIVE_INITIALISATION_FILE}"

  echo "" >> "${HIVE_INITIALISATION_FILE}"
}


########################################################################################
#																			 		   #
# Creates hive database                                                                #
#                                                                                      # 
# Arguments: hive_database_name - name of the hive database                            #
#            hive_database_location - location to be used for the database             #
#																			 		   # 
######################################################################################## 

function fn_create_hive_database(){
    hive_database_name="$1"
    hive_database_location="$2"
    hiveserver2_url=$3
    hive_username=$4
    hive_password=$5
    log_file_path=$6

    assert_variable_is_set "hive_database_name" "${hive_database_name}"

    assert_variable_is_set "hive_database_location" "${hive_database_location}"

    #hive_database_name_with_prefix="${hive_database_name}${HIVE_DATABASE_SUFFIX}"
        
    beeline -u ${hiveserver2_url} -n ${hive_username} -p ${hive_password} --showHeader=false --silent=true -e \
    "CREATE DATABASE IF NOT EXISTS ${hive_database_name} LOCATION '${hive_database_location}';" 1>> "${log_file_path}" 2>> "${log_file_path}"
    exit_code=$?

    fail_on_error="$BOOLEAN_TRUE"
    success_message="Successfully created hive database ${hive_database_name} with location ${hive_database_location}"
    failure_message="Failed to create hive database ${hive_database_name} with location ${hive_database_location}"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file_path}"
}

########################################################################################
#																			 		   #
# Execute hive script                                                                  #
# Arguments: pass any arguments that you want to pass to hive command                  #
#																			 		   # 
######################################################################################## 

function fn_create_hive_table(){

  hive_database="$1"
  data_layer_dir="$2"
  hive_table_name="$3"
  hive_ddl_file="$4"
  log_file_path="$5"
  hiveserver2_url=$6
  hive_username=$7
  hive_password=$8
  partition_column="$9"
  
  beeline -u $hiveserver2_url -n ${hive_username} -p ${hive_password} -hivevar database="${hive_database}" \
       -hivevar table_prefix="${HIVE_TABLE_PREFIX}" \
       -hivevar location="${data_layer_dir}" \
       -hivevar table_name="${hive_table_name}" \
       -hivevar ${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
       -hivevar ${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
       -hivevar ${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
       -hivevar partition_column="$partition_column" \
      --hivevar hive.querylog.location="${log_file_path}" \
      -f "${hive_ddl_file}" $@ 1>> "${log_file_path}" 2>> "${log_file_path}" 
  exit_code=$?

  fail_on_error=$BOOLEAN_TRUE
  success_message="Successfully created hive table ${hive_table_name}"
  failure_message="Failed to create hive table ${hive_table_name} hive script"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file_path}"
}

########################################################################################
#																			 		   #
# Execute hive script                                                                  #
# Arguments: pass any arguments that you want to pass to hive command                  #
#																			 		   # 
######################################################################################## 

function fn_add_batch_id_partition_to_hive_table(){
  hive_database="$1"
  hive_table="$2"
  partition_batch_id="$3"
  log_file=$4

  assert_variable_is_set "hive_database" "${hive_database}"

  assert_variable_is_set "hive_table" "${hive_table}"

  assert_variable_is_set "partition_batch_id" "${partition_batch_id}"

  fn_get_current_batch_id

  fn_get_module_log_dir_and_file

  module_type="hive"

  module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"

  mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

  module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"
  
  HIVE_INITIALISATION_FILE="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.rc"

  fn_generate_hive_initialization_file

  fn_log_info "Log file : ${module_log_file}" "${log_file}"

  fn_log_info "Out file : ${mobule_out_file}" "${log_file}"
  
  fn_log_info "Err file : ${module_err_file}" "${log_file}"

  hive_database_name_with_suffix="${hive_database}${HIVE_DATABASE_SUFFIX}"

  hive -d batch.id="${partition_batch_id}" \
       -hiveconf ${UNIQUE_PROPERTY_PREFIX}.batch.id="${partition_batch_id}" \
       -hiveconf ${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
       -hiveconf ${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
       -hiveconf ${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
      --hiveconf hive.querylog.location="${LOG_DIR_MODULE}/${module_log_file}" \
      -i "${HIVE_INITIALISATION_FILE}" \
      -e  "use ${hive_database_name_with_suffix}; ALTER TABLE ${HIVE_TABLE_PREFIX}${hive_table} ADD PARTITION (batch_id = '${partition_batch_id}');" $@ 1>> "${mobule_out_file}" 2>> "${module_err_file}" 

  exit_code=$?

  fail_on_error=$BOOLEAN_TRUE
  success_message="Successfully added parition ${partition_batch_id} to hive table ${hive_table}"
  failure_message="Failed to add parition ${partition_batch_id} to hive table ${hive_table}"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
}


########################################################################################
#																			 		   #
# Execute pig script																   #
#                                                                                      #
# Arguments: pass any arguments that you want to pass to pig command                   #
#																			 		   # 
######################################################################################## 

function fn_execute_mapreduce(){
  jar_path="$1"
  log_file=$2

  assert_variable_is_set "jar_path" "${jar_path}"
  
  fn_get_current_batch_id
  
  fn_get_module_log_dir_and_file
  
  module_type="mapreduce"
  
  module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"
  
  mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"
  
  module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

  fn_log_info "Log file : ${module_log_file}" "${log_file}"

  fn_log_info "Out file : ${mobule_out_file}" "${log_file}"
  
  fn_log_info "Err file : ${module_err_file}" "${log_file}"

  hadoop jar \
    -D${UNIQUE_PROPERTY_PREFIX}.batch.id="${BATCH_ID}" \
    -D${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
    -D${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
    -D${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
    -Djob.name="${MODULE_NAME}" \
  	"$@" 1>> "${mobule_out_file}" 2>> "${module_err_file}" 
  exit_code=$?

  fail_on_error=$BOOLEAN_TRUE
  success_message="Successfully executed mapreduce job ${jar_path}"
  failure_message="Mapreduce job ${jar_path} failed"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
}


########################################################################################
#																			 		   #
# Execute pig script with tez.                                                         #
# Argument :																		   #
#  	1. source database                                                                 #
#  	2. source table name                                                               #
#  	3. target database                                                                 #
#  	4. target table                                                                    #
#  	5. suite id                                                                        #
#  	6. source date                                                                     #
#  	7. suite name                                                                      #
#  	8. module name                                                                     #
#  	9. current maximum id                                                              #
#  	10. path of lookup                                                                 #
#  	11. log file path                                                                  #
#																			 		   # 
######################################################################################## 

function fn_execute_pig(){  
  source_database=$1
  source_table=$2
  target_database=$3
  target_table=$4
  suite_id=$5
  source_date=$6
  suite_name=$7
  module_name="${target_table}_tbl"
  log_file=$8  
  
  pig_file_path="${SUBJECT_AREA_HOME}/${module_name}/pig/module-transform.pig"
    
  current_batch_id_path="${BATCH_ID_DIR}/${suite_name}/current"

  batch_id=`cat $current_batch_id_path`
 
   pig -x tez -p source_database="${source_database}" \
      -p source_table="${HIVE_TABLE_PREFIX}${source_table}" \
      -p target_database="${target_database}" \
      -p target_table="${HIVE_TABLE_PREFIX}${target_table}" \
      -p suite_id="${suite_id}" \
      -p suite_name="${suite_name}" \
      -p source_date="${source_date}" \
      -useHCatalog \
      -logfile "${log_file}" \
      "$pig_file_path" 1>> "${log_file}" 2>> "${log_file}"
  exit_code=$?

  success_message="Successfully executed ${module_name} pig script "
  failure_message="Pig script ${module_name} failed"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_FALSE}" "${log_file}"
  return $exit_code
}


########################################################################################
#																			 		   #
# Execute pig script without tez.                                                      #
# Argument :																		   #
#  	1. source database                                                                 #
#  	2. source table name                                                               #
#  	3. target database                                                                 #
#  	4. target table                                                                    #
#  	5. suite id                                                                        #
#  	6. source date                                                                     #
#  	7. suite name                                                                      #
#  	8. module name                                                                     #
#  	9. current maximum id                                                              #
#  	10. log file path                                                                  #
#																			 		   # 
######################################################################################## 

function fn_execute_pig_without_tez(){  

  source_database=$1
  source_table=$2
  target_database=$3
  target_table=$4
  suite_id=$5
  source_date=$6
  suite_name=$7
  module_name="${target_table}_tbl"
  max_id=$8
  log_file=$9  
  
  pig_file_path="${SUBJECT_AREA_HOME}/${module_name}/pig/module-transform.pig"
    
  current_batch_id_path="${BATCH_ID_DIR}/${suite_name}/current"

  batch_id=`cat $current_batch_id_path`

  module_type="PIG"
  module_log_filename="${module_type}_$(echo "$target_table" | tr '[:lower:]' '[:upper:]').log"
  module_log_file="${LOG_DIR_SUBJECT_AREA}/${batch_id}/${module_log_filename}"
  
  fn_log_info "Log file : ${module_log_file}" "${log_file}"

   pig -p source_database="${source_database}" \
      -p source_table="${HIVE_TABLE_PREFIX}${source_table}" \
      -p target_database="${target_database}" \
      -p target_table="${HIVE_TABLE_PREFIX}${target_table}" \
      -p suite_id="${suite_id}" \
      -p suite_name="${suite_name}" \
      -p source_date="${source_date}" \
      -useHCatalog \
      -logfile "${module_log_file}" \
      "$pig_file_path" -m "${SUBJECT_AREA_HOME}/etc/subject-area.env.properties" -m "${SUBJECT_AREA_HOME}/etc/omniture.properties" 2&1>> "${module_log_file}"
  exit_code=$?

  fail_on_error=$BOOLEAN_FALSE
  success_message="Successfully executed ${MODULE_NAME} pig script "
  failure_message="Pig script ${MODULE_NAME} failed"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
  return $exit_code
}


########################################################################################
#																			 		   #
# Function to import data from external table.                                         #
# Argument :																		   #
#  	1. source table name                                                               #
#  	2. target table name                                                               #
#  	3. log file path                                                                   #
#																			 		   # 
########################################################################################

function fn_execute_sqoop_export_to_teradata(){
  source_table_name=$1
  target_table_name=$2
  log_file=$3
  num_mappers=10

  sqoop export \
    -libjars $LIB_JARS \
    -Dteradata.db.output.source.table="${source_table_name}" \ 
    -Dteradata.db.output.target.table="${target_table_name}" \ 
    -Dteradata.db.output.job.type="hcat" \ 
    -Dteradata.db.output.file.format="rcfile" \ 
    -Dteradata.db.output.method= \ 
    -Dteradata.db.output.num.mappers="${num_mappers}" \ 
    -Dteradata.db.output.stage.database= \ 
    -Dteradata.db.output.stage.table.name="${target_table_name}" \ 
    -Dteradata.db.output.batch.size= \ 
    -Dteradata.db.input.job.type=hcat \
    -Dteradata.db.input.target.table=hcat_table \
    --connect jdbc:teradata://td-host/Database=dbname 
    --connection-manager org.apache.sqoop.teradata.TeradataConnManager \
    --username dbc \
    --password dbc \
    --table employees2 \
    --export-dir /user/hrt_qa/test-sqoop/out \
    --batch
  exit_code=$?

  fail_on_error=$BOOLEAN_TRUE
  success_message="Successfully export table from hive to teradata database"
  failure_message="Failed to import table from teradata database to hive table ${hive_table}"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
}

function fn_execute_sqoop_export_to_oracle(){
   
   connection_url=$1
   target_table=$2
   dir_path=$3
   log_file_path=$4
   fail_on_error=${BOOLEAN_FALSE}
 
   sqoop export --connect "${connection_url}" \
      --username "${USERNAME}" \
      --password "${PASSWORD}" \
      --table ${target_table} \
      --fields-terminated-by '~' \
      --export-dir "${dir_path}" \
      --input-null-string "\\\\N" \
      --input-null-non-string "\\\\N" \
      -m "${NUMBER_OF_MAPPERS}" >> ${log_file_path} 2>&1
   exit_code="$?" 
   return ${exit_code}    
}

function fn_execute_sqoop_export_to_netezza(){

   connection_url=$1
   target_table=$2
   dir_path=$3
   netezza_driver=$4
    property_file_path=$5
   number_of_mappers=$6
   delimiter=$7
   log_file_path=$8
  
   fail_on_error=${BOOLEAN_FALSE}

   sqoop export -D sqoop.export.records.per.statement=10000 \
      --connect "${connection_url}" \
      --username "${USERNAME}" \
      --password "${PASSWORD}" \
      --table ${target_table} \
      --fields-terminated-by ${delimiter} \
      --driver "${netezza_driver}" \
      --export-dir "${dir_path}" \
      --input-null-string "\\\\N" \
      --input-null-non-string "\\\\N" \
      --connection-param-file "$property_file_path" \
      --batch \
     --direct \
      -m "${number_of_mappers}" >> ${log_file_path} 2>&1
   exit_code="$?"
   return ${exit_code}
}




########################################################################################
#																			 		   #
# Function to export data to external table.                                           #
# Argument :																		   #
#  	1. target table                                                                    #
#  	2. log file path                                                                   #
#																			 		   # 
########################################################################################
																		 		   
function fn_execute_sqoop_export(){
  target="$1"
  log_file=$2

  case "${target}" in

    teradata)

      fn_execute_sqoop_export_to_teradata "$@"

    ;;

    oracle)
 
      fn_execute_sqoop_export_to_oracle "${@:2}"

    ;;
    
    netezza)
 
      fn_execute_sqoop_export_to_netezza "${@:2}"

    ;;

    db2)

      exit_code=1

      fail_on_error=${BOOLEAN_TRUE}
      success_message=""
      failure_message="Sqoop export to db2 is not supported yet"
      fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"

    ;;

    *)

      echo $"Usage: $0 {teradata|oracle|netezza|db2}"

      exit 1
   
  esac
}

##
# Perform setup tasks for this module
#
function fn_module_setup(){
  log_file=$1
  fn_log_warn "Operation not implemented" "${log_file}"
}

##
# Mark the input for this job   
#
function fn_module_mark(){
  log_file=$1
  fn_log_warn "Operation not implemented" "${log_file}"
}

##
# Execute preparation script for this module
#
function fn_module_prepare(){
  log_file=$1
  fn_log_warn "Operation not implemented" "${log_file}"
}


##
# Execute business logic
#
function fn_module_transform(){
  log_file=$1
  fn_log_warn "Operation not implemented" "${log_file}"
}


##
# Validate the data processed by module
#    
function fn_module_validate(){
  log_file=$1
  fn_log_warn "Operation not implemented" "${log_file}"
}



##
# Validate the data processed by module
#
function fn_module_export(){
  log_file=$1	
  fn_log_warn "Operation not implemented" "${log_file}"
}



##
# Perform post module scripts execution tasks
#
function fn_module_cleanup(){
  log_file=$1 	
  fn_log_warn "Operation not implemented" "${log_file}"
}


########################################################################################
#																			 		   #
# Entry point for this module.                                                         #
# It executes operation passed on the command line as the first argument while         #
#  executing the script.                                                               #
#																			 		   # 
########################################################################################

function fn_main(){
  command="$1"

  case "${command}" in

    setup)

      fn_module_setup "${@:2}"

    ;;

    mark)

      fn_module_mark "${@:2}"    

    ;;
   
    import)

      fn_module_import "${@:2}"    

    ;;
    
    prepare)

      fn_module_prepare "${@:2}"

    ;;
   
    transform)

      fn_module_transform "${@:2}"

    ;;

    validate)

      fn_module_validate "${@:2}"

    ;;

    export)

      fn_module_export "${@:2}"

    ;;             

    cleanup)

      fn_module_cleanup "${@:2}"

    ;;
    
    delete)

      fn_module_delete "${@:2}"

    ;;

    *)

      echo $"Usage: $0 {setup|mark|prepare|transform|validate|export|cleanup}"

      exit 1
   
  esac
}


########################################################################################
#																			 		   #
# Create local directory if it does not already exist								   # 
#                                                                                      #
# Arguments: directory* - path of the directory to be created                          #
#            fail_on_error? - Flag to decide, in case of failure of this operation,    #
#				          wheather to exit the process with error code or              #
#				          just write error message and return.                         #
#																			 		   # 
########################################################################################

function fn_local_create_directory_if_not_exist(){
  directory=$1
  fail_on_error=$2
  log_file=$3

  assert_variable_is_set "directory" "${directory}"

  test -e "${directory}"

  exit_code=$?

  if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
  then
    mkdir -p "${directory}"
    exit_code=$?

    success_message="Created local directory ${directory}"
    failure_message="Failed to create local directory ${directory}"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"   
  else
    fn_log_info "Local directory ${directory} already exists" "${log_file}"
  fi
}


########################################################################################
#																			 		   #
# Moves a file from or folder from local to hadoop folder							   #
#                                                                                      #
# Arguments: file_or_directory* - path of the file or directory to be moved            #
#            to_directory* - path of the target directory                              #
#            skip_tests? - skip all the path checks. This is useful if this function   #
#							is called in a loop.                                       #
#            fail_on_error? - flag to decide, in case of failure of this operation,    #
#							  wheather to exit the process with error code or just     #
#							  write error message and return.                          #
#																			 		   # 
########################################################################################

function fn_hdp_put(){
  from_directory=$1
  to_directory=$2
  file_name=$3
  fail_on_error=$4
  log_file=$5    
  
  cd $from_directory
  
  hdfs dfs -put $file_name $to_directory
  exit_code=$?
  
  success_message="Successfully moved file or directory ${file_name} to directory ${to_directory}"
  failure_message="Failed to move file or directory ${file_name} to directory ${to_directory}"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"  
}


########################################################################################
#																			 		   #
# Copy a file from or folder from local to hadoop folder                              #
#																					   #
# Arguments: file_or_directory* - path of the file or directory to be moved            #
#            to_directory* - path of the target directory                              #
#            skip_tests? - skip all the path checks. This is useful if this            # 
#							function is called in a loop.                              #
#            fail_on_error? - flag to decide, in case of failure of this operation,    #
#							  wheather to exit the process with error code or          #
#							  just write error message and return.                     # 
#																			 		   #
########################################################################################

function fn_copy_from_local(){
  file_or_directory=$1
  to_directory=$2
  skip_tests=$3
  fail_on_error=$4
  log_file=$5

  filename=`basename ${file_or_directory}`
  
  
  if [ "${skip_tests}" != "${BOOLEAN_TRUE}" ]
  then
    test -e "${file_or_directory}"
    exit_code=$?
   
    success_message="Source local file/directory ${file_or_directory} exists"
    failure_message="Source local file/directory ${file_or_directory} does not exist"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
    
    fn_hadoop_create_directory_if_not_exists "${to_directory}" "${fail_on_error}" "${log_file}"
  fi

  hadoop fs -test -e "${to_directory}/${filename}"
  exit_code=$?

  if [ "${exit_code}" == "${EXIT_CODE_SUCCESS}" ]
  then
     message="File/directory ${to_directory}/${filename} already exist"     
     
    fn_log_info "${message}" "${log_file}"
    
    hadoop fs -rm -r "${to_directory}/*"
    fn_log_info "Directory deleted ${to_directory}/*" "${log_file}"
    
  fi
    hdfs dfs -copyFromLocal  "${file_or_directory}" "${to_directory}"
    exit_code=$?
  
    success_message="Successfully copy file or directory ${file_or_directory} to directory ${to_directory}"
    failure_message="Failed to copy file or directory ${file_or_directory} to directory ${to_directory}"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}" 
    
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    then
  	     return $exit_code
    fi 
    
    hdfs dfs -chmod 777 ${to_directory}/${filename}
    exit_code=$?
  
    success_message="Successfully changed file or directory permissions of ${to_directory}/${filename}"
    failure_message="Failed to change file or directory permission for ${to_directory}/${filename}"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
    
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    then
  	     return $exit_code
    fi

}
########################################################################################
#																			 		   #
# Copy a file from hadoop folder to hadoop folder		                               #
#																					   #
# Arguments: file_or_directory* - path of the file or directory to be moved            #
#            to_directory* - path of the target directory                              #
#            skip_tests? - skip all the path checks. This is useful if this            # 
#							function is called in a loop.                              #
#            fail_on_error? - flag to decide, in case of failure of this operation,    #
#							  wheather to exit the process with error code or          #
#							  just write error message and return.                     # 
#																			 		   #
########################################################################################

function fn_copy_from_hdfs(){
  from_directory=$1
  to_directory=$2
  skip_tests=$3
  fail_on_error=$4
  log_file=$5

  filename=`basename ${from_directory}`
  
  
  if [ "${skip_tests}" != "${BOOLEAN_TRUE}" ]
  then
    hadoop fs -test -e "${from_directory}"
    exit_code=$?
   
    success_message="Source HDFS file/directory ${from_directory} exists"
    failure_message="Source HDFS file/directory does not exist"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
    
    fn_hadoop_create_directory_if_not_exists "${to_directory}" "${fail_on_error}" "${log_file}"
  fi

  #echo "${to_directory}/${filename}"
  hadoop fs -test -e "${to_directory}/${filename}"
  exit_code=$?
  #echo "exit_code :: $exit_code"
  if [ "${exit_code}" == "${EXIT_CODE_SUCCESS}" ]
  then
     message="File/directory ${to_directory}/${filename} already exist"		
    fn_log_info "${message}" "${log_file}"
  else
    hdfs dfs -cp  "${from_directory}" "${to_directory}"
    exit_code=$?
  
    success_message="Successfully copied file or directory ${from_directory} to directory ${to_directory}"
    failure_message="Failed to copy file or directory ${from_directory} to directory ${to_directory}"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}" 
  
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    then
  	     return $exit_code
    fi  
    
    hdfs dfs -chmod 777 ${to_directory}/${filename}
    exit_code=$?
  
    success_message="Successfully changed file or directory permissions of ${to_directory}/${filename}"
    failure_message="Failed to change file or directory permission for ${to_directory}/${filename}"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
  
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    then
  	     return $exit_code
    fi
  fi
}
########################################################################################
#																			 		   #
# Moves a hadoop file or folder to local folder										   #
#																					   #	
# Arguments: file_or_directory* - path of the file or directory to be moved            #
#            to_directory* - path of the target directory                              #
#            skip_tests? - skip all the path checks. This is useful if this            #  
#						   function is called in a loop.                               # 
#            fail_on_error? - flag to decide, in case of failure of this operation,    # 
#                            wheather to exit the process with error code or just      #
#                            write error message and return.                           # 
#																			 		   #
########################################################################################

function fn_copy_to_local(){
  file_or_directory=$1
  to_directory=$2
  skip_tests=$3
  fail_on_error=$4
  log_file=$5

  filename=`basename ${file_or_directory}`
    
  if [ "${skip_tests}" != "${BOOLEAN_TRUE}" ]
  then

	hdfs dfs -test -e "${file_or_directory}"
    exit_code=$?
    
    success_message="Source hadoop file/directory ${file_or_directory} exists"
    failure_message="Source hadoop file/directory does not exist"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
    fn_local_create_directory_if_not_exist "${to_directory}" "${fail_on_error}"
  fi 
  
test -e "${to_directory}""${filename}"
  
exit_code=$?
    
  if [ "${exit_code}" == "${BOOLEAN_TRUE}" ]
  then
  		message="File/directory ${to_directory}${filename} already exist"
  		
  		fn_log_info "${message}" "${log_file}"
  else
  		hdfs dfs -copyToLocal  "${file_or_directory}" "${to_directory}"
    	exit_code=$?

	    success_message="Successfully moved file or directory ${file_or_directory} to directory ${to_directory}"
  		failure_message="Failed to move file or directory ${file_or_directory} to directory ${to_directory}"
  		fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}" 
  
  		chmod 777 ${to_directory}${filename}
  		exit_code=$?
  
		success_message="Successfully changed file or directory permissions of ${to_directory}${filename}"
  		failure_message="Failed to change file or directory permission for ${to_directory}${filename}"
  		fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
  fi
}


########################################################################################
#																			 		   #
# Removes all files in given hadoop folder 											   #
#																					   #	
# Arguments: file_or_directory* - path of the directory to be emptied                  # 
#            fail_on_error? - flag to decide, in case of failure of this operation,    #
#							  wheather to exit the process with error code or just     #
#							  write error message and return.                          #
#																			 		   #
########################################################################################

function fn_remove_hdp_file(){
 hadoop_directory=$1
 fail_on_error=$2
 log_file=$3
 
 assert_variable_is_set "hadoop_directory" "${hadoop_directory}"
 
 hadoop fs -test -e "${hadoop_directory}"
 
 exit_code=$?
 
 if [ "${exit_code}" != "${BOOLEAN_TRUE}" ]
 then
		success_message="Hadoop directory ${hadoop_directory} is exist"
  		failure_message="Hadoop directory ${hadoop_directory} does not exist"
  		fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"  
else		 	    
 		hdfs dfs -rm -f "${hadoop_directory}/*"
 		exit_code=$?
		
		success_message="Hadoop directory ${hadoop_directory} is emptied"
  		failure_message="Hadoop directory ${hadoop_directory} is not emptied"
  		fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}" 
 fi
}


##############################################################################
#                                                                            #
# Function Compares the checksums of an  TextFiles in tmp Directory in local #
# and a incoming directory of hadoop.                                        #
# Takes one parameter                                                        #
# 1. Full path of directory should contain file                              #
#                                                                            #
##############################################################################

function fn_check_sum()  {
	log_file=$1
 if [ -n "$CHECKSUM" ]; then
   alias CHECKSUM="$CHECKSUM"
   elif which md5deep &> /dev/null; then
   alias CHECKSUM='md5deep -e'
   else
   alias CHECKSUM='md5sum'
 fi

if [ -n "$1" ]; then
   check_sum_local=$(CHECKSUM "$1" | grep --only-matching -m 1 '^[0-9a-f]*')
   
   echo 'checksum for local file:' $check_sum_local
   
#   size=$(stat -c '%s' "$1");
#   blocks=$(expr $size / 2048);


   check_sum_hadoop=$( hdfs dfs -cat "$2" | md5sum | grep --only-matching -m 1 '^[0-9a-f]*')
   
   echo 'checksum for hadoop file:' $check_sum_hadoop

   success_message='Correct file copied on cluster'
   failure_message='There is discrepancy between local file and file present on cluster'
   if [ "$check_sum_local" = "$check_sum_hadoop" ]; then
	  exit_code=0
      fail_on_error=0
      fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
   else
      exit_code=-1
      fail_on_error=-1
      fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
   fi
fi
}

##############################################################################
#																			 #
# Function to import tables from oracle to HDFS								 #
#																			 #
# Parameter list    													     #
# 	1. database connection url												 #
# 	2. source table											                 #
#   3. Target table path                                                     #
#   4. log file path                                                         #
#																			 #
##############################################################################

function fn_execute_sqoop_import_from_oracle(){
   connection_url=$1
   source_table=$2
   target_dir_path=$3
   log_file_path=$4
   fail_on_error=${BOOLEAN_FALSE}
  
   sqoop import --connect "${connection_url}" \
      --username "${USERNAME}" \
      --password "${PASSWORD}" \
      --table ${source_table} \
      --fields-terminated-by '\t' \
      --target-dir "${target_dir_path}" \
      -m "${NUMBER_OF_MAPPERS}" >> ${log_file_path} 2>&1
   exit_code="$?"      
   return ${exit_code}    
}

function fn_execute_sqoop_import_from_oracle_with_query(){
   
   username=$1
   password=$2
   connection_url=$3
   query=$4
   target_dir_path=$5
   log_file_path=$6
   fail_on_error=${BOOLEAN_FALSE}
  
   sqoop import --connect "${connection_url}" \
      --username "${username}" \
      --password "${password}" \
      --query "${query}" \
      --fields-terminated-by '~' \
      --target-dir "${target_dir_path}" \
      -m "${NUMBER_OF_MAPPERS}" >> ${log_file_path} 2>&1
   exit_code="$?"      
   return ${exit_code}   
}


################################################################################
#                                                                              #
# Function to delete directory skiping trash								   #
#                                                                              #
################################################################################

function fn_hadoop_skiptrash_delete_directory_if_exists(){
  directory=$1
  fail_on_error=$2
  log_file=$3
 
  assert_variable_is_set "directory" "${directory}"

  hdfs dfs -test -e "${directory}"

  exit_code=$?

  if [ ${exit_code} == $EXIT_CODE_SUCCESS ]
  then
    hdfs dfs -rm -r -skipTrash "${directory}"
    exit_code=$?

    success_message="Deleted hadoop directory ${directory}"
    failure_message="Failed to delete hadoop directory ${directory}"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
    return ${exit_code} 
  else
    fn_log_info "Hadoop directory ${directory} does not exist" "${log_file}"
  fi
}


################################################################################
#                                                                              #
# Function to copy the local file to archive								   #
#                                                                              #
################################################################################

function fn_copy_from_local_to_archive(){
  file_or_directory=$1
  to_directory=$2
  skip_tests=$3
  fail_on_error=$4
  log_file=$5

  filename=`basename ${file_or_directory}`
  
  
  if [ "${skip_tests}" != "${BOOLEAN_TRUE}" ]
  then
    test -e "${file_or_directory}"
    exit_code=$?
   
    success_message="Source local file/directory ${file_or_directory} exists"
    failure_message="Source local file/directory ${file_or_directory} does not exist"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
    
    fn_hadoop_create_directory_if_not_exists "${to_directory}" "${fail_on_error}" "${log_file}"
  fi

  hadoop fs -test -e "${to_directory}/${filename}"
  exit_code=$?

  if [ "${exit_code}" == "${EXIT_CODE_SUCCESS}" ]
  then
     message="File/directory ${to_directory}/${filename} already exist"     
     
    fn_log_info "${message}" "${log_file}"
    
    
  fi
    hdfs dfs -copyFromLocal -f "${file_or_directory}" "${to_directory}"
    exit_code=$?
  
    success_message="Successfully copy file or directory ${file_or_directory} to directory ${to_directory}"
    failure_message="Failed to copy file or directory ${file_or_directory} to directory ${to_directory}"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}" 
    
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    then
  	     return $exit_code
    fi 
    
    hdfs dfs -chmod 777 ${to_directory}/${filename}
    exit_code=$?
  
    success_message="Successfully changed file or directory permissions of ${to_directory}/${filename}"
    failure_message="Failed to change file or directory permission for ${to_directory}/${filename}"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
    
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    then
  	     return $exit_code
    fi

}

################################################################################
#                                                                              #
# Function to update edp_audit table for respective table data                 #
#                                                                              #
################################################################################

function fn_update_edp_audit(){

        table_name=$(echo "$1" | tr '[:upper:]' '[:lower:]')
        min_seq=$2
        hive_database=$3
        hive_table=$4
        column_name=$5
        log_file_path=$6
        last_modified=`date +"%Y-%m-%d %H:%M:%S"`

        max_seq=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 \
          -e "SELECT COALESCE(MAX("$column_name"),0) FROM ${hive_database}.${hive_table};" 2>> ${log_file_path}`
        exit_code=$?
        
        if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   		then
	  		return $exit_code
   		else
        
        export PGPASSWORD=$POSTGRES_PASSWORD;
        export PGHOST=$POSTGRES_HOST;
        export PGPORT=$POSTGRES_PORT;
        psql $POSTGRES_DATABASE_NAME $POSTGRES_USERNAME 1>> ${log_file_path} 2>> ${log_file_path} << EOF
                update ${EDP_AUDIT} set last_modified='$last_modified', min_sequence=$min_seq, max_sequence=$max_seq
                where (table_name='$table_name');
EOF
exit_code=$?

fi

return $exit_code

}

################################################################################
#                                                                              #
# Function to load data from AWS S3 to HDFS					                   #
#                                                                              #
################################################################################

function fn_distcp_from_s3(){

	accesskey=$1
	secretkey=$2
	num_mappers=$3
	source_directory=$4
	destination_directory=$5
	log_file_path=$6
	
hadoop distcp -Dfs.s3n.awsAccessKeyId=${accesskey} -Dfs.s3n.awsSecretAccessKey=${secretkey} -m ${num_mappers} \
   -overwrite "s3n://${source_directory}" "${destination_directory}" 1>> "${log_file_path}" 2>> "${log_file_path}"
exit_code=$?

return $exit_code
}

################################################################################
#                                     End                                      #
################################################################################