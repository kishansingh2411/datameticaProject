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
#   Date        : 08/07/2016
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
#    1.0     DataMetica Team          08/07/2016       Initial version
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
  
  fn_enable_kerberos "$kerberos_enabled" "$log_file_path"

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

 
  fn_destroy_kerberos "$kerberos_enabled"
  
  if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
  then
      exit "${exit_code}"
  fi
}

##############################################################################
#																			 #
# Function to Enable kerberos               								 #
#																			 #
#############################################################################

function fn_enable_kerberos(){
	kerberos_enabled=$1
	log_file_path=$2

	if [ $kerberos_enabled == "true" ]; then  
   		kinit etlmgr@CSCDEV.COM -k -t /apphdp/hdp/.keytabs/etlmgr.keytab
   		exit_code=$?
	fi
	
	if [[ ${exit_code} -ne $EXIT_CODE_SUCCESS ]]
	then
	   fn_log_error "Failed while initalizing kerberos security." ${log_file_path}
	   exit ${exit_code}
	fi
	
}

##############################################################################
#																			 #
# Function to Destroy kerberos               								 #
#																			 #
#############################################################################

function fn_destroy_kerberos(){
	kerberos_enabled=$1

	if [ $kerberos_enabled == "true" ]; then
   		kdestroy
	fi
}

################################################################################
#                                     End                                      #
################################################################################