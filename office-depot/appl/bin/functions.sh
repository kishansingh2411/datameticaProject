#!/bin/bash
###############################################################################
#                               General Details                               #
###############################################################################
#                                                                             #
# Name                                                                        #
#     : functions.sh                                                          #
#                                                                             #
# Description                                                                 #
#     : This script consists of all the utility functions that are used by    #
#       module scripts. Modules shall not call and hadoop related commands    #
#       directly but instead write a wrapper function in this script with     #
#       proper handling of exit codes of the commands.                        #
#                                                                             #
# Author                                                                      #
#     : Office Depot Team                                                     #
#                                                                             #
# Note                                                                        #
#     : 1) Variables defined as final in the document must not be modified    #
#          by the module script code.                                         #
#       2) If function argument ends with * then it means that required       #
#          argument.                                                          #
#       3) If function argument ends with ? then it means that optional       #
#          argument.                                                          #
#                                                                             #
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
EXIT_CODE_FAIL=0

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

###
# Log any message
#
# Arguments: message* - string message 
# 			 log_file* - path of log file 
#
function fn_log(){
  message=$1
  log_file=$2
  echo "${message}"  >> ${log_file}
}


###
# Log info message
#
# Arguments: message* - string message 
#		     log_file* - path of log file   
#
function fn_log_info(){
  message=$1
  log_file=$2
  fn_log "INFO ${message}" "${log_file}"
}


###
# Log warning message
#
# Arguments: message* - string message 
#		     log_file* - path of log file     
#
function fn_log_warn(){
  message=$1
  log_file=$2
  fn_log "WARN ${message}" "${log_file}"
}


###
# Log error message
#
# Arguments: message* - string message 
#		     log_file* - path of log file     
#
function fn_log_error(){
  message=$1
  log_file=$2
  fn_log "ERROR ${message}" "${log_file}"
}


###
# Log given error message and exit the script with given exit code
#
# Arguments: exit_code* - exit code to be checked and exited with in case its not zero
#   		 failure_message* - message to log if the exit code is non zero 
#			 log_file* - path of log file  
#
function fn_exit_with_failure_message(){
  exit_code=$1
  failure_message=$2
  log_file=$3

  fn_log_error "${failure_message}" "${log_file}"
  exit "${exit_code}"
}


###
# Check the exit code and then log the message for success or failure based on 
# exit code. If fail_on_error flag value is non zero and the exit code is non
# zero then this process exits with that exit code.
#
# Arguments: exit_code* - exit code to be checked   
#   	     success_message* - message to log if the exit code is zero
#            failure_message* - message to log if the exit code is non-zero  
#            fail_on_error? - Flag to decide, in case of failure of this operation, wheather to exit the process with error code or just write error message and return.
#            log_file* - path of log file
#
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

###
# Verify if the variable is not-set/empty or not. In case it is non-set or empty,
# exit with failure message.
#
# Arguments: variable_name* - name of the variable to be check for being not-set/empty
#   	     variable_value* - value of the variable
#
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


###
# Delete local file 
#
# Arguments: file* - path of the file to be deleted 
#            fail_on_error? - Flag to decide, in case of failure of this operation, wheather to exit the process with error code or just write error message and return.
# 
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
}

###
# Create new local directory if it does not already exist
#
# Arguments: directory* - path of the directory to be created 
#   		 fail_on_error? - Flag to decide, in case of failure of this operation, wheather to exit the process with error code or just write error message and return.
#
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
  else
    fn_log_info "Local directory ${directory} already exists" "${log_file}"
  fi
}


###
# Delete hadoop directory if it already exists
#
# Arguments: directory* - path of the directory to be created 
#            fail_on_error? - Flag to decide, in case of failure of this operation, wheather to exit the process with error code or just write error message and return.
#
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
  else
    fn_log_info "Hadoop directory ${directory} does not exist" "${log_file}"
  fi
}

###
# Create hadoop directory if it does not already exist
#
# Arguments: directory* - path of the directory to be created 
#            fail_on_error? - Flag to decide, in case of failure of this operation, wheather to exit the process with error code or just write error message and return.
#
function fn_hadoop_create_directory_if_not_exists(){
  directory=$1
  fail_on_error=$2
  log_file=$3

  assert_variable_is_set "directory" "${directory}"

  hdfs dfs -test -e "${directory}"

  exit_code=$?

  if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
  then
    hdfs dfs -mkdir -p "${directory}"

    exit_code=$?

    success_message="Created hadoop directory ${directory}"

    failure_message="Failed to create hadoop directory ${directory}"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"  
  else
    fn_log_info "Hadoop directory ${directory} already exists" "${log_file}"
  fi
}



###
# Moves a hadoop file or folder to another hadoop folder
#
# Arguments: file_or_directory* - path of the file or directory to be moved
#   	     to_directory* - path of the target directory
#            skip_tests? - skip all the path checks. This is useful if this function is called in a loop.
#            fail_on_error? - flag to decide, in case of failure of this operation, wheather to exit the process with error code or just write error message and return.
#
function fn_hadoop_move_file_or_directory(){
  file_or_directory=$1
  to_directory=$2
  skip_tests=$3
  fail_on_error=$4
  log_file=$3

  if [ "${skip_tests}" != "${BOOLEAN_TRUE}" ]
  then
    assert_variable_is_set "file_or_directory" "${file_or_directory}"

    assert_variable_is_set "to_directory" "${to_directory}"

    hdfs dfs -test -e "${file_or_directory}"

    exit_code=$?

    success_message="Source hadoop file/directory ${file_or_directory} exists"

    failure_message="Source hadoop file/directory does not exist"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"  

    fn_hadoop_create_directory_if_not_exists "${to_directory}" "${fail_on_error}" "${log_file}"
  fi

  hadoop fs -mv "${file_or_directory}" "${to_directory}"

  exit_code=$?
  
  success_message="Successfully moved file or directory ${file_or_directory} to directory ${to_directory}"

  failure_message="Failed to move file or directory ${file_or_directory} to directory ${to_directory}"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}" 
}





###
# Generate new batch id.
# This function implementation is based on generating timestamp as batch id.
# But it may be used to simple test purposes. In case of enterprise scenarios,
# entrerprise workflow scheduler should be used which has capability to run
# multiple batches in parallel. This implementation stores the newly generated
# atch id into a file named as current into the directory configured using 
# BATCH_ID_DIR environment variable. For this first time this file wont be 
# present hence it will be create. Every next time the contents of the current
# file are moved into a file named after the batch id store into current file. 
# 
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



###
# Set the global BATCH_ID with current batch id if it not already set
# 
function fn_get_current_batch_id(){

  if [ -z "${BATCH_ID}" ];
  then
    current_batch_id_file="${BATCH_ID_DIR}/current"

    BATCH_ID=`cat ${current_batch_id_file}`
  fi
  assert_variable_is_set "BATCH_ID" "${BATCH_ID}"
}


###
# Set the global LOG_FILE_NAME_MODULE variable if it is not already set with current batch id
# 
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



###
# Creates directory structure for the hive table
#
# Arguments: data_layer_dir* - data layer where this table has to be created
#            hive_table_name* - name of the hive table to be used to create folder
#            create_landing_partition? - wheater to create landing partition or not landing partition is only applicable for incoming or security layers as input marks needs to be supported.
#
function fn_module_create_hive_table_data_layer_directory_structure(){
  data_layer_dir="$1"
  hive_table_name="$2"
  create_landing_partition="$3"
  log_file=$4

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

##
# Mark current batch input
#
# Arguments: module_landing_partition_dir - path to the landing partition directory
#            module_marked_batch_id_dir - path to the current batch id directory
# 
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


##
# Generate hive initialization script
#
# 
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


###
# Creates hive database
#
# Arguments: hive_database_name - name of the hive database
#            hive_database_location - location to be used for the database
#
function create_hive_database(){
    hive_database_name="$1"
    hive_database_location="$2"
    log_file=$3

    assert_variable_is_set "hive_database_name" "${hive_database_name}"

    assert_variable_is_set "hive_database_location" "${hive_database_location}"

    hive_database_name_with_prefix="${HIVE_DATABASE_PREFIX}${hive_database_name}"

    hive -e "CREATE DATABASE IF NOT EXISTS ${hive_database_name_with_prefix} LOCATION '${hive_database_location}'"

    exit_code=$?

    fail_on_error="$BOOLEAN_TRUE"

    success_message="Successfully created hive database ${hive_database_name_with_prefix} with location ${hive_database_location}"

    failure_message="Failed to create hive database ${hive_database_name_with_prefix} with location ${hive_database_location}"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
}

##
# Execute hive script
# Arguments: pass any arguments that you want to pass to hive command
# 
function fn_create_hive_table(){

  fn_get_current_batch_id

  fn_get_module_log_dir_and_file

  hive_database="$1"
  data_layer_dir="$2"
  hive_table_name="$3"
  hive_ddl_file="$4"
  log_file=$5

  hive_table_location="${data_layer_dir}/${hive_table_name}"

  assert_variable_is_set "hive_database" "${hive_database}"

  assert_variable_is_set "hive_ddl_file" "${hive_ddl_file}"

  module_type="hive"

  module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"

  mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

  module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"
  
  HIVE_INITIALISATION_FILE="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.rc"

  fn_generate_hive_initialization_file

  fn_log_info "Log file : ${module_log_file}" "${log_file}"

  fn_log_info "Out file : ${mobule_out_file}" "${log_file}"

  fn_log_info "Err file : ${module_err_file}" "${log_file}"

  hive -d batch.id="${BATCH_ID}" \
       -hiveconf database="${HIVE_DATABASE_PREFIX}${hive_database}" \
       -hiveconf namespace="${HIVE_TABLE_PREFIX}" \
       -hiveconf location="${hive_table_location}" \
       -hiveconf ${UNIQUE_PROPERTY_PREFIX}.batch.id="${BATCH_ID}" \
       -hiveconf ${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
       -hiveconf ${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
       -hiveconf ${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
      --hiveconf hive.querylog.location="${LOG_DIR_MODULE}/${module_log_file}" \
      -i "${HIVE_INITIALISATION_FILE}" \
      -f "${hive_ddl_file}" $@ 1>> "${mobule_out_file}" 2>> "${module_err_file}" 

  exit_code=$?

  fail_on_error=$BOOLEAN_TRUE

  success_message="Successfully created hive table ${hive_ddl_file} hive script"

  failure_message="Failed to create hive table ${hive_ddl_file} hive script"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
}


##
# Execute hive script
# Arguments: pass any arguments that you want to pass to hive command
#
# 
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

  hive_database_name_with_prefix="${HIVE_DATABASE_PREFIX}${hive_database}"

  hive -d batch.id="${partition_batch_id}" \
       -hiveconf ${UNIQUE_PROPERTY_PREFIX}.batch.id="${partition_batch_id}" \
       -hiveconf ${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
       -hiveconf ${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
       -hiveconf ${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
      --hiveconf hive.querylog.location="${LOG_DIR_MODULE}/${module_log_file}" \
      -i "${HIVE_INITIALISATION_FILE}" \
      -e  "use ${hive_database_name_with_prefix}; ALTER TABLE ${HIVE_TABLE_PREFIX}${hive_table} ADD PARTITION (batch_id = '${partition_batch_id}');" $@ 1>> "${mobule_out_file}" 2>> "${module_err_file}" 

  exit_code=$?

  fail_on_error=$BOOLEAN_TRUE

  success_message="Successfully added parition ${partition_batch_id} to hive table ${hive_table}"

  failure_message="Failed to add parition ${partition_batch_id} to hive table ${hive_table}"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
}


##
# Execute pig script
#
# Arguments: pass any arguments that you want to pass to pig command
#
# 
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


##
# Execute pig script
#
# Arguments: pass any arguments that could be passed to pig command
#
# 
function fn_execute_pig(){
  log_file=$1
  
  fn_get_current_batch_id
  
  fn_get_module_log_dir_and_file
  
  module_type="pig"
  
  module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"
  
  mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"
  
  module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

  fn_log_info "Log file : ${module_log_file}" "${log_file}"

  fn_log_info "Out file : ${mobule_out_file}" "${log_file}"
  
  fn_log_info "Err file : ${module_err_file}" "${log_file}"
  
  
  #USER_NAMESPACE_PROPERTIES_FILE parameter is defined in all module scripts  
  user_namespace_param_file_option="${USER_NAMESPACE_PROPERTIES_FILE}"

  if [ ! -f "$USER_NAMESPACE_PROPERTIES_FILE" ];                                                             
  then
    user_namespace_param_file_option="${HOME}/etc/namespace.properties"
  fi

  if [ "${ENABLE_PIG_REMOTE_DEBUGGING}" == "true" ]
  then
    INTERNAL_PIG_OPTS="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=${PIG_REMOTE_DEBUGGING_PORT},suspend=y"  
  fi

  if [ "${ENABLE_PIG_INSTRUMENTATION}" == "true" ]
  then
    PIG_INSTRUMENTATION_AGENT_OPTS="-javaagent:${REPOSITORY_DIR}/${PIG_INSTRUMENTATION_AGENT_JAR}"
    PIG_STATS_JAR_PATH="${REPOSITORY_DIR}/${PIG_STATS_JAR}"
  fi

  export PIG_OPTS="${PIG_INSTRUMENTATION_AGENT_OPTS} ${INTERNAL_PIG_OPTS} ${PIG_OPTS}"

  export PIG_CLASSPATH="${PIG_STATS_JAR_PATH}:${PIG_CLASSPATH}"

  pig -Dudf.import.list="${PIG_UDF_IMPORT_LIST}"  \
      -Dunique.property.prefix="${UNIQUE_PROPERTY_PREFIX}" \
      -D${UNIQUE_PROPERTY_PREFIX}.batch.id="${BATCH_ID}" \
      -D${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
      -D${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
      -D${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
      -Djob.name="${MODULE_NAME}" \
      -p BATCH_ID="${BATCH_ID}"  \
      -useHCatalog \
      -logfile "${module_log_file}" \
      -m "${user_namespace_param_file_option}" \
      -m "${HOME}/etc/default.pig.properties" \
      -m "${SUBJECT_AREA_HOME}/etc/subject-area.pig.properties" \
      -m "${PROJECT_HOME}/etc/project.pig.properties" \
      -m "${MODULE_HOME}/etc/module.pig.properties" \
      "$@" 1>> "${mobule_out_file}" 2>> "${module_err_file}" 

  exit_code=$?

  fail_on_error=$BOOLEAN_TRUE

  success_message="Successfully executed ${MODULE_NAME} pig script "

  failure_message="Pig script ${MODULE_NAME} failed"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
}


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


function fn_execute_sqoop_export(){
  target="$1"
  log_file=$2

  case "${target}" in

    teradata)

      fn_execute_sqoop_export_to_teradata "$@"

    ;;

    oracle)

      exit_code=1

      success_message=""

      failure_message="Sqoop export to oracle is not supported yet"

      fail_on_error=${BOOLEAN_TRUE}

      fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"

    ;;

    db2)

      exit_code=1

      success_message=""

      failure_message="Sqoop export to db2 is not supported yet"

      fail_on_error=${BOOLEAN_TRUE}

      fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"

    ;;

    *)

      echo $"Usage: $0 {teradata|oracle|db2}"

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


##
# Entry point for this module. 
# It executes operation passed on the command line as the first argument while executing the script.
#
function fn_main(){
  command="$1"

  case "${command}" in

    setup)

      fn_module_setup "${@:2}"

    ;;

    mark)

      fn_module_mark "${@:2}"    

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

    *)

      echo $"Usage: $0 {setup|mark|prepare|transform|validate|export|cleanup}"

      exit 1
   
  esac

}

###
# Create local directory if it does not already exist
#
# Arguments: directory* - path of the directory to be created 
#            fail_on_error? - Flag to decide, in case of failure of this operation, wheather to exit the process with error code or just write error message and return.
#
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

###
# Moves a file from or folder from local to hadoop folder
#
# Arguments: file_or_directory* - path of the file or directory to be moved
#            to_directory* - path of the target directory
#            skip_tests? - skip all the path checks. This is useful if this function is called in a loop.
#            fail_on_error? - flag to decide, in case of failure of this operation, wheather to exit the process with error code or just write error message and return.
#
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

###
# Moves a file from or folder from local to hadoop folder
#
# Arguments: file_or_directory* - path of the file or directory to be moved
#            to_directory* - path of the target directory
#            skip_tests? - skip all the path checks. This is useful if this function is called in a loop.
#            fail_on_error? - flag to decide, in case of failure of this operation, wheather to exit the process with error code or just write error message and return.
#
function fn_copy_from_local(){
  file_or_directory=$1
  to_directory=$2
  skip_tests=$3
  fail_on_error=$4
  log_file=$5

  filename=`basename ${file_or_directory}`
    

  if [ "${skip_tests}" != "${BOOLEAN_TRUE}" ]
  then
    assert_variable_is_set "file_or_directory" "${file_or_directory}"

    assert_variable_is_set "to_directory" "${to_directory}"

	test -e "${file_or_directory}"
	
	exit_code=$?
	
	success_message="Source local file/directory ${file_or_directory} exists"

    failure_message="Source local file/directory does not exist"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
    
    fn_hadoop_create_directory_if_not_exists "${to_directory}" "${fail_on_error}"
  fi

hadoop fs -test -e "${to_directory}""${filename}"
  
exit_code=$?

  if [ "${exit_code}" == "${BOOLEAN_TRUE}" ]
  then
  		message="File/directory ${to_directory}${filename} already exist"
  		
  		fn_log_info "${message}" "${log_file}"
  else
  		hdfs dfs -copyFromLocal  "${file_or_directory}" "${to_directory}"
    	
    	exit_code=$?
  
  		success_message="Successfully moved file or directory ${file_or_directory} to directory ${to_directory}"

  		failure_message="Failed to move file or directory ${file_or_directory} to directory ${to_directory}"

  		fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}" 
  
  		hdfs dfs -chmod 777 ${to_directory}${filename}
  
  		exit_code=$?
  
		success_message="Successfully changed file or directory permissions of ${to_directory}${filename}"

  		failure_message="Failed to change file or directory permission for ${to_directory}${filename}"
  		
  		fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${log_file}"
  fi
}


###
# Moves a hadoop file or folder to local folder
#
# Arguments: file_or_directory* - path of the file or directory to be moved
#            to_directory* - path of the target directory
#            skip_tests? - skip all the path checks. This is useful if this function is called in a loop.
#            fail_on_error? - flag to decide, in case of failure of this operation, wheather to exit the process with error code or just write error message and return.
#

function fn_copy_to_local(){
  file_or_directory=$1
  to_directory=$2
  skip_tests=$3
  fail_on_error=$4
  log_file=$5

  filename=`basename ${file_or_directory}`
    
  if [ "${skip_tests}" != "${BOOLEAN_TRUE}" ]
  then
    assert_variable_is_set "file_or_directory" "${file_or_directory}"

    assert_variable_is_set "to_directory" "${to_directory}"

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

###
# Removes all files in given hadoop folder 
#
# Arguments: file_or_directory* - path of the directory to be emptied
#            fail_on_error? - flag to decide, in case of failure of this operation, wheather to exit the process with error code or just write error message and return.
#
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
# Author : sarfarazkhan                                                      #
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

################################################################################
#                                     End                                      #
################################################################################