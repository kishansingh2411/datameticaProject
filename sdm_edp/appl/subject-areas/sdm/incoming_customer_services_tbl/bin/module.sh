#!/bin/bash

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: module.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : Functions to prepare incoming_customer_services table in Incoming layer
#                 1. Executes setup function to create incoming_customer_services table in Incoming Layer.
#                    Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)  
#                 2. Executes import function to build sqoop Incoming table.
#                    Input Arguments for this script are: "${source_date}" "${table_name}" "/log/file/path"
#
#   Author(s)   : DataMetica Team
#   Usage       : setup -
#                     sh module.sh "setup" "/log/file/path"
#               : import -
#                     sh module.sh "import" "${source_date}" "${table_name}" "/log/file/path"
#   Date        : 01/18/2017
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
#    1.0     DataMetica Team          01/18/2017       Initial version
#
#
#####################################################################################################################

###############################################################################
#                          Module Environment Setup                           #
###############################################################################

###                                                                           
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

###                                                                           
# Set module, project, subject area home paths.                               
MODULE_HOME=`dirname ${SCRIPT_HOME}`
SUBJECT_AREA_HOME=`dirname ${MODULE_HOME}`
SUBJECT_AREAS_HOME=`dirname ${SUBJECT_AREA_HOME}`
HOME=`dirname ${SUBJECT_AREAS_HOME}`

###                                                                           
# Load all environment properties files                                       
. ${HOME}/etc/namespace.properties
. ${HOME}/etc/beeline.properties                                                             
. ${HOME}/etc/default.env.properties                                                                     
. ${SUBJECT_AREA_HOME}/etc/subject-area.env.properties
. ${SUBJECT_AREA_HOME}/etc/oracle.properties
. ${SUBJECT_AREA_HOME}/etc/sdm.properties

###                                                                           
# Load all utility functions. All the script arguments are also passed to      
# functions.sh so that addition processing can be done inside function.sh     
# if required. E.g if the batch id passed from command line then functio      
# ns.sh can store it in a global environment variable so that module can      
# access it.                                                                  
. ${HOME}/bin/functions.sh "$@"
. ${SUBJECT_AREA_HOME}/bin/sdm_functions.sh

###############################################################################
#                                   Declare                                   #
###############################################################################

# Create initial setup for the tables to process the batch load 
function fn_module_setup(){

  log_file_path=$1
  
  # Create module data directory if not exists.
  fn_module_create_hive_table_data_layer_directory_structure \
    "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA}" \
    "${INCOMING_CUSTOMER_SERVICES_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_INCOMING}" \
     "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA}/${INCOMING_CUSTOMER_SERVICES_TBL}" \
     "${INCOMING_CUSTOMER_SERVICES_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"
  
}

#Transforming incoming_customer_services table at incoming layer
function fn_module_import(){
  
  source_date=$1
  table_name=$2
  log_file_path="$3"
  table_name=$(echo "$table_name" | tr '[:lower:]' '[:upper:]')
  connection_url="${DRIVER}${HOST}:${PORT}/${SCHEMA}"
  query=${QUERY_CUSTOMER_SERVICES}
  query=${query//sysdate/$source_date}
  formatted_date=`echo $source_date | tr -d -`
  
  target_dir_path="${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA}/${INCOMING_CUSTOMER_SERVICES_TBL}/p_yyyymmdd=$formatted_date" 
  fail_on_error="${BOOLEAN_FALSE}"

  #Delete HDFS  directory if exists
  fn_hadoop_delete_directory_if_exists "${target_dir_path}" "BOOLEAN_FALSE" "${log_file_path}"
  exit_code=$?
  if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
  then
      fn_log_error "Failed to delete directory ${target_dir_path}" ${log_file_path}
      return $exit_code
  fi
  fn_log_info "Successfully deleted directory ${target_dir_path}" "${log_file_path}" 
    
  fn_alter_table_drop_one_partitions \
       "${HIVE_DATABASE_NAME_INCOMING}" \
       "${INCOMING_CUSTOMER_SERVICES_TBL}" \
       "p_yyyymmdd=$formatted_date" \
       "${BOOLEAN_FALSE}" \
       "${HIVESERVER2_URL}" \
       "${HIVE_USENAME}" \
       "${HIVE_PASSWORD}" \
       "${log_file_path}"
   exit_code=$?
   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
   then
       fn_log_error "Failed to drop to drop partition for table ${INCOMING_CUSTOMER_SERVICES_TBL} " ${log_file_path}
       return $exit_code
   fi
   fn_log_info "Successfully drop partition if it exist for table ${INCOMING_CUSTOMER_SERVICES_TBL}" "${log_file_path}" 
   
   fn_import_table_to_hive_from_oracle \
     ${connection_url} \
     ${HIVE_DATABASE_NAME_INCOMING} \
     ${INCOMING_CUSTOMER_SERVICES_TBL} \
     "${query}" \
     "SITE_ID" \
     ${log_file_path} \
     ${NUM_OF_MAP_CUSTOMER_SERVICES}
   exit_code=$? 
   if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   then
	   return $exit_code
   fi	
    
}

###############################################################################
#                                  Implement                                  #
###############################################################################

#Calling setup function
fn_main "${@:1}"

###############################################################################
#                                     End                                     #
###############################################################################