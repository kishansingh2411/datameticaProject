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
#   Purpose:    : Module script perform following activities
#                 1. Executes setup function to prepare work_rsdvr_requests_archive_tbl table in work layer.
#               		Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)                 										  
#                 2. Executes prepare function.
#               		Input Arguments for this script are: "prepare" "${source_table}" "${log_file_path}"
#   Author(s)   : DataMetica Team
#   Usage       : setup - 
#						sh module.sh "setup" "/local/file/path" 
#				: Prepare - 
#						sh module.sh "Prepare" "${source_table}" "/local/file/path"
#   Date        : 05/05/2016
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
#    1.0     DataMetica Team          05/05/2016       Initial version
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
. ${HOME}/etc/postgres.properties
. ${HOME}/etc/namespace.properties                                                             
. ${HOME}/etc/beeline.properties
. ${HOME}/etc/default.env.properties     
. ${SUBJECT_AREA_HOME}/etc/subject-area.env.properties
. ${SUBJECT_AREA_HOME}/etc/mysql.properties
. ${SUBJECT_AREA_HOME}/etc/mrdvr.properties
. ${SUBJECT_AREA_HOME}/bin/mrdvr_functions.sh

###                                                                           
# Load all utility functions. All the script arguments are also passed to      
# functions.sh so that addition processing can be done inside function.sh     
# if required. E.g if the batch id passed from command line then functio      
# ns.sh can store it in a global environment variable so that module can      
# access it.                                                                  
. ${HOME}/bin/functions.sh "$@"

###############################################################################
#                                   Declare                                   #
###############################################################################

# Create initial setup for the tables to process the batch load 
function fn_module_setup(){

  log_file_path=$1
  # Create module data directory if not exists.
  fn_module_create_hive_table_data_layer_directory_structure \
    "${DATA_LAYER_DIR_WORK_SUBJECT_AREA_STAGING}" \
    "${WORK_RSDVR_REQUESTS_ARCHIVE_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_WORK}" \
     "${DATA_LAYER_DIR_WORK_SUBJECT_AREA_STAGING}/${WORK_RSDVR_REQUESTS_ARCHIVE_TBL}" \
     "${WORK_RSDVR_REQUESTS_ARCHIVE_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"
}

#Preparing incoming rsdvr_requests_archive table.

function fn_module_prepare(){

  source_table="$1"
  log_file_path="$2"
  target_dir="${DATA_LAYER_DIR_WORK_SUBJECT_AREA_STAGING}/${WORK_RSDVR_REQUESTS_ARCHIVE_TBL}/*"
  
  query="$(fn_build_query_for_import ${source_table} "${COL_RSDVR_REQUESTS_ARCHIVE}")"
  
  if [ -z "${query}" ]
  then
      fn_log_error "Failed to build query for ${source_table}" "${log_file_path}"
      return -1
  fi
  
  fn_hadoop_skiptrash_delete_directory_if_exists "${target_dir}" "BOOLEAN_FALSE" "${log_file_path}"
  exit_code=$?
  
  if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
  then
      fn_log_error "Failed to delete directory ${target_dir}" ${log_file_path}
      return $exit_code
  fi 
 
 
 #Sqoop INCOMING_RSDVR_REQUESTS_ARCHIVE table data
 
   fn_import_table_to_hive_from_mysql \
     "${connection_url}" \
     "${HIVE_DATABASE_NAME_WORK}" \
     "${WORK_RSDVR_REQUESTS_ARCHIVE_TBL}" \
     "${query}" \
     "${SPLITBY_COL_RSDVR_REQUESTS_ARCHIVE}" \
     "${log_file_path}" \
     "${NUM_MAPPERS}"
   exit_code=$?
   
   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
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