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
#                 1. Executes setup function to prepare gold_h_optimum_user_tbl table in gold layer.
#               		Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)                 										  
#                 2. Executes prepare function.
#               		Input Arguments for this script are: Phase name (prepare),Log file path(/local/file/path)
#   Author(s)   : DataMetica Team
#   Usage       : setup - 
#						sh module.sh "setup" "/local/file/path" 
#				: Prepare - 
#						sh module.sh "prepare" "/local/file/path"
#   Date        : 12/18/2016
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
#    1.0     DataMetica Team          12/18/2016     Initial version
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
. ${SUBJECT_AREA_HOME}/etc/ods.properties

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
    "${DATA_LAYER_DIR_WORK_SUBJECT_AREA_DATA}" \
    "${WORK_CUSTOMER_ACCOUNT_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_WORK_ODS}" \
     "${DATA_LAYER_DIR_WORK_SUBJECT_AREA_DATA}/${WORK_CUSTOMER_ACCOUNT_TBL}" \
     "${WORK_CUSTOMER_ACCOUNT_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"
     
}

#Preparing incoming optimum user table.
function fn_module_prepare(){

  source_table=${INCOMING_CUSTOMER_ACCOUNT_SOURCE_TBL}
  dtm_last_updated=$1
  current_date=$2
  log_file_path=$3
  connection_url="${DRIVER}${HOST_OVCDR}:${PORT_OVCDR}:${SCHEMA_OVCDR}"
  target_dir_path_temp="${DATA_LAYER_DIR_WORK_SUBJECT_AREA_DATA}/${WORK_CUSTOMER_ACCOUNT_TBL}" 
  fail_on_error="${BOOLEAN_FALSE}"
  

  #Delete hdfs temp directory if exists
  fn_hadoop_delete_directory_if_exists "${target_dir_path_temp}" "BOOLEAN_FALSE" "${log_file_path}"
  exit_code=$?
  
  if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
  then
      fn_log_error "Failed to delete directory ${target_dir}" ${log_file_path}
      return $exit_code
  fi 
    
    ## create query to import data 
    
     query="select *  FROM "${ORACLE_DATABASE_OVCDR}.${source_table}" WHERE DTM_CREATED  >= TO_DATE('${dtm_last_updated}','yyyy-mm-dd HH24:MI:SS') AND DTM_CREATED  < TO_DATE('${current_date}','yyyy-mm-dd HH24:MI:SS') OR  DTM_LAST_UPDATED >= TO_DATE('${dtm_last_updated}','yyyy-mm-dd HH24:MI:SS') AND DTM_LAST_UPDATED < TO_DATE('${current_date}','yyyy-mm-dd HH24:MI:SS') AND \$CONDITIONS"
   
   if [ -z "${query}" ]
  then
      fn_log_error "Failed to build query for ${source_table}" "${log_file_path}"
      return -1
  fi
     
  #Sqoop Customer data
  
  fn_execute_sqoop_import_from_oracle_with_query "${USERNAME_OVCDR}" "${PASSWORD_OVCDR}" "${connection_url}" \
  "${query}" "${target_dir_path_temp}" "${log_file_path}"
  exit_code=$?
  
  if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
  then
     fn_log_error "Failed while importing data from table ${source_table} to directory ${target_dir_path_temp}" "${log_file_path}"
     return $exit_code
  else
     fn_log_info "Imported data from table ${source_table} to directory ${target_dir_path_temp}" "${log_file_path}"        
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