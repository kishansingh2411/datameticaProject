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
#                 1. Executes setup function to prepare incoming_account_info table in incoming layer.
#               		Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)                 										  
#                 2. Executes prepare function.
#               		Input Arguments for this script are: Phase name (prepare),Log file path(/local/file/path)
#   Author(s)   : DataMetica Team
#   Usage       : setup - 
#						sh module.sh "setup" "/local/file/path" 
#				: Prepare - 
#						sh module.sh "prepare" "/local/file/path"
#   Date        : 02/01/2017
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
#    1.0     DataMetica Team          02/01/2017     Initial version
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
. $SUBJECT_AREA_HOME/bin/ods_functions.sh

###############################################################################
#                                   Declare                                   #
###############################################################################

# Create initial setup for the tables to process the batch load 
function fn_module_setup(){

  log_file_path=$1
  # Create module data directory if not exists.
  fn_module_create_hive_table_data_layer_directory_structure \
    "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_CUSTOMER}" \
    "${INCOMING_ACCOUNT_INFO_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_INCOMING_ODS}" \
     "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_CUSTOMER}/${INCOMING_ACCOUNT_INFO_TBL}" \
     "${INCOMING_ACCOUNT_INFO_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"
     
}

#Sqooping out the data from Oracle into Hive.
function fn_module_import(){

   src_table_name=`echo $INCOMING_ACCOUNT_INFO_TBL | cut -d'_' -f2- | tr '[:lower:]' '[:upper:]'`
   connection_url="${DRIVER}${IPTL_HOST}:${IPTL_PORT}/${IPTL_SERVICE_NAME}"
   
   # Below connections are for dev env
   #connection_url="${DRIVER}${HOST_OVCDR}:${PORT_OVCDR}:${SCHEMA_OVCDR}"
   
   source_date=$1
   log_file_path=$2
   
   target_dir_path="${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_CUSTOMER}/${INCOMING_ACCOUNT_INFO_TBL}/source_date="${source_date}"/*"
   
   #Delete hdfs directory if exists
   fn_hadoop_delete_directory_if_exists "${target_dir_path}" "BOOLEAN_FALSE" "${log_file_path}"
   exit_code=$?
  
   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
   then
      fn_log_error "Failed to delete directory ${target_dir_path}" ${log_file_path}
      return $exit_code
   fi
   fn_log_info "Successfully deleted directory ${target_dir_path}" "${log_file_path}"
   
   fn_import_from_oracle_asitis \
      ${connection_url} \
      ${src_table_name} \
      ${INCOMING_ACCOUNT_INFO_TBL} \
      ${BHVSMGR_SCHEMA} \
      ${HIVE_DATABASE_NAME_INCOMING_ODS} \
      ${BHV_EDPUSR} \
      ${BHV_EDPPWD} \
      ${NUMBER_OF_MAPPERS} \
      "ID_BHV_ACCOUNT" \
      "source_date" \
      "${source_date}" \
      ${log_file_path}
   exit_code=$?
   
   return ${exit_code}
}

###############################################################################
#                                  Implement                                  #
###############################################################################

#Calling setup function
fn_main "${@:1}"

###############################################################################
#                                     End                                     #
###############################################################################