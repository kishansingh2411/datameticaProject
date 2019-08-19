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
#                 1. Executes setup function to prepare directory structure for the table											                                						  
#                    and create gold_metadata_vma_nsn_call_usage_tbl in gold layer.
#               		Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)                 										  
#                 2. Executes transform function.
#               		Input Arguments for this script are: Phase name (transform),source_timestamp(201512280000), 
#															 Log file path(/local/file/path)
#   Author(s)   : DataMetica Team
#   Usage       : setup - 
#						sh module.sh "setup" "/local/file/path" 
#				: Transform - 
#						sh module.sh "transform" "20151228000" "/local/file/path"
#   Date        : 12/08/2016
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
#    1.0     DataMetica Team          12/08/2016       Initial version
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
. ${SUBJECT_AREA_HOME}/etc/ovcdr.properties

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
    "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA}" \
    "${GOLD_METADATA_VMA_NSN_CALL_USAGE_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_GOLD}" \
     "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA}/${GOLD_METADATA_VMA_NSN_CALL_USAGE_TBL}" \
     "${GOLD_METADATA_VMA_NSN_CALL_USAGE_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"  
}


#Preparing gold_metadata_vma_nsn_call_usage  table.
function fn_module_prepare(){
  
  log_file_path="$1"
   
    hql_file="${MODULE_HOME}/hive/module-prepare.hql"
  
  beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} \
   -hivevar gold_database="${HIVE_DATABASE_NAME_GOLD}" \
   -hivevar table_prefix="${HIVE_TABLE_PREFIX}" \
   -hivevar gold_table="${GOLD_A_VMA_NSN_CALL_USAGE_TBL}" \
   -hivevar gold_metadata_table="${GOLD_METADATA_VMA_NSN_CALL_USAGE_TBL}" \
   -hivevar ${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
   -hivevar ${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
   -hivevar ${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
   --hivevar hive.querylog.location="${LOG_DIR_MODULE}/${log_file_path}" \
   -f "${hql_file}" 1>> "${log_file_path}" 2>> "${log_file_path}" 
  exit_code=$?
  
  if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
  then
     return $exit_code
  fi
  
}

#Transforming gold_metadata_vma_nsn_call_usage  table.
function fn_module_transform(){
  
  log_file_path="$1"
  source_date="$2"
  
  hql_file="${MODULE_HOME}/hive/module-transform.hql"
  
  beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} \
   -hivevar gold_database="${HIVE_DATABASE_NAME_GOLD}" \
   -hivevar table_prefix="${HIVE_TABLE_PREFIX}" \
   -hivevar gold_table="${GOLD_A_VMA_NSN_CALL_USAGE_TBL}" \
   -hivevar gold_metadata_table="${GOLD_METADATA_VMA_NSN_CALL_USAGE_TBL}" \
   -hivevar incoming_database="${HIVE_DATABASE_NAME_INCOMING}" \
   -hivevar incoming_table="${INCOMING_VMA_NSN_CALL_TBL}" \
   -hivevar source_date="${source_date}" \
   -hivevar ${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
   -hivevar ${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
   -hivevar ${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
   --hivevar hive.querylog.location="${log_file_path}" \
   -f "${hql_file}" 1>> "${log_file_path}" 2>> "${log_file_path}" 
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