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
#                 2. Executes transform function.
#               		Input Arguments for this script are: Phase name (transform),Log file path(/local/file/path),source_date(2015-12-28)
#   Author(s)   : DataMetica Team
#   Usage       : setup - 
#						sh module.sh "setup" "/local/file/path" 
#				: Transform - 
#						sh module.sh "transform" "/local/file/path" "2015-12-28"
#   Date        : 12/28/2015
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
#    1.0     DataMetica Team          12/28/2015       Initial version
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

log_file_path=$2

# Create initial setup for the tables to process the batch load 
function fn_module_setup(){

  log_file_path=$1
  # Create module data directory if not exists.
  fn_module_create_hive_table_data_layer_directory_structure \
    "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA_CUSTOMER}" \
    "${GOLD_H_OPTIMUM_USER_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_GOLD}" \
     "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA_CUSTOMER}/${GOLD_H_OPTIMUM_USER_TBL}" \
     "${GOLD_H_OPTIMUM_USER_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"
}

# Deriving gold optimum user table at Gold layer
function fn_module_transform(){
  
  log_file_path=$1
  source_date=$2
  
  source_database=${HIVE_DATABASE_NAME_INCOMING}
  source_table=${INCOMING_H_OPTIMUM_USER_TBL}
  target_database=${HIVE_DATABASE_NAME_GOLD}
  target_table=${GOLD_H_OPTIMUM_USER_TBL}
  source_date=${source_date}

  pig_file_path="${MODULE_HOME}/pig/module-transform.pig"

  current_batch_id_path="${BATCH_ID_DIR_SUBJECT_AREA}/current"

  batch_id=`cat $current_batch_id_path`

  module_type="PIG"
  module_log_filename="${module_type}_$(echo "$target_table" | tr '[:lower:]' '[:upper:]')"
  module_log_file="${LOG_DIR_SUBJECT_AREA}/${module_log_filename}.log"
  module_err_file="${LOG_DIR_SUBJECT_AREA}/${module_log_filename}.err"
  module_out_file="${LOG_DIR_SUBJECT_AREA}/${module_log_filename}.out"

  fn_log_info "Log file : ${module_log_file}" "${log_file_path}"

  fn_hadoop_delete_directory_if_exists "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA_CUSTOMER}/${GOLD_H_OPTIMUM_USER_TBL}" \
     "${BOOLEAN_FALSE}" \
     "${log_file_path}"
  exit_code=$?
  
  if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
  then
      fn_log_error "Failed to empty path ${DATA_LAYER_DIR_GOLD_SUBJECT_AREA_CUSTOMER}/${GOLD_H_OPTIMUM_USER_TBL}" ${log_file_path}
      return $exit_code
  fi   
    
  fn_log_info "Deleted all the files from ${DATA_LAYER_DIR_GOLD_SUBJECT_AREA_CUSTOMER}/${GOLD_H_OPTIMUM_USER_TBL}" ${log_file_path}

  fn_hadoop_create_directory_if_not_exists "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA_CUSTOMER}/${GOLD_H_OPTIMUM_USER_TBL}" \
     "${BOOLEAN_FLASE}" \
     "${log_file_path}"
  exit_code=$?
  
  if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
  then
      fn_log_error "Failed to create directory  ${DATA_LAYER_DIR_GOLD_SUBJECT_AREA_CUSTOMER}/${GOLD_H_OPTIMUM_USER_TBL}" ${log_file_path}
      return $exit_code
  fi  
  

    pig -x tez -p source_database="${source_database}" \
      -p source_table="${HIVE_TABLE_PREFIX}${source_table}" \
      -p target_database="${target_database}" \
      -p target_table="${HIVE_TABLE_PREFIX}${target_table}" \
      -p source_date="${source_date}" \
      -p version=${version} \
      -p udf_jar_path="${PIG_UDF_JAR_PATH}" \
      -useHCatalog \
      -logfile "${module_log_file}" \
      "$pig_file_path" 1>> "${module_err_file}" 2>> "${module_out_file}"
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