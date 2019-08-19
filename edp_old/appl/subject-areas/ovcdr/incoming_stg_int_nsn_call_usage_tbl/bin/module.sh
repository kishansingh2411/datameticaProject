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
#                    and create int_nsn_call_usage table at Incoming layer.
#               		Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)                 										  
#                 2. Executes transform function.
#               		Input Arguments for this script are: Phase name (transform),source_timestamp(201512280000), 
#															 Log file path(/local/file/path)
#   Author(s)   : DataMetica Team
#   Usage       : setup - 
#						sh module.sh "setup" "/local/file/path" 
#				: Transform - 
#						sh module.sh "transform" "20151228000" "/local/file/path"
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
#    1.0     DataMetica Team          12/12/2016      Initial version
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
    "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_DATA}" \
    "${INCOMING_STG_INT_NSN_CALL_USAGE_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_INCOMING}" \
     "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_DATA}/${INCOMING_STG_INT_NSN_CALL_USAGE_TBL}" \
     "${INCOMING_STG_INT_NSN_CALL_USAGE_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"  
}

#Preparing incoming_a_vma_nsn_call_usage_tbl  table.
function fn_module_prepare(){

  data_file_name=$1
  source_date="$2"
  log_file_path="$3"
 
  file_or_directory="${data_file_name}"
  to_directory="${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_DATA}/${INCOMING_STG_INT_NSN_CALL_USAGE_TBL}/source_date=${source_date}"
 
  fn_copy_from_local "${file_or_directory}" \
     "${to_directory}" \
     "${BOOLEAN_FALSE}" \
     "${BOOLEAN_FALSE}" \
     "${log_file_path}"
  exit_code=$?

  if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
  then
     fn_log_error "Failed while copying file ${data_file_name} to HDFS from local" ${log_file_path}
     return $exit_code
  fi
  
   # Alter table to add partition
  fn_alter_table_with_one_partitions \
     "${HIVE_DATABASE_NAME_INCOMING}" \
     "${INCOMING_STG_INT_NSN_CALL_USAGE_TBL}" \
     "source_date='${source_date}'" \
     "${BOOLEAN_FALSE}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}" \
     "${log_file_path}"
   exit_code=$?
   
   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
   then
      fn_log_error "Failed to alter table $INCOMING_STG_INT_NSN_CALL_USAGE_TBL for the source_date=${source_date}  , Quitting the prossess..!!!" "${log_file_path}"
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