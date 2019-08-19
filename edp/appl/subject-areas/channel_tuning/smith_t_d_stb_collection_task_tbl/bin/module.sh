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
#                 1. Executes setup function to prepare smith_t_d_stb_collection_task_tbl table in smith layer.
#               		Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)                 										  
#                 2. Executes prepare function.
#               		Input Arguments for this script are: "prepare" "${source_table}" "/local/file/path"
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
. ${SUBJECT_AREA_HOME}/etc/netezza.properties
. ${SUBJECT_AREA_HOME}/etc/channel_tuning.properties
. ${SUBJECT_AREA_HOME}/bin/channel_tuning_functions.sh

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
    "${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_DIMENSION}" \
    "${SMITH_T_D_STB_COLLECTION_TASK_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_SMITH}" \
     "${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_DIMENSION}/${SMITH_T_D_STB_COLLECTION_TASK_TBL}" \
     "${SMITH_T_D_STB_COLLECTION_TASK_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"
}

#Preparing smith_t_d_stb_collection_task_tbl table.
function fn_module_prepare(){   
  
  log_file_path=$1
      
  hive_script_path="${MODULE_HOME}/hive/module-transform.hql"

  hadoop fs -test -e ${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_DIMENSION}/${SMITH_T_D_STB_COLLECTION_TASK_TBL}
  exit_code=$?
  
  if [ ${exit_code} == 0 ]
  then
     hadoop fs -rm -r "${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_DIMENSION}/${SMITH_T_D_STB_COLLECTION_TASK_TBL}/*"
  fi
  
   fn_load_target_tables \
         "${HIVE_DATABASE_NAME_SMITH}" \
         "${SMITH_T_D_STB_COLLECTION_TASK_TBL}" \
         "${HIVE_DATABASE_NAME_INCOMING}" \
         "${INCOMING_T_D_STB_COLLECTION_TASK_TBL}" \
         "${hive_script_path}" \
         "${log_file_path}" \
         "${HIVESERVER2_URL}" \
         "${HIVE_USENAME}" \
         "${HIVE_PASSWORD}"
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