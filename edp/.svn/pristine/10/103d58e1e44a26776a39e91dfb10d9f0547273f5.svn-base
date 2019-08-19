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
#                 1. Executes setup function to create work_bhv_acct_phone_nbr_sdl_tmp table in work layer.
#               		Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)                 										  
#                 2. Executes prepare function.
#               		Input Arguments for this script are: Phase name (prepare),Log file path(/local/file/path)
#   Author(s)   : DataMetica Team
#   Usage       : setup - 
#						sh module.sh "setup" "/local/file/path" 
#				: Prepare - 
#						sh module.sh "prepare" "/local/file/path"
#   Date        : 02/07/2017
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
#    1.0     DataMetica Team          02/07/2017     Initial version
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
    "${WORK_BHV_ACCT_PHONE_NBR_SDL_TMP_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_WORK_ODS}" \
     "${DATA_LAYER_DIR_WORK_SUBJECT_AREA_DATA}/${WORK_BHV_ACCT_PHONE_NBR_SDL_TMP_TBL}" \
     "${WORK_BHV_ACCT_PHONE_NBR_SDL_TMP_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"     
}

#Preparing work_bhv_acct_phone_nbr_sdl_tmp optimum table.
function fn_module_prepare(){

   source_date=$1
   log_file_path=$2

   target_dir_path="${DATA_LAYER_DIR_WORK_SUBJECT_AREA_DATA}/${WORK_BHV_ACCT_PHONE_NBR_SDL_TMP_TBL}/*"

   #Delete data from work table
   fn_hadoop_delete_directory_if_exists "${target_dir_path}" "BOOLEAN_FALSE" "${log_file_path}"
   exit_code=$?
   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
   then
      fn_log_error "Failed to delete data from directory ${target_dir_path}" ${log_file_path}
      return $exit_code
   fi
   
   fn_log_info "Successfully deleted data from directory ${target_dir_path}" "${log_file_path}" 

   hql_file=${MODULE_HOME}/hive/module-transform.hql
   dtm_expired_default=`echo "${DEFAULT_TIMESTAMP}" | cut -d" " -f1`

   beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} \
   -hivevar work_database=${HIVE_DATABASE_NAME_WORK_ODS} \
   -hivevar gold_database=${HIVE_DATABASE_NAME_GOLD_ODS} \
   -hivevar work_bhv_acct_phone_nbr_sdl_tbl="${WORK_BHV_ACCT_PHONE_NBR_SDL_TBL}" \
   -hivevar work_bhv_acct_phone_nbr_sdl_tmp_tbl="${WORK_BHV_ACCT_PHONE_NBR_SDL_TMP_TBL}" \
   -hivevar gold_bhv_acct_phone_nbr_sdl_tbl="${GOLD_BHV_ACCT_PHONE_NBR_SDL_TBL}" \
   -hivevar dtm_expired_default="${dtm_expired_default}" \
   -f "${hql_file}" 1>> "${log_file_path}" 2>> "${log_file_path}" 
   exit_code=$?
   
   if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   then
      fn_log_error "Failed to load data in ${WORK_BHV_ACCT_PHONE_NBR_SDL_TMP_TBL} table." "${log_file_path}"
      return $exit_code
   else
      fn_log_info "Successfully loaded data in ${WORK_BHV_ACCT_PHONE_NBR_SDL_TMP_TBL} table." "${log_file_path}"        
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