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
#                    and create gold_bhv_acct_phone_nbr_opt_sdl table in gold layer.
#               		Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)                 										  
#                 2. Executes prepare function.
#               		Input Arguments for this script are: Phase name (prepare), 
#															 Log file path(/local/file/path)
#   Author(s)   : DataMetica Team
#   Usage       : setup - 
#						sh module.sh "setup" "/local/file/path" 
#				: Transform - 
#						sh module.sh "prepare" "/local/file/path"
#   Date        : 03/02/2017
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
#    1.0     DataMetica Team          03/02/2017       Initial version
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
. ${SUBJECT_AREA_HOME}/etc/oracle.properties
. ${SUBJECT_AREA_HOME}/etc/subject-area.env.properties
. ${SUBJECT_AREA_HOME}/etc/ods.properties

###                                                                           
# Load all utility functions. All the script arguments are also passed to      
# functions.sh so that addition processing can be done inside function.sh     
# if required. E.g if the batch id passed from command line then functio      
# ns.sh can store it in a global environment variable so that module can      
# access it.                                                                  
. ${HOME}/bin/functions.sh "$@"
. ${SUBJECT_AREA_HOME}/bin/ods_functions.sh

###############################################################################
#                                   Declare                                   #
###############################################################################

# Create initial setup for the tables to process the batch load 
function fn_module_setup(){

  log_file_path=$1	
	  
  # Create module data directory if not exists.
  fn_module_create_hive_table_data_layer_directory_structure \
    "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA_CUSTOMER}" \
    "${GOLD_BHV_ACCT_PHONE_NBR_OPT_SDL_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_GOLD}" \
     "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA_CUSTOMER}/${GOLD_BHV_ACCT_PHONE_NBR_OPT_SDL_TBL}" \
     "${GOLD_BHV_ACCT_PHONE_NBR_OPT_SDL_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"  
}

# Preparing gold_bhv_acct_phone_nbr_opt_sdl table at gold layer
function fn_module_prepare(){

  log_file_path="${1}"
  source_date="${2}"
  
  fn_hadoop_delete_directory_if_exists "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA_CUSTOMER}/${GOLD_BHV_ACCT_PHONE_NBR_OPT_SDL_TBL}/*" \
     "${BOOLEAN_FALSE}" \
     "${log_file_path}"
  
  hql_file="${MODULE_HOME}/hive/module-prepare.hql"
  
  beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} \
   -hivevar gold_database=${HIVE_DATABASE_NAME_GOLD_ODS} \
   -hivevar gold_opt_sdl_table=${GOLD_BHV_ACCT_PHONE_NBR_OPT_SDL_TBL} \
   -hivevar gold_opt_table=${GOLD_BHV_ACCT_PHONE_NBR_OPT_TBL} \
   -hivevar gold_sdl_table=${GOLD_BHV_ACCT_PHONE_NBR_SDL_TBL} \
   -hivevar source_date=${source_date} \
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