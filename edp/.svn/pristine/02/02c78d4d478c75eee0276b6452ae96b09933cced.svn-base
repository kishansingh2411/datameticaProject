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
#   Purpose:    : Module script to load gold opt hit data table
#                 1. Executes setup function to prepare directory structure for the table											                                						  
#                    and create gold_opt_app_hit_data_tbl in gold layer.
#               	 Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)                 										  
#                 2. Executes transform function.
#               	 Input Arguments for this script are: Phase name (transform), Suite name(stock-97), suite id (9), 
#                                                         source date,Log file path(/local/file/path)
#   Author(s)   : DataMetica Team
#   Usage       : setup - 
#						sh module.sh "setup" "/log/file/path" 
#				: Transform - 
#						sh module.sh "transform" "${suite_name}" "${suite_id}" "2015-12-28" "/log/file/path"
#
#   Date        : 09/12/2016
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
#    1.0     DataMetica Team          09/12/2016       Initial version
#
#
#####################################################################################################################

###############################################################################
#                          Module Environment Setup                           #
###############################################################################

###                                                                           
# Find absolute path to this script which is used to define module, proje     
# ct, subject area home directory paths.                                      
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
. ${SUBJECT_AREA_HOME}/etc/optimum_app.properties

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
    "${GOLD_OPT_APP_HIT_DATA_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_GOLD}" \
     "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA}/${GOLD_OPT_APP_HIT_DATA_TBL}" \
     "${GOLD_OPT_APP_HIT_DATA_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"
}

# Deriving opt hit data table at Gold layer
function fn_module_transform(){

    suite_name=$1
    suite_id=$2
    source_date=$3
    log_file_path=$4
    username=`echo $USER`
    
    pig_script_path="${MODULE_HOME}/pig/module-transform.pig"
   
    pig -useHCatalog -p suite_name=$suite_name \
		-p suite_id=$suite_id \
		-p source_date=$source_date \
		-p username=$username \
		-p namenode_service="${NAMENODE_SERVICE}" \
		-p hive_database_name_incoming=${HIVE_DATABASE_NAME_INCOMING} \
		-p hive_database_name_gold=${HIVE_DATABASE_NAME_GOLD} \
		-p incoming_hit_data_tbl="${HIVE_TABLE_PREFIX}${INCOMING_HIT_DATA_TBL}" \
		-p incoming_browser_tbl="${HIVE_TABLE_PREFIX}${INCOMING_BROWSER_TBL}" \
		-p incoming_connection_type_tbl="${HIVE_TABLE_PREFIX}${INCOMING_CONNECTION_TYPE_TBL}" \
		-p incoming_country_tbl="${HIVE_TABLE_PREFIX}${INCOMING_COUNTRY_TBL}" \
		-p incoming_javascript_version_tbl="${HIVE_TABLE_PREFIX}${INCOMING_JAVASCRIPT_VERSION_TBL}" \
		-p incoming_languages_tbl="${HIVE_TABLE_PREFIX}${INCOMING_LANGUAGES_TBL}" \
		-p incoming_operating_systems_tbl="${HIVE_TABLE_PREFIX}${INCOMING_OPERATING_SYSTEMS_TBL}" \
		-p incoming_plugins_tbl="${HIVE_TABLE_PREFIX}${INCOMING_PLUGINS_TBL}" \
		-p incoming_search_engines_tbl="${HIVE_TABLE_PREFIX}${INCOMING_SEARCH_ENGINES_TBL}" \
		-p gold_opt_hit_data_tbl="${HIVE_TABLE_PREFIX}${GOLD_OPT_APP_HIT_DATA_TBL}" \
		-f $pig_script_path 2> ${log_file_path};
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