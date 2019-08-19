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
#   Purpose:    : Module script to calculate gold aggregate unique visitor count
#                 1. Executes setup function to prepare directory structure for the table											                                						  
#                    and create gold_aggregate_unique_visitor_counts_tbl in gold layer.
#               	 Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)                 										  
#                 2. Executes transform function.
#               	 Input Arguments for this script are: Phase name Phase name (transform), Suite name (cablevis-uow-com), 
#                                                         Period Start date(2015-12-28),Period End date (2015-12-28), 
#                                                         Frequency (daily), Log file path(/local/file/path) 
#   Author(s)   : DataMetica Team
#   Usage       : setup - 
#						sh module.sh "setup" "/log/file/path" 
#				: Transform - 
#						sh module.sh "transform" "${suite_name}" "2015-12-28" "2015-12-28" "daily" "20151228" "/log/file/path"
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
. ${SUBJECT_AREA_HOME}/etc/omniture.properties
. ${SUBJECT_AREA_HOME}/bin/omniture_functions.sh

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
  
#Create module data directory if not exists.
  fn_module_create_hive_table_data_layer_directory_structure \
    "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA}" \
    "${GOLD_AGGREGATE_UNIQUE_VISITOR_COUNT_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
    #Create module data directory if not exists.
    
    fn_module_create_hive_table_data_layer_directory_structure \
    "${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_AGGREGATE}" \
    "${SMITH_AGGREGATE_UNIQUE_VISITOR_COUNT_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
 # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_GOLD}" \
     "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA}/${GOLD_AGGREGATE_UNIQUE_VISITOR_COUNT_TBL}" \
     "${GOLD_AGGREGATE_UNIQUE_VISITOR_COUNT_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"  
     
   # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_SMITH}" \
     "${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_AGGREGATE}/${SMITH_AGGREGATE_UNIQUE_VISITOR_COUNT_TBL}" \
     "${SMITH_AGGREGATE_UNIQUE_VISITOR_COUNT_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"  
}

function fn_module_transform(){

   suite_name=$1
   period_start_date=$2
   period_end_date=$3
   frequency=$4
   duration=$5
   log_file_path=$6
   cutoff_date=$CUTOFF_DATE
   
   if [[ $suite_name == "vow" ]]
   then
      fn_log_info "Executing for vow" "${log_file_path}"
      table_name=${GOLD_VOW_HIT_DATA_TBL}
      column_name="evar11_optimum_id"
      hive_script_path="${MODULE_HOME}/hive/module-transform.hql"
      
   elif [[ $suite_name == "vow-esp" ]]
   then
      fn_log_info "Executing for vow-esp" "${log_file_path}"
      table_name=${GOLD_VOW_HIT_DATA_ESP_TBL}
      column_name="evar11_optimum_id"
      hive_script_path="${MODULE_HOME}/hive/module-transform.hql"
      
   elif [[ $suite_name == "onet_prod" ]]
   then
      fn_log_info "Executing for onet_prod" "${log_file_path}"
      table_name=${GOLD_OPT_HIT_DATA_TBL}
      column_name="prop11_optimum_id"
      hive_script_path="${MODULE_HOME}/hive/module-transform.hql"
      
   elif [[ $suite_name == "onet_prod_esp" ]]
   then
      fn_log_info "Executing for onet_prod_esp" "${log_file_path}"
      table_name=${GOLD_OPT_HIT_DATA_ESP_TBL}
      column_name="prop11_optimum_id"
      hive_script_path="${MODULE_HOME}/hive/module-transform.hql"
      
   elif [[ $suite_name == "cablevis-uow-com" ]]
   then
      fn_log_info "Executing for unified" "${log_file_path}"
      table_name=${GOLD_UNIFIED_HIT_DATA_TBL}
      column_name="prop11_optimum_id"   
      hive_script_path="${MODULE_HOME}/hive/module-unified-transform.hql"
      
   else
      fn_log_error "Got invalid suite_name $suite_name while transform phase of $frequency unique visitor aggregate"  \
         ${log_file_path}
      return -1
   fi
 
   fn_calculate_aggregates \
      "$suite_name" \
      "$period_start_date" \
      "$period_end_date" \
      "$frequency" \
      "$duration" \
      "${HIVE_DATABASE_NAME_GOLD}" \
      "${HIVE_DATABASE_NAME_SMITH}" \
      "${HIVE_TABLE_PREFIX}${table_name}" \
      "${HIVE_TABLE_PREFIX}${GOLD_AGGREGATE_UNIQUE_VISITOR_COUNT_TBL}" \
      "${HIVE_TABLE_PREFIX}${SMITH_AGGREGATE_UNIQUE_VISITOR_COUNT_TBL}" \
      "$hive_script_path" \
      "${log_file_path}" \
      "$HIVESERVER2_URL" \
      "${HIVE_USENAME}" \
      "${HIVE_PASSWORD}" \
      "$cutoff_date"
      exit_code=$?
      
      if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
      then
         fn_log_error "Failed to calculate $frequency unique visitor aggregates for gold layer, Quitting the process" "${log_file_path}"
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