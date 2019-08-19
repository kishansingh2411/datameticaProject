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
#   Purpose:    : Functions to prepare Derived Lookup tables in Incoming layer
#                 1. Executes setup function to create derived lookup tables in Incoming Layer.
#                    Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)  
#                 2. Executes transform function to build Incoming table.
#                    Input Arguments for this script are: Phase name (transform),Suite name (cablevis-uow-com), 
#                                                         Suite Id (7), Source date (2015-12-28),
#                                                         Log file path(/local/file/path)
#   Author(s)   : DataMetica Team
#   Usage       : setup -
#                    sh module.sh "setup" "/log/file/path"
#               : transform -
#                    sh module.sh "transform" "${suite_name}" "${suite_id}" "2015-12-28" "/log/file/path"
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
. ${SUBJECT_AREA_HOME}/etc/omniture.properties

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
    "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LOOKUP}" \
    "${INCOMING_OPTIMUM_ISP_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
           
  fn_module_create_hive_table_data_layer_directory_structure \
    "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LOOKUP}" \
    "${INCOMING_OPTIMUM_SITE_SECTIONS_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
   fn_module_create_hive_table_data_layer_directory_structure \
    "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LOOKUP}" \
    "${INCOMING_OPTIMUM_PAGENAMES_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
   fn_module_create_hive_table_data_layer_directory_structure \
    "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LOOKUP}" \
    "${INCOMING_OPTIMUM_ERROR_MESSAGE_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_INCOMING}" \
     "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LOOKUP}" \
     "" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
	 "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"  
}

#Transforming the Derived table at Incoming layer
function fn_module_transform(){

    suite_name=$1
    suite_id=$2
    source_date=$3
    log_file_path=$4   

    module_name=`basename ${MODULE_HOME}`

    max_isp_id=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
               "select coalesce(max(isp_id),0) \
               from ${HIVE_DATABASE_NAME_INCOMING}.${HIVE_TABLE_PREFIX}${INCOMING_OPTIMUM_ISP_TBL} \
               where suite_id = ${suite_id};"`
    exit_code=$?    
    
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    then
         fn_log_error "Failed to fetch max_isp_id from hive table $INCOMING_OPTIMUM_ISP_TBL" "${log_file_path}" 
  	     return $exit_code
    fi 
    
    max_site_section_id=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false \
                        --silent=true --outputformat=tsv2 -e"select coalesce(max(site_section_id),0) \
                        from ${HIVE_DATABASE_NAME_INCOMING}.${HIVE_TABLE_PREFIX}${INCOMING_OPTIMUM_SITE_SECTIONS_TBL} \
                        where suite_id = ${suite_id};"`
    exit_code=$?    
    
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    then
         fn_log_error "Failed to fetch max_site_section_id from hive table $INCOMING_OPTIMUM_SITE_SECTIONS_TBL" "${log_file_path}"
  	     return $exit_code
    fi
    
    max_pagenames_id=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true \
                      --outputformat=tsv2 -e"select coalesce(max(PAGE_NAME_ID),0) \
                      from ${HIVE_DATABASE_NAME_INCOMING}.${HIVE_TABLE_PREFIX}${INCOMING_OPTIMUM_PAGENAMES_TBL} \
                      where suite_id = ${suite_id};"`
    exit_code=$?    
    
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    then
    	fn_log_error "Failed to fetch max_pagenames_id from hive table $INCOMING_OPTIMUM_PAGENAMES_TBL" "${log_file_path}"
  	     return $exit_code
    fi
    
    max_error_message_id=0    
    pig_script_path="${MODULE_HOME}/pig/module-transform.pig"
    
    if [[ ! $suite_name == *vow* ]]
    then
       pig_script_path="${MODULE_HOME}/pig/module-transform-opt_error_message.pig"
       max_error_message_id=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true \
                             --outputformat=tsv2 -e"select coalesce(max(OPT_ERR_MSG_ID),0) \
                             from ${HIVE_DATABASE_NAME_INCOMING}.${HIVE_TABLE_PREFIX}${INCOMING_OPTIMUM_ERROR_MESSAGE_TBL} \
                             where suite_id = ${suite_id};"`
       exit_code=$?    
    
       if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
       then
           fn_log_error "Failed to fetch max_error_message_id from hive table $INCOMING_OPTIMUM_ERROR_MESSAGE_TBL" "${log_file_path}"
  	       return $exit_code
       fi      
    fi    
  
   if [[ $suite_name == 'channel900' ]]
    then
     INCOMING_HIT_DATA_TBL=${INCOMING_HIT_DATA_STOCK36_TBL}
   fi
   
   current_batch_id_path="${BATCH_ID_DIR}/${suite_name}/current"

   batch_id=`cat $current_batch_id_path`   

   pig -p suite_name=$suite_name \
       -p suite_id=$suite_id \
       -p source_date=$source_date \
       -p source_database=${HIVE_DATABASE_NAME_INCOMING} \
       -p source_table="${HIVE_TABLE_PREFIX}${INCOMING_HIT_DATA_TBL}" \
       -p LOOKUP_MACROS_SCRIPT_ABSOLUTE_PATH="${SUBJECT_AREA_HOME}/${LOOKUP_MACROS_RELATIVE_PATH}" \
       -p max_isp_id=${max_isp_id} \
       -p isp_table=${INCOMING_OPTIMUM_ISP_TBL} \
       -p max_site_section_id=${max_site_section_id} \
       -p site_section_table=${INCOMING_OPTIMUM_SITE_SECTIONS_TBL} \
       -p max_pagenames_id=${max_pagenames_id} \
       -p pagenames_table=${INCOMING_OPTIMUM_PAGENAMES_TBL} \
       -p max_error_message_id=${max_error_message_id} \
       -p error_message_table=${INCOMING_OPTIMUM_ERROR_MESSAGE_TBL} \
       -p pig_script_path=${MODULE_HOME}/pig/module-transform.pig \
       -p path_lookup=${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LOOKUP} \
       -useHCatalog \
       -logfile ${log_file_path} \
       -f "$pig_script_path" 1>> "${log_file_path}" 2>> "${log_file_path}";
   exit_code=$?

   success_message="Successfully executed ${module_name} pig script "
   failure_message="Pig script ${module_name} failed"
   fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_FALSE}" "${log_file_path}"
   return $exit_code
}

###############################################################################
#                                  Implement                                  #
###############################################################################

#Calling setup function
fn_main "${@:1}"

###############################################################################
#                                     End                                     #
###############################################################################