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
#   Purpose:    : Functions to prepare browser table in work layer
#                 1. Executes setup function to create browser table in Work Layer.
#                    Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)  
#                 2. Executes prepare function to add partitions.
#                    Input Arguments for this script are: Phase name (prepare), Suite Id (9), Source date (2015-12-28),
#                                                         Log file path(/local/file/path)
#   Author(s)   : DataMetica Team
#   Usage       : setup -
#                    sh module.sh "transform" "/log/file/path"
#               : prepare -
#                    sh module.sh "export" "suite_name" "/log/file/path"
#   Date        : 21/09/2016
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
#    1.0     DataMetica Team          09/22/2016       Initial version
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
. ${SUBJECT_AREA_HOME}/etc/optimum_app.properties
. ${SUBJECT_AREA_HOME}/etc/netezza.properties

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

function fn_module_transform(){
suite_name=$1
source_date=$2
log_file_path=$3
dir_path=${DATA_LAYER_DIR_OUTGOING_SUBJECT_AREA}/${suite_name}
hive_script_path=$MODULE_HOME/hive/module-transform.hql

fn_hadoop_delete_directory_if_exists "${dir_path}" "${BOOLEAN_FALSE}" "${log_file_path}"

exit_code=$?   
	    
	if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	  then
	    fn_log_error "Failed to create directory at Outgoing layer, Quitting the prossess..!!!" "${log_file_path}"
	  	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${EXIT_CODE_FAIL}" "${source_date}" "${log_file_path}" "${source_file_name}" " "
	fi   

beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} \
    -hivevar directory_path=${dir_path} \
    -hivevar hive_database_name_gold=${HIVE_DATABASE_NAME_GOLD} \
    -hivevar gold_opt_app_hit_data_tbl=${GOLD_OPT_APP_HIT_DATA_TBL} \
    -hivevar source_date=${source_date} \
    -hivevar delimiter=${DELIMITER} \
    -f ${hive_script_path} >> ${log_file_path} 2>&1;
  exit_code=$?
     if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
     then
         return $exit_code
    fi
}

function fn_module_export(){
suite_name=$1
log_file_path=$2
netezza_export_property=${SUBJECT_AREA_HOME}/etc/netezza.properties
connection_url="${DRIVER}://${HOST}:${PORT}/${NETEZZA_DATABASE}"
target_table=${NETEZZA_TABLE}
dir_path=${DATA_LAYER_DIR_OUTGOING_SUBJECT_AREA}/${suite_name}

 sqoop eval --connect "${connection_url}" \
      --username "${USERNAME}" \
      --password "${PASSWORD}" \
      --query "DELETE FROM ${target_table}" >> $log_file_path
 exit_code=$?  
  if [[ ${exit_code} -ne $EXIT_CODE_SUCCESS ]]
  then
	 fn_log_error "failed to delete data from ${target_table} table" "${log_file_path}"
	 return $exit_code
  fi 
  fn_log_info "successfully deleted data from ${target_table} table" "${log_file_path}"
  
fn_execute_sqoop_export "netezza" "${connection_url}" "${target_table}" \
       "${dir_path}" "${NETEZZA_DRIVER}" "$netezza_export_property" "$NUMBER_OF_MAPPERS" "${DELIMITER}" "${log_file_path}"
  exit_code=$?   
	    
	if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	  then
	    fn_log_error "Failed to export data from outgoing layer to netezza table, Quitting the process..!!!" "${log_file_path}"
	  	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${EXIT_CODE_FAIL}" "${source_date}" "${log_file_path}" "${source_file_name}" " "
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