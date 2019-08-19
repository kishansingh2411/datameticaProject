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
#   Purpose:    : Module script to export smith unique visitor aggregate count into oracle
#                 1. Executes Prepare function to prepare smith unique visitor aggregate count into mentioned directory
#                 2. Executes Export function.
#                     This function export smith unique visitor aggregate count data into related Oracle table using Sqoop Job
#   Author(s)   : DataMetica Team
#   Usage       : Prepare - 
#						sh module.sh "prepare" ${suite_name} ${frequency} ${period}	"/log/file/path" ${dtm_created} 
#				: Export - 
#						sh module.sh "export" "${suite_name}" "${frequency}" "${duration}" "/log/file/path"
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
. ${SUBJECT_AREA_HOME}/etc/oracle.properties

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

function fn_module_prepare(){

   suite_name=$1
   frequency=$2
   period=$3
   log_file_path=$4
   dtm_created=`date +"%Y-%m-%d"`

   dir_path=${DATA_LAYER_DIR_OUTGOING_SUBJECT_AREA}/${suite_name}
   hive_script_path="${MODULE_HOME}/hive/module-transform.hql"

   hadoop fs -rm -r -f ${dir_path}

   beeline -u $HIVESERVER2_URL -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} -hivevar suite_name="$suite_name" \
      -hivevar frequency="$frequency" \
      -hivevar period="$period" \
      -hivevar dir_path="$dir_path" \
      -hivevar hive_database_name_smith="${HIVE_DATABASE_NAME_SMITH}" \
      -hivevar smith_aggregate_unique_visitor_count_tbl="${SMITH_AGGREGATE_UNIQUE_VISITOR_COUNT_TBL}" \
      -f $hive_script_path >> ${log_file_path} 2>&1;
   exit_code=$?
  
  if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
  then
     fn_log_error "Failed while exporting data to directory ${dir_path}" "${log_file_path}"
     return $exit_code     
  fi
}

#Preparing oracle table.
function fn_module_export(){
 
  suite_name=$1
  frequency=$2
  duration=$3
  log_file_path=$4
  
  
  connection_url="${DRIVER}${HOST}:${PORT}:${SCHEMA}"
  target_table="${TARGET_UNIQUE_VISITOR_AGG_TBL}"
  dir_path=${DATA_LAYER_DIR_OUTGOING_SUBJECT_AREA}/${suite_name}
  
  if [ $frequency == "daily" ]
  then
    sqoop eval --connect "${connection_url}" \
      --username "${USERNAME}" \
      --password "${PASSWORD}" \
      --query "DELETE FROM ${target_table} WHERE SUITE_NAME='${suite_name}' AND FREQUENCY='${frequency}' \
         AND PERIOD='${duration}'" >> $log_file_path
     exit_code=$?
     if [[ ${exit_code} -ne $EXIT_CODE_SUCCESS ]]
	  then
		 fn_log_error "For suite ${suite_name}, failed to delete data from ${target_table} table for ${frequency} frequency and period ${duration}" "${log_file_path}"
		 return $exit_code
	  fi 
	  fn_log_info "For suite ${suite_name}, successfully deleted data from ${target_table} table for ${frequency} frequency and period ${duration}" "${log_file_path}"
  fi
  
  sqoop eval --connect "${connection_url}" \
      --username "${USERNAME}" \
      --password "${PASSWORD}" \
      --query "delete from ${target_table} where SUITE_NAME='${suite_name}' and FREQUENCY='${frequency}' \
         and PERIOD='${duration}' and COUNT_TYPE like '%SITE_UNIQUE_VISITOR%'" >> $log_file_path
  exit_code=$?  
  if [[ ${exit_code} -ne $EXIT_CODE_SUCCESS ]]
  then
	 fn_log_error "For suite ${suite_name}, failed to delete data from ${target_table} table for $frequency frequency and period '${duration}' where COUNT_TYPE with SITE_UNIQUE_VISITOR" "${log_file_path}"
	 return $exit_code
  fi 
  fn_log_info "For suite ${suite_name}, successfully deleted data from ${target_table} table for $frequency frequency and period '${duration}' where COUNT_TYPE with SITE_UNIQUE_VISITOR" "${log_file_path}"
  
  fn_execute_sqoop_export "oracle" ${connection_url} ${target_table} ${dir_path} ${log_file_path}
  exit_code=$?
  if [[ ${exit_code} -ne $EXIT_CODE_SUCCESS ]]
  then
	 fn_log_error "Failed while exporting data to table ${target_table} from directory ${dir_path}" "${log_file_path}"
	 return $exit_code
  fi 
  fn_log_info "Exported data to oracle table ${target_table} from directory ${dir_path}" "${log_file_path}"
}

###############################################################################
#                                  Implement                                  #
###############################################################################

#Calling setup function
fn_main "${@:1}"

###############################################################################
#                                     End                                     #
###############################################################################
