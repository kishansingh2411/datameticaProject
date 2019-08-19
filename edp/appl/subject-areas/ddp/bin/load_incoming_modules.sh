#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_incoming_modules.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will load data in all Incoming tables.
#                 Input Arguments for this script are: "key_param_id"              
#   Author(s)   : DataMetica Team
#   Usage       : load_incoming_modules.sh "key_param_id" 
#   Date        : 05/02/2016
#   Log File    : .../log/ddp/DDP_600004_LD_${table_name}.log
#   SQL File    : 
#   Error File  : .../log/ddp/DDP_600004_LD_${table_name}.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          05/02/2016       Initial version
#
#
#####################################################################################################################

###############################################################################
#                          Module Environment Setup                           #
###############################################################################

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

# Set module, project, subject area home paths.
SUBJECT_AREA_HOME=`dirname ${SCRIPT_HOME}`
SUBJECT_AREAS_HOME=`dirname ${SUBJECT_AREA_HOME}`
HOME=`dirname ${SUBJECT_AREAS_HOME}`

###                                                                           
# Load all environment properties files
source $HOME/etc/namespace.properties
source $HOME/etc/beeline.properties
source $HOME/etc/default.env.properties
source $HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $SUBJECT_AREA_HOME/etc/ddp.properties
source $SUBJECT_AREA_HOME/etc/oracle.properties

source $HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/ddp_functions.sh

##############################################################################
#                                                                             #
# Checking number of parameter passed                                         #
#                                                                             #
##############################################################################

if [ "$#" -ne 3 ] 
then
   echo "Illegal number of parameters. Parameters expected are [ "key_param_id" "table_name" ]"
   exit -1
fi

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

key_param_id="$1"
table_name="$2"
table_type="$3"

job_name="DDP_600004_LD_`echo ${table_name} | tr '[:lower:]' '[:upper:]'`"

params="$(fn_get_ddp_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit -1
fi

load_start_date=$(echo $params | cut -d'~' -f1)
load_end_date=$(echo $params | cut -d'~' -f2) 
source_date="${load_start_date}"
start_time=`date +"%Y-%m-%d %H:%M:%S"`
fn_get_current_batch_id
log_file_path="${LOG_DIR}/${SUBJECT_AREA_NAME}/${job_name}.log"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}" 

##############################################################################
#																			 #
# Loading Incoming tables                                                    #
#																			 #
##############################################################################
    
  source_table="edp_${table_name}"
  target_table="incoming_${table_name}"

  max_seq_id="$(fn_get_max_seq_id ${source_table})"
  exit_code=$?
  
  if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
  then
     fn_log_error "Failed to fetched max_seq_id from ${DDP_AUDIT} for table ${target_table}" "${log_file_path}"
     fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "     
  fi
  
  if [ "${table_type}" == "${MEDIUM_TBL}" ]
  then
     sh ${SUBJECT_AREA_HOME}/work_${table_name}_tbl/bin/module.sh "prepare" "${max_seq_id}" "${source_table}" "${log_file_path}" ${load_start_date} ${load_end_date}
     exit_code=$?   

     if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
     then
        fn_log_error "Failed to load "${table_name}" in Work layer" "${log_file_path}"
        fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "
     fi 

     fn_log_info "Successfully loaded ${table_name} in Work layer" "${log_file_path}"
  fi
  
##############################################################################
#																			 #
# Calling prepare phase                                                      #
#																			 #
##############################################################################
 
  sh ${SUBJECT_AREA_HOME}/${target_table}_tbl/bin/module.sh "prepare" "${max_seq_id}" "${source_table}" "${log_file_path}" ${load_start_date} ${load_end_date}
  exit_code=$?   

  if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
  then
     fn_log_error "Failed to load "${table_name}" in Incoming layer" "${log_file_path}"
     fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "
  fi 

  fn_log_info "Successfully loaded ${table_name} in Incoming layer" "${log_file_path}"
 
  fn_update_ddp_audit "${source_table}" "${max_seq_id}" "${HIVE_DATABASE_NAME_INCOMING}" "${target_table}" \
     "${HIVESERVER2_URL}" "${HIVE_USENAME}" "${HIVE_PASSWORD}" "${log_file_path}" \
     "${load_start_date}" "${load_end_date}"
  exit_code=$?
	     
  if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
  then
     fn_log_error "Failed to update ${DDP_AUDIT} for min and max sequence id " "${log_file_path}"
     fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "
  fi
 
  fn_log_info "Successfully updated ${DDP_AUDIT} for min and max sequence id " "${log_file_path}"

  new_min_seq_id=${max_seq_id}
  new_max_seq_id="$(fn_get_max_seq_id ${source_table})"
  exit_code=$?
  
  if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
  then
     fn_log_error "Failed to fetched updated max_seq_id from ${DDP_AUDIT} for table ${table_name}" "${log_file_path}"
     fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "
  fi
  
  sequence_column=$(echo $table_name | cut -d'_' -f2 )
  
  if [ "${new_max_seq_id}" == "0" ]
  then
     hive_counts=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true \
                    --outputformat=tsv2 -e "SELECT COUNT(*) FROM $HIVE_DATABASE_NAME_INCOMING.${HIVE_TABLE_PREFIX}incoming_${sequence_column};"`
  else
     hive_counts=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true \
              --outputformat=tsv2 -e "SELECT COUNT(*) FROM $HIVE_DATABASE_NAME_INCOMING.${HIVE_TABLE_PREFIX}incoming_${sequence_column} \
              WHERE ${sequence_column}_seq > ${new_min_seq_id} and  ${sequence_column}_seq <= ${new_max_seq_id};"`
  fi

  fn_log_info "Successfully captured hive ${table_name} count " "${log_file_path}"

##############################################################################
#                                                                            #
# Validating Hive Counts                                                     #
#                                                                            #
##############################################################################

table_name=$2

if [ "${table_name}" != "controlparams" ]
then
   oracle_count="$(fn_validate_counts ${source_table} ${hive_counts} ${new_min_seq_id} ${ORACLE_DATABASE} ${load_start_date} ${load_end_date})"
   exit_code=$?

   if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   then
      fn_log_error "Failed to fetched Oracle record count for table ${table_name}" "${log_file_path}"
      fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "   
   fi 
   
   fn_log_info "Successfully captured oracle ${source_table} count " "${log_file_path}"
   
   if [ ! "${hive_counts}" == "${oracle_count}" ]
   then
      fn_log_warn "Hive table count does not matched with Oracle count" ${log_file_path}
      fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "${hive_counts}" "${EXIT_CODE_FAIL}" \
	     "${source_date}" "${log_file_path}" "" "Expected Count - ${oracle_count}" "PARTIAL_LOADED"
	     
   fi
fi

##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

exit_code="${EXIT_CODE_SUCCESS}"

fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "${hive_counts}" "${exit_code}" "${source_date}" "${log_file_path}" "" " "

##############################################################################
#                                    End                                     #
##############################################################################