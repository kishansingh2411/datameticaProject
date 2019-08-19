#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: compute_statistics.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will load data in all gold tables.
#                 Input Arguments for this script are: "key_param_id"           
#   Author(s)   : DataMetica Team
#   Usage       : compute_statistics.sh "key_param_id"
#   Date        : 12/20/2016
#   Log File    : .../log/ovcdr/${job_name}.log
#   SQL File    : 
#   Error File  : .../log/ovcdr/${job_name}.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          12/20/2016       Initial version
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
PROJECT_HOME=`dirname ${SUBJECT_AREAS_HOME}`

###                                                                           
# Load all environment properties files
source $PROJECT_HOME/etc/namespace.properties
source $PROJECT_HOME/etc/beeline.properties
source $PROJECT_HOME/etc/default.env.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/ovcdr.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/ovcdr_functions.sh

source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/ovcdr_functions.sh

##############################################################################
#                                                                            #
# Checking number of parameter passed                                        #
#                                                                            #
##############################################################################

if [ "$#" -ne 1 ] 
then
   echo "Illegal number of parameters. Parameters expected are [ "key_param_id"]"
   exit -1
fi

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

key_param_id="$1"

params="$(fn_get_ovcdr_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id" 
    exit -1
fi

source_date=$(echo $params | cut -f 1 -d '~'| cut -f 1 -d ' ')
dataset_name_upper=$(echo $params | cut -f 3 -d '~')
dataset_name=$(echo "$dataset_name_upper" | tr '[:upper:]' '[:lower:]')
job_name="EDP_${key_param_id}_COMP_STATS"
start_date=`date +"%Y-%m-%d"`
start_time=`date +"%Y-%m-%d %H:%M:%S"`
current_batch_id_path="${BATCH_ID_DIR}/$dataset_name/current"
batch_id=`cat $current_batch_id_path`
log_file_path="${LOG_DIR_SUBJECT_AREA}/${job_name}.log"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}" 

##############################################################################
#																			 #
# Compute Insert Records Count                                               #
#																			 #
##############################################################################

if [[ $dataset_name == "vma_nsn" ]]
then
   table_name=${GOLD_A_VMA_NSN_CALL_USAGE_TBL}
 
elif [[ $dataset_name == "int_rad" ]]
then
   table_name=${GOLD_A_INT_RAD_CALL_USAGE_TBL}
 
elif [[ $dataset_name == "dom_nsn_out" ]]
then
   table_name=${GOLD_A_DOM_NSN_OUT_USAGE_TBL}
   
elif [[ $dataset_name == "int_nsn" ]]
then
    table_name=${GOLD_A_INT_NSN_CALL_USAGE_TBL}
    
elif [[ $dataset_name == "dom_nsn_in" ]]
then
    table_name=${GOLD_A_DOM_NSN_IN_USAGE_TBL}
    
else
	fn_log_info "Invalid dataset name $dataset_name provided!!!"  ${log_file_path}
	exit -1
fi

##############################################################################
#																			 #
# Compute Inserted Records Count                                             #
#																			 #
##############################################################################

insert_rec_cnt="$(fn_get_count "${HIVESERVER2_URL}" "${HIVE_USENAME}" "${HIVE_PASSWORD}" "${HIVE_DATABASE_NAME_GOLD}" "${table_name}" "I" "${source_date}")"
exit_code=$?

if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
  then
     fn_log_error "Failed while computing insert record count for table ${table_name} " "${log_file_path}"
     fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "
  fi
 
fn_log_info "Successfully computed insert record count for table ${table_name} " "${log_file_path}"

##############################################################################
#																			 #
# Compute Updated Records Count                                               #
#																			 #
##############################################################################

update_rec_cnt="$(fn_get_count "${HIVESERVER2_URL}" "${HIVE_USENAME}" "${HIVE_PASSWORD}" "${HIVE_DATABASE_NAME_GOLD}" "${table_name}" "U" "${source_date}")"
exit_code=$?

if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
  then
     fn_log_error "Failed while computing update record count for table ${table_name} " "${log_file_path}"
     fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "
  fi
 
fn_log_info "Successfully computed update record count for table ${table_name} " "${log_file_path}"

##############################################################################
#																			 #
# Compute Total Records Count                                               #
#																			 #
##############################################################################

total_rec_cnt="$(fn_get_count "${HIVESERVER2_URL}" "${HIVE_USENAME}" "${HIVE_PASSWORD}" "${HIVE_DATABASE_NAME_GOLD}" "${table_name}" "T" "${source_date}")"
exit_code=$?

if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
  then
     fn_log_error "Failed while computing Total record count for table ${table_name} " "${log_file_path}"
     fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "
  fi
 
fn_log_info "Successfully computed Total record count for table ${table_name} " "${log_file_path}"


##############################################################################
#																			 #
# Insert statistics into Postgres                                            #
#																			 #
##############################################################################

fn_insert_cdc_process "${SUBJECT_AREA_NAME}" "${table_name}" "${source_date}" "0" "0" "${insert_rec_cnt}" "${update_rec_cnt}" "0" "${total_rec_cnt}" "${log_file_path}" 

##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "" " "

##############################################################################
#                                    End                                     #
##############################################################################