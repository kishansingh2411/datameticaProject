#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_work_sequence.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will load data in all Incoming tables.
#                 Input Arguments for this script are: "key_param_id" "table_name"             
#   Author(s)   : DataMetica Team
#   Usage       : load_work_delete_modules.sh "key_param_id" "table_name"
#   Date        : 04/12/2016
#   Log File    : .../log/mrdvr/${job_name}.log
#   SQL File    : 
#   Error File  : .../log/mrdvr/${job_name}.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          04/12/2016       Initial version
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
source $SUBJECT_AREA_HOME/etc/mrdvr.properties
source $SUBJECT_AREA_HOME/etc/mysql.properties

source $HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/mrdvr_functions.sh

##############################################################################
#                                                                             #
# Checking number of parameter passed                                         #
#                                                                             #
##############################################################################

if [ "$#" -ne 2 ] 
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
table_name="$(echo "$2" | tr '[:upper:]' '[:lower:]')"

if [[ $table_name == "rsdvr_preferences" ]]
then
    job_name="EDP_MRDVR_600006_WRK_PRF_DEL"
elif [[ $table_name == "rsdvr_recordings" ]]
then
    job_name="EDP_MRDVR_600006_WRK_REC_DEL"
elif [[ $table_name == "rsdvr_requests" ]]
then
    job_name="EDP_MRDVR_600006_WRK_REQ_DEL"
elif [[ $table_name == "rsdvr_schedule" ]]
then
    job_name="EDP_MRDVR_600006_WRK_SCH_DEL"
elif [[ $table_name == "rsdvr_requests_archive" ]]
then
    job_name="EDP_MRDVR_600006_WRK_RQA_DEL"
elif [[ $table_name == "rsdvr_recordings_archive" ]]
then
    job_name="EDP_MRDVR_600006_WRK_RCA_DEL"
else
    echo "Wrong table name provided"
    exit -1
fi

params="$(fn_get_mrdvr_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id" 
    exit -1
fi

start_date=$(echo $params | cut -d'~' -f1)
source_date=$start_date
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
# Executing work delete functionality table                                  #
#																			 #
##############################################################################

target_table=${HIVE_TABLE_PREFIX}"work_${table_name}_delete"

sh ${SUBJECT_AREA_HOME}/${target_table}_tbl/bin/module.sh "transform" "${log_file_path}" "${start_date}"
exit_code=$? 

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "For date ${source_date}, failed while executing delete for "${target_table}" in work layer" "${log_file_path}"
	fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "
fi 

fn_log_info "For date ${source_date}, successfully executed delete for ${target_table} in Work layer" "${log_file_path}"

##############################################################################
#                                                                            #
# Get Hive Counts                                                            #
#                                                                            #
##############################################################################

hive_counts="$(fn_get_hive_count "${HIVESERVER2_URL}" "${HIVE_USENAME}" "${HIVE_PASSWORD}" "${HIVE_DATABASE_NAME_WORK}" ${target_table})"
exit_code=$?

if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
then
	fn_log_error "For date ${source_date}, failed to fetched hive record count for table ${target_table}" "${log_file_path}"
	fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "   
fi

fn_log_info "For date ${source_date}, successfully fetched hive record count ${hive_counts} for table ${target_table} in Work layer" "${log_file_path}"

##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "${hive_counts}" "${exit_code}" "${source_date}" "${log_file_path}" "" " "

##############################################################################
#                                    End                                     #
##############################################################################