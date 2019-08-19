#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_gold_modules.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will load data in all gold tables.
#                 Input Arguments for this script are: "key_param_id" "table_name" "operation"             
#   Author(s)   : DataMetica Team
#   Usage       : load_gold_modules.sh "key_param_id" "table_name"
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
#                                                                            #
# Checking number of parameter passed                                        #
#                                                                            #
##############################################################################

if [ "$#" -ne 3 ] 
then
   echo "Illegal number of parameters. Parameters expected are [ "key_param_id" "table_name" "operation" ]"
   exit -1
fi

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

key_param_id="$1"
table_name="$(echo "$2" | tr '[:upper:]' '[:lower:]')"
operation=$3

if [[ $table_name == "rsdvr_preferences_delta" ]] && [[ $operation == "transform" ]]
then
    job_name="EDP_MRDVR_600006_GLD_PRF_DLT_I"
elif [[ $table_name == "rsdvr_preferences_delta" ]] && [[ $operation == "delete" ]]
then
    job_name="EDP_MRDVR_600006_GLD_PRF_DLT_D"    
elif [[ $table_name == "rsdvr_preferences_last_rec" ]] && [[ $operation == "transform" ]]
then
    job_name="EDP_MRDVR_600006_GLD_PRF_LR_I"
elif [[ $table_name == "rsdvr_preferences_last_rec" ]] && [[ $operation == "delete" ]]
then
    job_name="EDP_MRDVR_600006_GLD_PRF_LR_D"    	
elif [[ $table_name == "rsdvr_recordings_delta" ]] && [[ $operation == "transform" ]]
then
    job_name="EDP_MRDVR_600006_GLD_REC_DLT_I"
elif [[ $table_name == "rsdvr_recordings_delta" ]] && [[ $operation == "delete" ]]
then
    job_name="EDP_MRDVR_600006_GLD_REC_DLT_D"    
elif [[ $table_name == "rsdvr_recordings_last_rec" ]] && [[ $operation == "transform" ]]
then
    job_name="EDP_MRDVR_600006_GLD_REC_LR_I"
elif [[ $table_name == "rsdvr_recordings_last_rec" ]] && [[ $operation == "delete" ]]
then
    job_name="EDP_MRDVR_600006_GLD_REC_LR_D"    	
elif [[ $table_name == "rsdvr_requests_delta" ]] && [[ $operation == "transform" ]]
then
    job_name="EDP_MRDVR_600006_GLD_REQ_DLT_I"
elif [[ $table_name == "rsdvr_requests_delta" ]] && [[ $operation == "delete" ]]
then
    job_name="EDP_MRDVR_600006_GLD_REQ_DLT_D"    
elif [[ $table_name == "rsdvr_requests_last_rec" ]] && [[ $operation == "transform" ]]
then
    job_name="EDP_MRDVR_600006_GLD_REQ_LR_I"	
elif [[ $table_name == "rsdvr_requests_last_rec" ]] && [[ $operation == "delete" ]]
then
    job_name="EDP_MRDVR_600006_GLD_REQ_LR_D"	    
elif [[ $table_name == "rsdvr_schedule_delta" ]] && [[ $operation == "transform" ]]
then
    job_name="EDP_MRDVR_600006_GLD_SCH_DLT_I"
elif [[ $table_name == "rsdvr_schedule_delta" ]] && [[ $operation == "delete" ]]
then
    job_name="EDP_MRDVR_600006_GLD_SCH_DLT_D"    
elif [[ $table_name == "rsdvr_schedule_last_rec" ]] && [[ $operation == "transform" ]]
then
    job_name="EDP_MRDVR_600006_GLD_SCH_LR_I"	
elif [[ $table_name == "rsdvr_schedule_last_rec" ]] && [[ $operation == "delete" ]]
then
    job_name="EDP_MRDVR_600006_GLD_SCH_LR_D"	    
elif [[ $table_name == "rsdvr_requests_archive_delta" ]] && [[ $operation == "transform" ]]
then
    job_name="EDP_MRDVR_600006_GLD_RQA_DLT_I"	    
elif [[ $table_name == "rsdvr_requests_archive_delta" ]] && [[ $operation == "delete" ]]
then
    job_name="EDP_MRDVR_600006_GLD_RQA_DLT_D"	    
elif [[ $table_name == "rsdvr_requests_archive_last_rec" ]] && [[ $operation == "transform" ]]
then
    job_name="EDP_MRDVR_600006_GLD_RQA_LR_I"	    
elif [[ $table_name == "rsdvr_requests_archive_last_rec" ]] && [[ $operation == "delete" ]]
then
    job_name="EDP_MRDVR_600006_GLD_RQA_LR_D"
elif [[ $table_name == "rsdvr_recordings_archive_delta" ]] && [[ $operation == "transform" ]]
then
    job_name="EDP_MRDVR_600006_GLD_RCA_DLT_I"	    
elif [[ $table_name == "rsdvr_recordings_archive_delta" ]] && [[ $operation == "delete" ]]
then
    job_name="EDP_MRDVR_600006_GLD_RCA_DLT_D"	    
elif [[ $table_name == "rsdvr_recordings_archive_last_rec" ]] && [[ $operation == "transform" ]]
then
    job_name="EDP_MRDVR_600006_GLD_RCA_LR_I"	    
elif [[ $table_name == "rsdvr_recordings_archive_last_rec" ]] && [[ $operation == "delete" ]]
then
    job_name="EDP_MRDVR_600006_GLD_RCA_LR_D"	    

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

source_date=$(echo $params | cut -d'~' -f1)
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
# Loading gold data                                                          #
#																			 #
##############################################################################

if [[ $operation == "transform" ]]
then
	sh ${SUBJECT_AREA_HOME}/gold_${table_name}_tbl/bin/module.sh "transform" "${log_file_path}" "${source_date}" 
	exit_code=$?   
	
	if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	then
		fn_log_error "Failed while performing insert-update for "${table_name}" table in Gold layer" "${log_file_path}"
		fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "
	fi 
	
	fn_log_info "Successfully performed insert-update for ${table_name} table in Gold layer" "${log_file_path}"
fi	

##############################################################################
#																			 #
# Loading gold historic data                                                 #
#																			 #
##############################################################################

if [[ $operation == "delete" ]]
then 
	sh ${SUBJECT_AREA_HOME}/gold_${table_name}_tbl/bin/module.sh "delete" "${log_file_path}" "${source_date}" 
	exit_code=$?   
	
	if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	then
		fn_log_error "Failed while updating delete records for "${table_name}" table data in Gold layer" "${log_file_path}"
		fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "
	fi 

	fn_log_info "Successfully updated delete records for ${table_name} table in Gold layer" "${log_file_path}"
fi

##############################################################################
#                                                                            #
# Get Hive Counts                                                            #
#                                                                            #
##############################################################################

target_table="gold_${table_name}"

hive_counts="$(fn_get_hive_count "${HIVESERVER2_URL}" "${HIVE_USENAME}" "${HIVE_PASSWORD}" "${HIVE_DATABASE_NAME_GOLD}" ${target_table} "${operation}")"
exit_code=$?

if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
then
	fn_log_error "Failed to fetched hive record count for table ${target_table}" "${log_file_path}"
	fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "   
fi

fn_log_info "Successfully fetched hive record count for table ${target_table} in Gold layer" "${log_file_path}"

##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "${hive_counts}" "${exit_code}" "${source_date}" "${log_file_path}" "" " "

##############################################################################
#                                    End                                     #
##############################################################################