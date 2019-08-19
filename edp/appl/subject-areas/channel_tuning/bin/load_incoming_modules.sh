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
#                 Input Arguments for this script are: "key_param_id" "table name"              
#   Author(s)   : DataMetica Team
#   Usage       : load_incoming_modules.sh "key_param_id" "table name" 
#   Date        : 04/12/2016
#   Log File    : .../log/channel_tuning/${job_name}.log
#   SQL File    : 
#   Error File  : .../log/channel_tuning/${job_name}.log
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
source $SUBJECT_AREA_HOME/etc/channel_tuning.properties
source $SUBJECT_AREA_HOME/etc/netezza.properties
source $HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/channel_tuning_functions.sh

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

if [[ $table_name == "nz_t_f_split_channel_tuning_rst" ]]
then
    job_name="CHN_"${key_param_id}"_INCOM_SPLT_CHT_RST"
elif [[ $table_name == "t_cvb_chdv_a_hh_series_summary" ]]
then
    job_name="CHN_"${key_param_id}"_INCOM_HH_SRS_SUMM"
elif [[ $table_name == "t_cvb_f_stb_device_state" ]]
then
    job_name="CHN_"${key_param_id}"_INCOM_F_DVC_STATE"
elif [[ $table_name == "t_f_household_device" ]]
then
    job_name="CHN_"${key_param_id}"_INCOM_F_HH_DEVICE"
else
    echo "Wrong table name provided"
    exit -1
fi

params="$(fn_get_channel_tuning_params ${key_param_id})"

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
# Calling prepare phase                                                      #
#																			 #
##############################################################################

source_table="${table_name}"
target_table="incoming_${table_name}"
 
sh ${SUBJECT_AREA_HOME}/${target_table}_tbl/bin/module.sh "prepare" "${source_table}" "${log_file_path}" "${source_dir}"
exit_code=$?   

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "Failed to load "${target_table}" in Incoming layer" "${log_file_path}"
	fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "
fi 

fn_log_info "Successfully loaded ${target_table} in Incoming layer" "${log_file_path}"


##############################################################################
#                                                                            #
# Validating Hive Counts                                                     #
#                                                                            #
##############################################################################

if [ ${source_table} != t_cvb_f_stb_device_state ]
then
 column_name=`echo "${source_table}_col" | tr '[:lower:]' '[:upper:]'`
 eval column_name=\${$column_name}

 hive_counts=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true \
			--outputformat=tsv2 -e "SELECT COUNT(*) FROM $HIVE_DATABASE_NAME_INCOMING.${HIVE_TABLE_PREFIX}${target_table} \
			 WHERE \
			 ${column_name} >= '${load_start_date}' and \
			 ${column_name} < '${load_end_date}' ;"`                                                     
 exit_code=$?

 if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
 then
 	fn_log_error "Failed to fetched hive record count for table ${target_table}" "${log_file_path}"
	fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "   
 fi 

 fn_log_info "Successfully captured hive ${target_table} count " "${log_file_path}"	
      

 database_schema_name=`echo "${source_table}_USR" | tr '[:lower:]' '[:upper:]'`
 eval database_schema_name=\${$database_schema_name}

 netezza_count="$(fn_validate_counts ${source_table} "${database_schema_name}" ${load_start_date} ${load_end_date} ${column_name} ${NETEZZA_DATABASE} ${NETEZZA_DRIVER} )"
 exit_code=$?

 if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
 then
	fn_log_error "Failed to fetched Netezza record count for table ${table_name}" "${log_file_path}"
	fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "   
 fi 

 fn_log_info "Successfully captured netezza ${source_table} count " "${log_file_path}"
 
 if [ ! "${hive_counts}" == "${netezza_count}" ]
 then
	fn_log_warn "Hive table count does not matched with Netezza count" ${log_file_path}
	fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "${hive_counts}" "${EXIT_CODE_FAIL}" \
		"${source_date}" "${log_file_path}" "" "Expected Count - ${netezza_count}" "PARTIAL_LOADED"
 fi
 else
  hive_counts=" "
fi
##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "${hive_counts}" "${exit_code}" "${source_date}" "${log_file_path}" "" " "

##############################################################################
#                                    End                                     #
##############################################################################