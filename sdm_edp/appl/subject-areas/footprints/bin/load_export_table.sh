#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_export_table.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will export data to Oracle table
#                 Input Arguments for this script are: key_Param_id 
#                                                                     
#   Author(s)   : DataMetica Team
#   Usage       : sh load_export_table.sh key_Param_id
#   Date        : 02/12/2017
#   Log File    : .../log/${job_name}.log
#   SQL File    :                                 
#   Error File  : .../log/${job_name}.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          02/12/2017      Initial version
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
source $SUBJECT_AREA_HOME/etc/oracle.properties
source $SUBJECT_AREA_HOME/etc/footprints.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/footprints_functions.sh

##############################################################################
#                                                                            #
# Checking number of parameter passed                                        #
#                                                                            #
##############################################################################

if [ "$#" -ne 1 ]; then
  echo "Illegal number of parameters. Parameters expected [ key_param_id ]"
  exit 
fi

##############################################################################
#																			 #
#   Local Param 				 			     						     #
#                                    										 #
##############################################################################

key_param_id="$1"

params="$(fn_get_footprints_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

source_date=$(echo $params | cut -f 1 -d '~'| cut -f 1 -d ' ')
dataset_name=$(echo $params | cut -f 3 -d '~')
dataset_name=$(echo "$dataset_name" | tr '[:upper:]' '[:lower:]')
start_time=`date +"%Y-%m-%d %H:%M:%S"`
job_name="EDP_FPT_${key_param_id}_CMTS_NOD_EXPT"
batch_id_dataset_name_dir_path=${BATCH_ID_DIR}/$dataset_name
current_batch_id_path="$batch_id_dataset_name_dir_path/current"
batch_id=`cat $current_batch_id_path`
log_file_dir="${LOG_DIR_SUBJECT_AREA}"
log_file_path="${log_file_dir}/${job_name}.log"
formatted_source_date=`echo $source_date | tr -d -`
source_file_name=cmts_interface_mac_${formatted_source_date}
source_file_path="${LOCAL_DATA_DIRECTORY}"

##############################################################################
#																			 #
# Checking date format		                                                 #
#																			 #
##############################################################################

fn_check_date_format $source_date

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}" 

##############################################################################
#                                                                            #
# Exporting data to Oracle table   							                 #
#                                                                            #
##############################################################################

dir_path="${DATA_LAYER_DIR_OUTGOING_SUBJECT_AREA}/${OUTGOING_EXPORT_CMTS_NODE_TBL}/p_yyyymmdd=${formatted_source_date}/"
target_table="${DATABASE}.${TARGET_TABLE}"

fn_execute_sqoop_export "oracle" ${connection_url} ${target_table} ${dir_path} ${log_file_path}
exit_code=$?
if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "Failed to export data to Oracle table ${target_table} from directory ${dir_path}" "${log_file_path}"
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" ""
fi 
fn_log_info "Successfully exported data to Oracle table ${target_table} from directory ${dir_path}" "${log_file_path}"

##############################################################################
#                                                                            #
# Record count                                                               #
#                                                                            #
##############################################################################

current_date=`date +"%Y-%m-%d"`
record_count_sql=`sqoop eval --connect "${connection_url}" \
      --username "${USERNAME}" \
      --password "${PASSWORD}" \
      --query "select count(*) FROM ${target_table} WHERE trunc(time_of_update)=to_date('$current_date','yyyy-mm-dd')"` >> $log_file_path
     exit_code=$?
     if [[ ${exit_code} -ne $EXIT_CODE_SUCCESS ]]
          then
            fn_log_error "Failed to compute record count from ${target_table} table " "${log_file_path}"
            fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" ""
          fi
          fn_log_info "Successfully computed record count from ${target_table} table " "${log_file_path}"

record_count_cut=`echo $record_count_sql | head -6 `
record_count=`echo $record_count_cut | cut -d'|' -f4`
	  
##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "${record_count}" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name}"  ""
 
##############################################################################
#                                    End                                     #
##############################################################################