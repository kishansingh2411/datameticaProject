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
#   Purpose:    : This will load data in all work layer.                            
#                 Input Arguments for this script is: key_Param_id.               
#   Author(s)   : DataMetica Team
#   Usage       : sh load_work_modules.sh  key_Param_id
#   Date        : 12/19/2016
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
#    1.0     DataMetica Team          12/19/2016       Initial version
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

##############################################################################
#																			 #
#   Local Param 				 			     						     #
#                                    										 #
##############################################################################

key_param_id="$1"
operation="$2"

params="$(fn_get_ovcdr_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

source_date=$(echo $params | cut -f 1 -d '~'| cut -f 1 -d ' ')
dataset_name_upper=$(echo $params | cut -f 3 -d '~')
dataset_name=$(echo "$dataset_name_upper" | tr '[:upper:]' '[:lower:]')
param_numeric=$(echo ${params} | cut -f4 -d'~')

start_time=`date +"%Y-%m-%d %H:%M:%S"`
current_batch_id_path="${BATCH_ID_DIR}/$dataset_name/current"
batch_id=`cat $current_batch_id_path`

##############################################################################
#																			 #
# Checking operation to use                                                  #
#																			 #
##############################################################################

fn_check_operation_name $operation

if [[ $operation == insert ]] 
 then 
    operation_name="prepare"
    job_name="EDP_${key_param_id}_INSRT_WORK_MDULS"
elif [[ $operation == update ]] 
 then
   operation_name="transform"
   job_name="EDP_${key_param_id}_UPD_WORK_MDULS"
elif [[ $operation == delete ]] 
 then
   operation_name="delete" 
   job_name="EDP_${key_param_id}_DEL_WORK_MDULS"
fi

log_file_path="${LOG_DIR_SUBJECT_AREA}/${job_name}.log"

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
#																			 #
# Loading Work table                                                         #
#																			 #
##############################################################################

source_date_file=`echo ${source_date//-/''}`
source_file_path=`ls ${LOCAL_DATA_DIRECTORY}/$dataset_name_upper"_"$source_date_file*.gz`
source_file_name=`basename $source_file_path`
file_name=`echo ${source_file_name//.gz}`

fn_log_info "Loading work for dataset_name $dataset_name" "${log_file_path}"

if [[ $dataset_name == "vma_nsn" ]]
then
   table_name=${WORK_A_VMA_NSN_CALL_USAGE_TBL}
elif [[ $dataset_name == "int_rad" ]]
then
   table_name=${WORK_A_INT_RAD_CALL_USAGE_TBL}
elif [[ $dataset_name == "dom_nsn_out" ]]
then
   table_name=${WORK_A_DOM_NSN_OUT_USAGE_TBL}
elif [[ $dataset_name == "int_nsn" ]]
then
    table_name=${WORK_A_INT_NSN_CALL_USAGE_TBL}
elif [[ $dataset_name == "dom_nsn_in" ]]
then
    table_name=${WORK_A_DOM_NSN_IN_USAGE_TBL}
else
	fn_log_info "Invalid dataset_name $dataset_name provided!!!"  ${log_file_path}
	exit -1
fi

sh $SUBJECT_AREA_HOME/${table_name}_tbl/bin/module.sh \
   "$operation_name" \
   "${source_date}" \
   "${file_name}" \
   "${log_file_path}" \
   "${param_numeric}"
exit_code=$?

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
   fn_log_error "For dataset_name $dataset_name, failed to load work table ${table_name}, Quitting the process" "${log_file_path}"
   fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name}" " "
fi   

fn_log_info "For dataset_name $dataset_name, successfully loaded work table ${table_name}" "${log_file_path}"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

if [[ $operation == insert ]]
then 
    record_count=$(fn_get_count "${HIVESERVER2_URL}" "${HIVE_USENAME}" "${HIVE_PASSWORD}" "${HIVE_DATABASE_NAME_WORK}" "${table_name}" "I" "${source_date}")
    exit_code=$?
elif [[ $operation == update ]]
then
    record_count=$(fn_get_count "${HIVESERVER2_URL}" "${HIVE_USENAME}" "${HIVE_PASSWORD}" "${HIVE_DATABASE_NAME_WORK}" "${table_name}" "U" "${source_date}")
    exit_code=$?
elif [[ $operation == delete ]] 
then  
   record_count="NA"
   exit_code=$?
fi

if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
then
   fn_log_error "Failed while computing record count for table ${table_name} " "${log_file_path}"
   fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "
fi
                           
fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "${record_count}" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name}" " "

##############################################################################
#                                    End                                     #
##############################################################################