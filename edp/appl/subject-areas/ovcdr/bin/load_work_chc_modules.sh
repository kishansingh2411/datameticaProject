#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_work_chc_modules.sh 
#   Program type: Unix Bash Shell script
#   Purpose:    : This will load data in all work layer.                            
#                 Input Arguments for this script is: key_Param_id.               
#   Author(s)   : DataMetica Team
#   Usage       : sh load_work_chc_modules.sh  key_Param_id
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

params="$(fn_get_ovcdr_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

source_date=$(echo $params | cut -f1 -d'~'| cut -f1 -d ' ')
dataset_name_upper=$(echo $params | cut -f3 -d'~')
dataset_name=$(echo "$dataset_name_upper" | tr '[:upper:]' '[:lower:]')
param_numeric=$(echo ${params} | cut -f4 -d'~')

job_name="EDP_${key_param_id}_LD_WORK_CHC_MDULS"
start_time=`date +"%Y-%m-%d %H:%M:%S"`
current_batch_id_path="${BATCH_ID_DIR}/$dataset_name/current"
batch_id=`cat $current_batch_id_path`
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

fn_log_info "Loading work CHC for dataset_name $dataset_name" "${log_file_path}"

if [[ $dataset_name == "vma_nsn" ]]
then
   table_name=${WORK_CHC_VMA_NSN_CALL_USAGE_TBL}     
elif [[ $dataset_name == "int_rad" ]]
then
   table_name=${WORK_CHC_INT_RAD_CALL_USAGE_TBL}
elif [[ $dataset_name == "dom_nsn_out" ]]
then
   table_name=${WORK_CHC_DOM_NSN_OUT_USAGE_TBL}
elif [[ $dataset_name == "int_nsn" ]]
then
    table_name=${WORK_CHC_INT_NSN_CALL_USAGE_TBL}
elif [[ $dataset_name == "dom_nsn_in" ]]
then
    table_name=${WORK_CHC_DOM_NSN_IN_USAGE_TBL}
else
	fn_log_info "Invalid dataset_name $dataset_name provided!!!"  ${log_file_path}
	exit -1
fi

sh $SUBJECT_AREA_HOME/${table_name}_tbl/bin/module.sh "prepare" \
   "${source_date}" \
   "${log_file_path}" \
   "${param_numeric}"
exit_code=$?


if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
   fn_log_error "For dataset_name $dataset_name, failed to load chc work table ${table_name}, Quitting the process" "${log_file_path}"
   fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name}" " "
fi   

fn_log_info "For dataset_name $dataset_name, successfully loaded chc work table ${table_name}" "${log_file_path}"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

record_count=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} \
   --showHeader=false --silent=true --outputformat=tsv2 -e \
   "SELECT COUNT(*) FROM ${HIVE_DATABASE_NAME_WORK}.${HIVE_TABLE_PREFIX}${table_name};"`
                           
fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "${record_count}" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name}" " "

##############################################################################
#                                    End                                     #
##############################################################################