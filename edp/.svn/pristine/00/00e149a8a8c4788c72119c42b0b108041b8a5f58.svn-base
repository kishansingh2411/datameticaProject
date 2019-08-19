#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_gold_historic_data.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will load historic data in all gold tables.
#                 Input Arguments for this script are: "key_param_id" "table_name"              
#   Author(s)   : DataMetica Team
#   Usage       : load_gold_historic_data.sh "key_param_id" "table_name"
#   Date        : 04/12/2016
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
source $SUBJECT_AREA_HOME/etc/ovcdr.properties
source $SUBJECT_AREA_HOME/etc/oracle.properties

source $HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/ovcdr_functions.sh

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

key_param_id="$1"
table_name="$(echo "$2" | tr '[:upper:]' '[:lower:]')"

if [[ $table_name == "vma_nsn_call_usage" ]]
then
    job_name="EDP_OVCDR_${key_param_id}_GD_MD_VMA_NSN_CALL"
    
elif [[ $table_name == "dom_nsn_out_usage" ]]
then
    job_name="EDP_OVCDR_${key_param_id}_GD_MD_DOM_NSN_OUT"	
  
elif [[ $table_name == "int_rad_call_usage" ]]
  then
    job_name="EDP_OVCDR_${key_param_id}_GD_MD_INT_RAD_CALL"
   
elif [[ $table_name == "int_nsn_call_usage" ]]
then
    job_name="EDP_OVCDR_${key_param_id}_GD_MD_INT_NSN_CALL"
   
elif [[ $table_name == "dom_nsn_in_usage" ]]
then
    job_name="EDP_OVCDR_${key_param_id}_GD_MD_DOM_NSN_IN"
   
else
    echo "Wrong table name provided"
    exit -1
fi

params="$(fn_get_ovcdr_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id" 
    exit -1
fi

source_date=$(echo $params | cut -d'~' -f1)
start_time=`date +"%Y-%m-%d %H:%M:%S"`
dataset_name=$(echo $params | cut -f 3 -d '~')
dataset_name=$(echo "$dataset_name" | tr '[:upper:]' '[:lower:]')
batch_id_dataset_name_dir_path=${BATCH_ID_DIR}/$dataset_name
current_batch_id_path="$batch_id_dataset_name_dir_path/current"
batch_id=`cat ${current_batch_id_path}`
log_file_path="${LOG_DIR_SUBJECT_AREA}/${job_name}.log"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}" 

##############################################################################
#																			 #
# Loading gold historic data                                                 #
#																			 #
##############################################################################
 
sh ${SUBJECT_AREA_HOME}/gold_metadata_${table_name}_tbl/bin/module.sh "prepare" "${log_file_path}" 
exit_code=$?   

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "Failed to load gold_metadata_${table_name} table historic data in gold layer" "${log_file_path}"
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "
fi 

fn_log_info "Successfully loaded gold_metadata_${table_name} table historic data in gold layer" "${log_file_path}"


##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "${record_count}" "${exit_code}" "${source_date}" "${log_file_path}" "" " "

##############################################################################
#                                    End                                     #
##############################################################################