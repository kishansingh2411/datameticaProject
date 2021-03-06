#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_archive_modules.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will move source file into archive directory
#                 Input Arguments for this script are: "key_param_id"
#   Author(s)   : DataMetica Team
#   Usage       : sh load_archive_modules.sh "key_param_id"
#   Date        : 01/30/2017
#   Log File    : .../log/ivr/${job_name}.log
#   SQL File    : 
#   Error File  : .../log/ivr/${job_name}.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          01/30/2017       Initial version
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
source $SUBJECT_AREA_HOME/etc/ivr.properties
source $HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/ivr_functions.sh

###############################################################################
#                                                                             #
# Checking number of parameter passed                                         #
#                                                                             #
###############################################################################

if [ "$#" -ne 1 ]
then
  echo "Illegal number of parameters.  Parameters expected [key_param_id]"
  exit 
fi

##############################################################################
# Local Params				 								 	 			 #
##############################################################################

key_param_id="$1"

params="$(fn_get_ivr_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

source_date=$(echo $params | cut -f 1 -d '~'| cut -f 1 -d ' ')
dataset_name=$(echo $params | cut -f 3 -d '~')
dataset_name=$(echo "$dataset_name" | tr '[:upper:]' '[:lower:]')
start_time=`date +"%Y-%m-%d %H:%M:%S"`
job_name="EDP_IVR_${key_param_id}_LD_ARCH"
batch_id_dataset_name_dir_path=${BATCH_ID_DIR}/$dataset_name
current_batch_id_path="$batch_id_dataset_name_dir_path/current"
batch_id=`cat $current_batch_id_path`
log_file_dir="${LOG_DIR_SUBJECT_AREA}"
log_file_path="${log_file_dir}/${job_name}.log"
formatted_source_date=`echo $source_date | tr -d -`
source_file_path=`ls -lrt "${LOCAL_DATA_DIRECTORY}/${SUBJECT_AREA_NAME}/v.CDR.log."*".${formatted_source_date}" | awk -F" " '{print$9}'`
source_file_name=v.CDR.log.${formatted_source_date}

if [ -z "$source_file_path" ]
then
 echo "There is no file for the date $formatted_source_date present on local"
 fn_log_info "There is no file for the date $formatted_source_date present on local" "${log_file_path}"
 exit -1
fi

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}" 

##############################################################################
#																			 #
# Archiving Source files                                                     #
#																			 #
##############################################################################

for source_file in ${source_file_path[@]}
do
	fn_copy_from_local_to_archive "${source_file}" \
	   "${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}" \
	   "${BOOLEAN_TRUE}" \
	   "${BOOLEAN_FALSE}" \
	   "${log_file_path}"
	exit_code=$?   
	if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	then
	   fn_log_error "Failed to archived "${source_file}" file at location ${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}" "${log_file_path}"
	   fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file}" " "
	fi
	
	fn_log_info "Successfully archived ${source_file} file at location ${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}" "${log_file_path}"

##############################################################################
#                                                                            #
# Deleting file from local directory                                         #
#                                                                            #
##############################################################################

	source_file_name=`basename ${source_file}`
	fn_local_delete_file "${LOCAL_DATA_DIRECTORY}/${SUBJECT_AREA_NAME}/${source_file_name}" "${BOOLEAN_FALSE}" "${log_file_path}" 
	exit_code=$?
	if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	then
	   fn_log_error "Failed to delete "${source_file_name}" file at location ${local_directory_path}" "${log_file_path}"
	   fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name}" " "
	fi
	
	fn_log_info "Successfully deleted ${source_file_name} file at location ${local_directory_path}" "${log_file_path}"

done
##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name}" " "

##############################################################################
#                                    End                                     #
##############################################################################