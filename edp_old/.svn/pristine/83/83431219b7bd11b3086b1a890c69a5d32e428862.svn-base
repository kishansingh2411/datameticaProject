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
#                 Input Arguments for this script is: key_Param_id.
#   Author(s)   : DataMetica Team
#   Usage       : sh load_archive_modules.sh key_Param_id
#   Date        : 10/03/2016
#   Log File    : .../log/hulu/HULU_600003_ARCHIVE_SRC_FILE.log
#   SQL File    : 
#   Error File  : .../log/hulu/HULU_600003_ARCHIVE_SRC_FILE.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          12/28/2015       Initial version
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
source $PROJECT_HOME/etc/default.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/etc/hulu.properties
source $SUBJECT_AREA_HOME/bin/hulu_functions.sh

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
#																			 #
#   Local Param 				 			     						     #
#                                    										 #
##############################################################################

key_param_id="$1"
job_name="HULU_600003_ARCHIVE_SRC_FILE"

params="$(fn_get_hulu_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

source_date=$(date -d "$params" +%Y%m%d)

start_time=`date +"%Y-%m-%d %H:%M:%S"`
current_batch_id_path="${BATCH_ID_DIR}/hulu/current"
batch_id=`cat $current_batch_id_path`

log_file_path="${LOG_DIR_SUBJECT_AREA}/hulu/${job_name}.log"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}"

##############################################################################
#																			 #
# Move To archive layer                                                      #
#																			 #
##############################################################################

source_tgz_file_path=`hadoop fs -ls ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/HULU_OTT-${source_date}*.tgz | awk '{print$8}'`
source_tgz_file_name=`basename $source_tgz_file_path`
source_ctrl_file_path=`hadoop fs -ls ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/HULU_OTT-${source_date}*.ctrl | awk '{print$8}'`
source_ctrl_file_name=`basename ${source_ctrl_file_path}`

fn_hadoop_move_file_or_directory "${source_tgz_file_path}" \
   "${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}" \
   "${BOOLEAN_TRUE}" \
   "${BOOLEAN_FALSE}" \
   "${log_file_path}"
exit_code1=$?   

fn_hadoop_move_file_or_directory "${source_ctrl_file_path}" \
   "${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}" \
   "${BOOLEAN_TRUE}" \
   "${BOOLEAN_FALSE}" \
   "${log_file_path}"
exit_code2=$?   

if [[ ${exit_code1} -eq $EXIT_CODE_SUCCESS && ${exit_code2} -eq $EXIT_CODE_SUCCESS ]]
then
	fn_log_info "Successfully archived both files at location ${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}" "${log_file_path}"
else
	fn_log_error "Failed to archived both files or one of the at location ${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}" "${log_file_path}" 
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${EXIT_CODE_FAIL}" \
	      "${source_date}" "${log_file_path}" "${source_tgz_file_name}" " "
fi

fn_update_hulu_params ${key_param_id}
exit_code=$?

if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
then
   fn_log_error "Failed while updating Key_params table" "${log_file_path}"
   fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "   
fi

fn_log_info "Successfully updated key params table" "${log_file_path}"

##############################################################################
#                                                                            #
# Deleting file from local directory                                         #
#                                                                            #
##############################################################################

hadoop fs -test -e ${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}/${source_tgz_file_name}
exit_code1=$?

hadoop fs -test -e ${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}/${source_ctrl_file_name}
exit_code2=$?

if [[ ${exit_code1} -eq $EXIT_CODE_SUCCESS && ${exit_code2} -eq $EXIT_CODE_SUCCESS ]]
then
	fn_local_delete_file "${LOCAL_DATA_DIRECTORY}/${source_tgz_file_name}" \
	"${BOOLEAN_FALSE}" \
	"${log_file_path}"
	exit_code1=$?

	fn_local_delete_file "${LOCAL_DATA_DIRECTORY}/${source_ctrl_file_name}" \
	"${BOOLEAN_FALSE}" \
	"${log_file_path}"
	exit_code2=$?
	
	if [[ ${exit_code1} -eq $EXIT_CODE_SUCCESS && ${exit_code2} -eq $EXIT_CODE_SUCCESS ]]
	then
		fn_log_info "Successfully deleted both files at location ${LOCAL_DATA_DIRECTORY}" "${log_file_path}"		
	else
	    fn_log_error "Failed to delete both files or one of the file at location ${LOCAL_DATA_DIRECTORY}" "${log_file_path}"
		fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" \
		"${source_date}" "${log_file_path}" "${source_tgz_file_name}" " "
	fi
else
    fn_log_error "Both files or one of the file does not exists in archive layer" "${log_file_path}"
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${EXIT_CODE_FAIL}" \
	"${source_date}" "${log_file_path}" "${source_tgz_file_name}" " "
fi

##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
   "${source_date}" "${log_file_path}" "${source_tgz_file_name}" " "

##############################################################################
#                                    End                                     #
##############################################################################
