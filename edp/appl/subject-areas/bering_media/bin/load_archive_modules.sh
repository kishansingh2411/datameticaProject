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
#                 Input Arguments for this script are: source_timestamp (201510150100).
#   Author(s)   : DataMetica Team
#   Usage       : sh load_archive_modules.sh "201510150100"
#   Date        : 12/28/2015
#   Log File    : .../log/bering_media/BERING_MEDIA_MOVE_TO_ARCHIVE_JOB.log
#   SQL File    : 
#   Error File  : .../log/bering_media/BERING_MEDIA_MOVE_TO_ARCHIVE_JOB.log
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

SUBJECT_AREA_HOME=`dirname ${SCRIPT_HOME}`
SUBJECT_AREAS_HOME=`dirname ${SUBJECT_AREA_HOME}`
HOME=`dirname ${SUBJECT_AREAS_HOME}`

###                                                                           
# Load all environment properties files
source $HOME/etc/namespace.properties
source $HOME/etc/default.env.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $HOME/etc/postgres.properties
source $HOME/bin/functions.sh

###############################################################################
#                                                                             #
# Checking number of parameter passed                                         #
#                                                                             #
###############################################################################

if [ "$#" -ne 1 ]
then
  echo "Illegal number of parameters"
  exit 
fi

##############################################################################
# Local Params				 								 	 			 #
##############################################################################

source_timestamp="$1"
source_data_date=`(echo $source_timestamp | cut -c1-8)`
source_data_hour=`(echo $source_timestamp | cut -c9,10)`
source_data_minutes=`(echo $source_timestamp | cut -c11,12)`
formatted_source_date=`date -d $source_data_date +%Y-%m-%d`
formatted_source_timestamp="$formatted_source_date $source_data_hour:$source_data_minutes:00"

current_batch_id_path="${BATCH_ID_DIR_SUBJECT_AREA}/current"
batch_id=`cat $current_batch_id_path`

job_name="BERING_MEDIA_MOVE_TO_ARCHIVE_JOB"
start_time=`date +"%Y-%m-%d %H:%M:%S"`
log_file_path="${LOG_DIR_SUBJECT_AREA}/${job_name}.log"
source_file_path="${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/beringmedia_dhcp_log_${source_timestamp}.dat"
source_file_name=`basename ${source_file_path}`

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

fn_hadoop_move_file_or_directory "${source_file_path}" \
   "${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}" \
   "${BOOLEAN_TRUE}" \
   "${BOOLEAN_FALSE}" \
   "${log_file_path}"
exit_code=$?   

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
   fn_log_error "Failed to archived "${source_file_name}" file at location ${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}" "${log_file_path}"
   fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" \
      "${formatted_source_timestamp}" "${log_file_path}" "${source_file_name}" " "
fi

fn_log_info "Successfully archived ${source_file_name} file at location ${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}" "${log_file_path}"

##############################################################################
#                                                                            #
# Deleting file from local directory                                         #
#                                                                            #
##############################################################################

fn_local_delete_file "${LOCAL_DATA_DIRECTORY}/${source_file_name}" "${BOOLEAN_FALSE}" "${log_file_path}" 
exit_code=$?

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
   fn_log_error "Failed to delete "${source_file_name}" file at location ${local_directory_path}" "${log_file_path}"
   fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" \
      "${formatted_source_timestamp}" "${log_file_path}" "${source_file_name}" " "
fi

fn_log_info "Successfully deleted ${source_file_name} file at location ${local_directory_path}" "${log_file_path}"

##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
   "${formatted_source_timestamp}" "${log_file_path}" "${source_file_name}" " "

##############################################################################
#                                    End                                     #
##############################################################################
