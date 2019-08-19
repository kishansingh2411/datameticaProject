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
#   Purpose:    : This will move source file and control file into archive directory.
#				  It will  also update the key-param table 	
#                 Input Arguments for this script is: key_Param_id.
#   Author(s)   : DataMetica Team
#   Usage       : sh load_archive_modules.sh key_Param_id
#   Date        : 12/15/2016
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
#    1.0     DataMetica Team          12/15/2016       Initial version
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
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/etc/ovcdr.properties
source $SUBJECT_AREA_HOME/bin/ovcdr_functions.sh

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
params="$(fn_get_ovcdr_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

source_date=$(echo $params | cut -f 1 -d '~'| cut -f 1 -d ' ')
source_date_format=`echo "${source_date//-/}"`

dataset_name=$(echo $params | cut -f 3 -d '~')
dataset_name=$(echo "$dataset_name" | tr '[:upper:]' '[:lower:]')
job_name="EDP_${key_param_id}_ARCHIVE_SRC_FILE" 
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
# create hadoop directory                                                    #
#																			 #
##############################################################################

file_prefix=`echo ${dataset_name} | tr '[:lower:]' '[:upper:]'`"_${source_date_format}"

source_file_path=`ls ${LOCAL_DATA_DIRECTORY}/${file_prefix}*.gz`
source_file_name=`basename $source_file_path`

control_file_path=`ls ${LOCAL_DATA_DIRECTORY}/${file_prefix}*.ctl`
control_file_name=`basename $control_file_path`

target_dir="${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}/${dataset_name}"

fn_hadoop_create_directory_if_not_exists "${target_dir}" "${BOOLEAN_FALSE}" "${log_file_path}"
exit_code=$?   

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "Failed to create HDFS directory "${target_dir}", Quitting the process" "${log_file_path}"
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name},${control_file_name}" " "
fi
 
fn_log_info "Successfully created HDFS directory "${target_dir}" " "${log_file_path}"

##############################################################################
#																			 #
# Move Data file To archive layer                                            #
#																			 #
##############################################################################

fn_copy_from_local "${source_file_path}" "${target_dir}" "${BOOLEAN_TRUE}" "${BOOLEAN_FALSE}" "${log_file_path}"
exit_code=$?   

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "Failed to archived source data file "${source_file_name}", Quitting the process" "${log_file_path}"
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name},${control_file_name}" " "
fi

fn_log_info "Successfully archived source data file ${source_file_name} at location ${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}" "${log_file_path}"

##############################################################################
#																			 #
# Move Control file To archive layer                                         #
#																			 #
##############################################################################

fn_copy_from_local "${control_file_path}" "${target_dir}" "${BOOLEAN_TRUE}" "${BOOLEAN_FALSE}" "${log_file_path}"
exit_code=$?   

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "Failed to archived control file "${control_file_name}", Quitting the process" "${log_file_path}"
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name},${control_file_name}" " "
fi

fn_log_info "Successfully archived control file ${source_file_name} at location ${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}" "${log_file_path}"

##############################################################################
#                                                                            #
# Update Key Params Table                                                    #
#                                                                            #
##############################################################################

fn_update_ovcdr_params ${key_param_id}
exit_code=$?

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "Failed to update Key Params table, Quitting the process" "${log_file_path}"
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name},${control_file_name}" " "
fi

fn_log_info "Successfully updated Key Params table for key param id ${key_param_id}" "${log_file_path}"

##############################################################################
#                                                                            #
# Deleting source data file from local directory                             #
#                                                                            #
##############################################################################

hadoop fs -test -e ${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}/${dataset_name}/${source_file_name}
exit_code=$?

if [ $exit_code == ${EXIT_CODE_SUCCESS} ]
then
   fn_local_delete_file "${LOCAL_DATA_DIRECTORY}/${source_file_name}" "${BOOLEAN_FALSE}" "${log_file_path}"
   exit_code=$?

   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
   then
       fn_log_error "Failed to delete source data file "${source_file_name}" at location ${LOCAL_DATA_DIRECTORY}, quitting the process" "${log_file_path}"
       fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name},${control_file_name}" " "
   fi
fi

fn_log_info "Successfully deleted source data file ${source_file_name} at location ${LOCAL_DATA_DIRECTORY}" "${log_file_path}"

##############################################################################
#                                                                            #
# Deleting Control file from local directory                                 #
#                                                                            #
##############################################################################

hadoop fs -test -e ${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}/${dataset_name}/${control_file_name}
exit_code=$?

if [ $exit_code == ${EXIT_CODE_SUCCESS} ]
then
   fn_local_delete_file "${LOCAL_DATA_DIRECTORY}/${control_file_name}" "${BOOLEAN_FALSE}" "${log_file_path}"
   exit_code=$?

   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
   then
       fn_log_error "Failed to delete control file "${source_file_name}" at location ${LOCAL_DATA_DIRECTORY}, quitting the process" "${log_file_path}"
       fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name},${control_file_name}" " "
   fi
fi

fn_log_info "Successfully deleted control file ${source_file_name} at location ${LOCAL_DATA_DIRECTORY}" "${log_file_path}"
   
##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name},${control_file_name}" " "

##############################################################################
#                                    End                                     #
##############################################################################
