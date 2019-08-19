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
#   Log File    : .../log/${suite_name}/${job_name}.log
#   SQL File    : 
#   Error File  : .../log/${suite_name}/${job_name}.log
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
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/etc/omniture.properties
source $SUBJECT_AREA_HOME/bin/omniture_functions.sh

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
suite_data_file_path=$SUBJECT_AREA_HOME/metadata/Optimum_Suite.txt

params="$(fn_get_omniture_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

source_date=$(echo $params | cut -f 1 -d '~'| cut -f 1 -d ' ')
suite_name=$(echo $params | cut -f 2 -d '~')
suite_name=$(echo "$suite_name" | tr '[:upper:]' '[:lower:]')


if [ $suite_name == "$CHANNEL900" ]
then
 	job_name="STK36_600005_ARCHIVE_SRC_FILE" 
elif [ $suite_name == "$ONET_PROD" ]
then
   	job_name="ONET_600009_ARCHIVE_SRC_FILE"
elif [ $suite_name == "cablevis-uow-com" ]
then
   job_name="UOW_600002_ARCHIVE_SRC_FILE"
else
	echo "Invalid suite_name provided"
	exit
fi


start_time=`date +"%Y-%m-%d %H:%M:%S"`
current_batch_id_path="${BATCH_ID_DIR}/$suite_name/current"
batch_id=`cat $current_batch_id_path`
executor_log_file=${LOG_DIR_SUBJECT_AREA}/${suite_name}/EXECUTOR_${key_param_id}.log
log_file_path="${LOG_DIR_SUBJECT_AREA}/${suite_name}/${job_name}.log"

##############################################################################
#																			 #
# Checking mandatory Params                                                  #
#																			 #
##############################################################################

fn_check_mandatory_params "suite_name,$suite_name"
  
##############################################################################
#                                                                            #
# Checking Correct Suite Name                                                #
#                                                                            #
##############################################################################

fn_check_suite_name "$suite_name"
  
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

source_file_path=`hadoop fs -ls ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/$suite_name/*$source_date.tar* | awk '{print$8}'`
source_file_name=`basename $source_file_path`

##############################################################################
#																			 #
# create hadoop directory                                                    #
#																			 #
##############################################################################

target_dir="${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}/${suite_name}"

fn_hadoop_create_directory_if_not_exists "${target_dir}" "${BOOLEAN_FALSE}" "${log_file_path}"
exit_code=$?   

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "Failed to create hdfs directory "${target_dir}"  file, Quitting the process" "${log_file_path}"
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" \
	  "${source_date}" "${log_file_path}" "${source_file_name}" " "
fi
 
fn_log_info "Successfully created hdfs directory "${target_dir}" " "${log_file_path}"

##############################################################################
#																			 #
# Move To archive layer                                                      #
#																			 #
##############################################################################

fn_hadoop_move_file_or_directory "${source_file_path}" \
   "${target_dir}" \
   "${BOOLEAN_TRUE}" \
   "${BOOLEAN_FALSE}" \
   "${log_file_path}"
exit_code=$?   

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "Failed to archived "${source_file_name}" file, Quitting the process" "${log_file_path}"
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" \
	  "${source_date}" "${log_file_path}" "${source_file_name}" " "
fi

fn_log_info "Successfully archived ${source_file_name} file at location ${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}" "${log_file_path}"

##############################################################################
#                                                                            #
# Update Key Params Table                                                    #
#                                                                            #
##############################################################################

fn_update_omniture_params ${key_param_id}
exit_code=$?

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "Failed to update Key Params table, Quitting the process" "${log_file_path}"
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" \
	  "${source_date}" "${log_file_path}" "${source_file_name}" " "
fi

fn_log_info "Successfully updated Key Params table" "${log_file_path}"

##############################################################################
#                                                                            #
# Deleting file from local directory                                         #
#                                                                            #
##############################################################################



hadoop fs -test -e ${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}/${suite_name}/${source_file_name}
exit_code=$?

if [ $exit_code == ${EXIT_CODE_SUCCESS} ]
then
   fn_local_delete_file "${LOCAL_DATA_DIRECTORY}/${source_file_name}" \
      "${BOOLEAN_FALSE}" \
      "${log_file_path}"
   exit_code=$?

   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
   then
       fn_log_error "Failed to delete "${source_file_name}" file at location ${LOCAL_DATA_DIRECTORY}, quitting the process" "${log_file_path}"
       fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" \
	   "${source_date}" "${log_file_path}" "${source_file_name}" " "
   fi
fi

fn_log_info "Successfully deleted ${source_file_name} file at location ${LOCAL_DATA_DIRECTORY}" "${log_file_path}"
   
##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
   "${source_date}" "${log_file_path}" "${source_file_name}" " "

##############################################################################
#                                    End                                     #
##############################################################################