#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: generate_batch_id.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : Generating new Batch_id for the current process.                            
#                 Input Arguments for this script are: key_param_id.               
#   Author(s)   : DataMetica Team
#   Usage       : sh generate_batch_id.sh "key_param_id"
#   Date        : 04/12/2016
#   Log File    : .../log/channel_tuning/CHT_600008_BATCH_ID_GEN.log
#   SQL File    :                                 
#   Error File  : .../log/channel_tuning/CHT_600008_BATCH_ID_GEN.log
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
source $SUBJECT_AREA_HOME/etc/channel_tuning.properties
source $SUBJECT_AREA_HOME/bin/channel_tuning_functions.sh

##############################################################################
#                                                                            #
# Checking number of parameter passed                                        #
#                                                                            #
##############################################################################

if [ "$#" -ne 1 ] 
then
  echo "Illegal number of parameters: Parameter expected are : key_param_id"
  exit -1
fi

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

key_param_id="$1"
job_name="CHN_"$key_param_id"_BATCH_ID_GEN"

params="$(fn_get_channel_tuning_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id" 
    exit -1
fi

source_date=$(echo $params | cut -f 1 -d '~'| cut -f 1 -d ' ')
start_time=`date +"%Y-%m-%d %H:%M:%S"`
current_batch_id_path="$BATCH_ID_DIR_SUBJECT_AREA/current"
log_file_dir="${LOG_DIR}/${SUBJECT_AREA_NAME}"
log_file_path="${log_file_dir}/${job_name}.log"

##############################################################################
#																			 #
# Creating local directories for                                             #
#      1. log file									                         #	
#	   2. logging batch Id							                         #
#																			 #
##############################################################################

fn_local_create_directory_if_not_exists "${log_file_dir}" "${BOOLEAN_TRUE}" "${log_file_path}"

fn_local_create_directory_if_not_exists "${BATCH_ID_DIR_SUBJECT_AREA}" "${BOOLEAN_TRUE}" "${log_file_path}"

##############################################################################
#																			 #
# Initialization                                                             #
#																			 #
##############################################################################

export PGPASSWORD=$POSTGRES_PASSWORD;
new_batch_id=`psql -X -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USERNAME \
              -P t -P format=unaligned $POSTGRES_DATABASE_NAME -c "select nextval('generate_seq_id')" 2>> ${log_file_path}`

if [ -z "$new_batch_id" ]
then
   fn_log_error "Failed while creating new BatchId!!!" "${log_file_path}"
   exit -1
fi
  
fn_log_info "Successfully created new BatchId [$new_batch_id]" "${log_file_path}"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${new_batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}"
  
##############################################################################
#																			 #
# Removing Last batchId if presents 						     			 #
#																			 #
##############################################################################

if [ -e $current_batch_id_path ]
then
   rm  $current_batch_id_path
   exit_code=$?

   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
   then
      fn_log_error "Failed to delete Batch_id, Quitting the process" "${log_file_path}"
      
   	  fn_update_job_statistics "${new_batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
   	     "${source_date}" "${log_file_path}" " " " "
   fi
   
   fn_log_info "Deleted existing batchId !!!" "${log_file_path}"
fi

##############################################################################
#																			 #
# Registering new BatchId 						 							 #
#																			 #
##############################################################################

echo "${new_batch_id}" > "${current_batch_id_path}"
exit_code=$?

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
   fn_log_error "Failed to write new batch id ${new_batch_id} to ${current_batch_id_path} file" "${log_file_path}"

   fn_update_job_statistics "${new_batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
      "${source_date}" "${log_file_path}" " " " "
fi
	
fn_log_info "Successfully generated new batch id is ${new_batch_id} and added it to ${current_batch_id_path} file" "${log_file_path}"

chmod 600 ${current_batch_id_path}

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_update_job_statistics "${new_batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" " " " "

##############################################################################
#                                    End                                     #
##############################################################################