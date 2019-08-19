#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: executor.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : Execute all steps one by one needed for Near Real-time DDP Ingestion Process
#   Author(s)   : DataMetica Team
#   Usage       : sh executor.sh 
#   Date        : 08/07/2016
#   Log File    : .../log/ddp/EXECUTOR.log
#   SQL File    : 
#   Error File  : .../log/ddp/EXECUTOR.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          08/07/2016       Initial version
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

PROJECT_HOME=`dirname ${SCRIPT_HOME}`

# Load all environment properties files
source $PROJECT_HOME/etc/beeline.properties
source $PROJECT_HOME/etc/camus-ddp.properties
source $PROJECT_HOME/etc/postgres.properties
source $PROJECT_HOME/bin/functions.sh
source $PROJECT_HOME/etc/ddp-rt.properties

#######################################################################################
#                                                                                     #
#   Providing permission and changing files from DOS to Unix format 		          #
#                                                                                     #
#######################################################################################   

chmod 755 -R $PROJECT_HOME
find $PROJECT_HOME/lib -type f ! \( -name '*.jar' \) -exec dos2unix {} \;
find $PROJECT_HOME/bin/* -type f -exec dos2unix {} \;
#find $PROJECT_HOME/etc/* -type f -exec dos2unix {} \;

##############################################################################
# Local Params				 								 	 			 #
##############################################################################

job_name="DDP_6000010_NEAR_RT_INGESTION"
start_time=`date +"%Y-%m-%d %H:%M:%S"`
log_file_path="${LOG_DIR}/${SUBJECT_AREA_NAME}/${job_name}.log"
current_batch_id_path="$BATCH_ID_DIR_SUBJECT_AREA/current"
source_date=`date +%Y-%m-%d:%H:%M:%S`

##############################################################################
#																			 #
# Creating local directories for                                             #
#      1. log file									                         #	
#	   2. logging batch Id							                         #
#																			 #
##############################################################################

echo "LOG_DIR : $LOG_DIR"
echo "SUBJECT_AREA_NAME : $SUBJECT_AREA_NAME"
echo "log_file_path : $log_file_path"
echo "BATCH_ID_DIR_SUBJECT_AREA : $BATCH_ID_DIR_SUBJECT_AREA"

fn_local_create_directory_if_not_exists "${LOG_DIR}/${SUBJECT_AREA_NAME}" "1" "${log_file_path}"

fn_local_create_directory_if_not_exists "${BATCH_ID_DIR_SUBJECT_AREA}" "1" "${log_file_path}"

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
      
   	  fn_update_job_statistics "${new_batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" " " " "
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

   fn_update_job_statistics "${new_batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" " " " "
fi
	
fn_log_info "Successfully generated new batch id is ${new_batch_id} and added it to ${current_batch_id_path} file" "${log_file_path}"

chmod 600 ${current_batch_id_path}


##############################################################################
#																			 #
# Execute Camus run                                                          #
#																			 #
##############################################################################

camus-run -D schema.registry.url=${SCHEMA_REGISTRY_URL} -P $PROJECT_HOME/etc/camus-ddp.properties
exit_code=$?

if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
then
 	fn_log_error "Failed while executing camus job" "${log_file_path}"
	fn_update_job_statistics "${new_batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "   
fi 

fn_log_info "Successfully executed camus job at ${source_date}" "${log_file_path}"
 
##############################################################################
#																			 #
# Hive Metastore check				 								 	 	 #
#																			 #
##############################################################################

echo $etl_destination_subdirs | sed -n 1'p' | tr ',' '\n' | while read table; 
do
	echo "table name : $HIVE_DATABASE_NAME_INCOMING.$table"
	beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true -e \
    "MSCK REPAIR TABLE $HIVE_DATABASE_NAME_INCOMING.$table;" 1>> "${log_file_path}" 2>> "${log_file_path}"
    exit_code=$?
    
    if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
	then
	 	fn_log_error "Failed while executing camus job" "${log_file_path}"
		fn_update_job_statistics "${new_batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "   
	fi
done

##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${new_batch_ids}" "${job_name}" "${start_time}" "" "${exit_code}" "${source_date}" "${log_file_path}" "" " "

##############################################################################
#                                    End                                     #
##############################################################################