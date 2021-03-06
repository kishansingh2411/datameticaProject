#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: source_customer_data.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will Source Customer Data into incoming layer
#					Input Arguments for this script are: source_date (2015-12-28)
#   Author(s)   : DataMetica Team
#   Usage       : sh source_customer_data.sh "2015-12-28"
#   Date        : 12/28/2015
#   Log File    : .../log/ods/${job_name}.log
#   SQL File    : 
#   Error File  : .../log/ods/${job_name}.log
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
source $PROJECT_HOME/etc/beeline.properties
source $PROJECT_HOME/etc/default.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $SUBJECT_AREA_HOME/etc/oracle.properties
source $PROJECT_HOME/bin/functions.sh

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

source_date=`date --date="yesterday" +"%Y-%m-%d"`
start_time=`date +"%Y-%m-%d %H:%M:%S"`
job_name="ODS_600019_LD_CUSTOMER_DATA"
log_file_dir=${LOG_DIR_SUBJECT_AREA}/customer_onet_prod
batch_id_dir="$BATCH_ID_DIR_SUBJECT_AREA/customer_onet_prod"
current_batch_id_path="$batch_id_dir/current"
log_file_path=${log_file_dir}/${job_name}.log

##############################################################################
#																			 #
# Creating Local Log directory                                               #
#																			 #
##############################################################################

fn_local_create_directory_if_not_exists "${log_file_dir}" "${fail_on_error}" "${log_file_path}"

fn_local_create_directory_if_not_exists "${BATCH_ID_DIR_SUBJECT_AREA}" "${BOOLEAN_TRUE}" "${log_file_path}"

##############################################################################
#																			 #
# Initialization                                                             #
#																			 #
##############################################################################

export PGPASSWORD=$POSTGRES_PASSWORD;
new_batch_id=`psql -X -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USERNAME \
   -P t -P format=unaligned $POSTGRES_DATABASE_NAME \
   -c "select nextval('generate_seq_id')"`

if [ -z "$new_batch_id" ]
then
   fn_log_info "Failed while creating new BatchId for sourcing customer data!!!" "${log_file_path}"
   exit -1
else
   fn_log_info "Successfully created new BatchId [$new_batch_id] for sourcing customer data" "${log_file_path}"
fi

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

batch_id=`cat $current_batch_id_path`

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}" 

###############################################################################
#																			  #
#  Get customer data                                                          #
#																			  #
###############################################################################

tables=($SUBJECT_AREA_HOME/incoming_h_optimum_user_tbl)
for table in "${tables[@]}"
do  
   table_name=`basename ${table}`
   sh $table/bin/module.sh "prepare" "${log_file_path}"
   exit_code=$? 
    
   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
   then
      fn_log_error "Failed to source table "${table_name}" in incoming layer" "${log_file_path}"
      fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" \
         "${source_date}" "${log_file_path}" "NA" " "
   fi   

    record_count=`grep "Map output records=" ${log_file_path} | cut -d'=' -f2`
    fn_log_info "Successfully prepared ${table_name} in incoming layer" "${log_file_path}"
done

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "${record_count}" "${exit_code}" \
   "${source_date}" "${log_file_path}" "NA" " "

##############################################################################
#                                    End                                     #
##############################################################################