#!/bin/ksh

######################################################################################################################
# source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_500600_archive_data_redshift_to_s3.ksh
#   Program type: Unix K Shell script
#   Purpose:    : The purpose of this script is to unload a dimension table to s3
#   Author(s)   : DataMetica Team
#   Usage       : ksh load_500600_archive_data_redshift_to_s3.ksh $job_id $key_param
#   Date        : 07/13/2016
#   Log File    : /app-am/logs/edp_offload/load_500600_archive_data_redshift_to_s3.log.*
#   SQL File    :
#   Error File  :
#   Dependency  :
#   Disclaimer  :
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          08/12/2016       Initial version
#
#
#####################################################################################################################

AUTODIR=/app-am
. $AUTODIR/edp_offload/conf/edp_offload.cfg
. $AUTODIR/conf/.auto_cfg

ACCESS_KEY=$AWS_ACCESS_KEY_ID
SECRET_KEY=$AWS_SECRET_ACCESS_KEY
S3_BUCKET=$S3PATH/edp_offload
REDSHFT_USER=$CVRSAM_CVBMGR_USER
REDSHFT_DATABASE=$CVRSAM_CVBMGR_DATABASE
REDSHFT_PASS=$CVRSAM_CVBMGR_PASS
REDSHFT_HOST=$CVRSAM_CVBMGR_HOST
REDSHFT_PORT=$CVRSAM_CVBMGR_PORT

##############################################################################
#                                                                            #
# functions                                                                  #
#                                                                            #
##############################################################################

fn_log()
{
  message="$1"
  log_file="$2"
  echo "${message}"  >> ${log_file}
}

fn_log_info()
{
  message="$1"
  log_file="$2"
  fn_log "INFO ${message}" "${log_file}"
}
fn_log_error()
{
  message="$1"
  log_file="$2"
  fn_log "ERROR ${message}" "${log_file}"
}

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

job_id=$1
key_param_id=$2

# Variable assignment
export PROCESS_NAME=load_500600_archive_data_redshift_to_s3.ksh
export FW_LOG_PATH=${AUTODIR}/logs/edp_offload
export COMMON_SCRIPT_PATH=${AUTODIR}/common/bin

#Logfile
LOGFILE_NAME=load_500600_archive_data_redshift_to_s3_${job_id}.log
log_file_path=${FW_LOG_PATH}/${LOGFILE_NAME}

CURR_STAGE=1

date_fmt=`date +%Y%m%d%H%M%S`

##############################################################################
#																			 #
# Getting parameters from job master                		 	 			 #
#																			 #
##############################################################################

##  Validating passed AWS_JOB_ID from Primary range >= MIN_JOB_ID and <= MAX_JOB_ID
aws_job_master_params=(`psql "host=$REDSHFT_HOST port=$REDSHFT_PORT dbname=$REDSHFT_DATABASE \
   user=$REDSHFT_USER password=$REDSHFT_PASS" -F --no-allign -q -A -t -c \
   "SELECT nvl(AWS_JOB_ID,0) || '~' || coalesce(TARGET_BUCKET_DIR) FROM AWS_JOB_MASTER WHERE AWS_JOB_ID = ${job_id} \
   and AWS_JOB_ID >= ${MIN_JOB_ID} and AWS_JOB_ID <= ${MAX_JOB_ID};"`)
status=$?

if [ -z ${aws_job_master_params} ]
then
   fn_log_error "Error : Failed while validating from AWS_JOB_MASTER table. Exiting the program..." "${log_file_path}"
   echo "Error in $0 while validating ${job_id} JOB_ID with AWS_JOB_MASTER." | mailx -s "Error in ${PROCESS_NAME} for ${job_id}" $EMAIL_ERROR_LIST
   mv -f ${log_file_path} ${log_file_path}.${date_fmt}
   exit 1
fi

fn_log_info "successfully retrieved aws_job_master params." "${log_file_path}"

AWS_JOB_ID=`echo ${aws_job_master_params} | cut -d'~' -f1`
table_name=`echo ${aws_job_master_params} | cut -d'~' -f2 | cut -d'/' -f2 | tr '[:upper:]' '[:lower:]'`

if [ ! ${AWS_JOB_ID} -ge ${MIN_JOB_ID} ] && [ ${AWS_JOB_ID} -le ${MAX_JOB_ID} ]
then
   fn_log_error "Error : Invalid ${job_id} job_id. Exiting the program...1" "${log_file_path}"
   echo "Error in $0, No entry found for table ${table_name} in AWS_JOB_MASTER." | mail -s "Error in ${PROCESS_NAME} for ${table_name}" $EMAIL_ERROR_LIST
   mv -f ${log_file_path} ${log_file_path}.${date_fmt}
   exit 1
fi

fn_log_info "Value in AWS_JOB_ID: $AWS_JOB_ID" "${log_file_path}"

##############################################################################
#																			 #
# Deleting existing S3 directories for table                            	 #
#																			 #
##############################################################################

s3cmd --access_key="${ACCESS_KEY}" --secret_key="${SECRET_KEY}" rm ${S3_BUCKET}/${table_name}/ --recursive
exit_code=$?

if [ ! ${exit_code} == 0 ]
then
   fn_log_error "Failed while deleting the directories (${S3_BUCKET}/${table_name}/) in S3 bucket." "${log_file_path}"
   echo "Error in $0, Failed while deleting the directories (${S3_BUCKET}/${table_name}/) in S3 bucket" | mail -s "Error in ${PROCESS_NAME} for ${table_name}" $EMAIL_ERROR_LIST
   mv -f ${log_file_path} ${log_file_path}.${date_fmt}
   exit -1
fi

fn_log_info "Successfully deleted directory ${S3_BUCKET}/${table_name}/ " "${log_file_path}"

##############################################################################
#																			 #
# Copy data from redshift to s3 bucket                                  	 #
#																			 #
##############################################################################

case ${table_name} in
   *_d_*)

      fn_log_info  "Starting the DTU  for dimension table at `date +"%Y-%m-%d_%H:%M:%S"`" "${log_file_path}"

      ## Invoking offload job for dimension table ${table_name}
      ${FRAMEWORK_DIR}/aws_master_shell.ksh $job_id 1>>"${log_file_path}" 2>> "${log_file_path}"
      status=$?

      fn_log_info "DTU finished for dimension table at `date +"%Y-%m-%d_%H:%M:%S"`" "${log_file_path}"

   ;;
esac

if [ $status -ne 0 ]
then
   fn_log_info "Error : DTU occured in stage $CURR_STAGE. Exiting the program...5" "${log_file_path}"
   echo "Error in $0, Failed while executing data transport utility for Process ${PROCESS_NAME} for table ${table_name} on date : `date +"%Y-%m-%d_%H:%M:%S"`." | mailx -s "Failed Process ${PROCESS_NAME} for ${table_name}" $EMAIL_ERROR_LIST
   mv -f ${log_file_path} ${log_file_path}.${date_fmt}
   exit 1
fi

fn_log_info "End success $0 for date : `date +"%Y-%m-%d_%H:%M:%S"`" "${log_file_path}"
mv -f ${log_file_path} ${log_file_path}.${date_fmt}
echo "Process ${PROCESS_NAME} executed successfully for table ${table_name} on date : `date +"%Y-%m-%d_%H:%M:%S"`." | mailx -s "Completed Process ${PROCESS_NAME} for ${table_name}" $EMAIL_SUCCESS_LIST

# exit with success
exit 0

##############################################################################
#                                    End                                     #
##############################################################################