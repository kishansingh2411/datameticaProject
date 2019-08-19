#!/bin/ksh

######################################################################################################################
# source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: unld_s3.ksh
#   Program type: Unix K Shell script
#   Purpose:    : The purpose of this script is to unload a table to s3
#   Author(s)   : DataMetica Team
#   Usage       : ksh unld_s3
#   Date        : 07/13/2016
#   Log File    : /app-am/logs/edp_offload/unld_s3.log.*
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
#     1.0     DataMetica Team          08/12/2016       Initial version
#
#
#####################################################################################################################

AUTODIR=/app-am
. $AUTODIR/edp_offload/conf/edp_offload.cfg
. $AUTODIR/conf/.auto_cfg

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

key_param_id=$1

# Variable assignment
export PROCESS_NAME=update_aws_key_params.ksh
export FW_LOG_PATH=${AUTODIR}/logs/edp_offload
export COMMON_SCRIPT_PATH=${AUTODIR}/common/bin

#Logfile
LOGFILE_NAME=update_aws_key_params.log
log_file_path=${FW_LOG_PATH}/${LOGFILE_NAME}

CURR_STAGE=1

date_fmt=`date +%Y%m%d%H%M%S`

##############################################################################
#                                                                            #                                                                                #
# Updating t_key_params table                                                #
#                                                                            #                                                                            #
##############################################################################

update_key_params=(`psql "host=$REDSHIFT_DBHOST port=$REDSHIFT_DBPORT dbname=$REDSHIFT_DBNAME \
   user=$REDSHIFT_DBUSER password=$REDSHIFT_DBPASS" -F --no-allign -q -A -t -c \
   "UPDATE T_KEY_PARAMS \
    SET \
       PARAM_DTM_START = DATEADD(H,COALESCE(INCREMENTAL_IN_HRS,0),PARAM_DTM_START), \
       PARAM_DTM_END = DATEADD(H,COALESCE(INCREMENTAL_IN_HRS,0),PARAM_DTM_END), \
       DTM_LAST_UPDATED = GETDATE() \
    WHERE KEY_PARAM_ID = ${key_param_id} ;"`)
status=$?

if [ ${status} -ne 0 ]
then
   fn_log_error "Error : Failed while updating t_key_params table for key_param ${key_param_id}. Exiting the program..." "${log_file_path}"
   echo "Error in $0 while updating t_key_params table for key_param ${key_param_id}." | mailx -s "Error in ${PROCESS_NAME} for key_param ${key_param_id}" $EMAIL_ERROR_LIST
   mv -f ${log_file_path} ${log_file_path}.${date_fmt}
   exit 1
fi

fn_log_info "successfully updated key_params for next run ." "${log_file_path}"


mv -f ${log_file_path} ${log_file_path}.${date_fmt}

echo "${PROCESS_NAME} executed successfully for Key_param_id ${key_param_id}." | mailx -s "${PROCESS_NAME} for ${key_param_id} completed on date : `date`" $EMAIL_SUCCESS_LIST

# exit with success
exit 0

##############################################################################
#                                    End                                     #
##############################################################################