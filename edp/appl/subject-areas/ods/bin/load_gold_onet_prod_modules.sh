#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_gold_h_optimum_user.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will remove duplicates present in incoming_h_optimum_user and load gold_h_optimum_user
#					Input Arguments for this script are: source_date (2015-12-28)
#   Author(s)   : DataMetica Team
#   Usage       : sh load_gold_h_optimum_user.sh "2015-12-28"
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
source $SUBJECT_AREA_HOME/etc/ods.properties

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

source_date=`date --date="yesterday" +"%Y-%m-%d"`
start_time=`date +"%Y-%m-%d %H:%M:%S"`
job_name="ODS_600019_GOLD_CUSTOMER_DATA"
log_file_dir=${LOG_DIR_SUBJECT_AREA}/customer_onet_prod
batch_id_dir="$BATCH_ID_DIR_SUBJECT_AREA/customer_onet_prod"
current_batch_id_path="$batch_id_dir/current"
log_file_path=${log_file_dir}/${job_name}.log
BATCH_ID=`cat ${current_batch_id_path}`

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}" 

###############################################################################
#																			  #
#  Get customer data                                                          #
#																			  #
###############################################################################

table_name=${GOLD_H_OPTIMUM_USER_TBL}
sh $SUBJECT_AREA_HOME/${table_name}_tbl/bin/module.sh "transform" "${log_file_path}" "${source_date}"
exit_code=$?
    
if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
   fn_log_error "Failed to transform table "${table_name}" in gold layer, Quitting the process" "${log_file_path}" 

   fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" \
      "${source_date}" "${log_file_path}" "NA" " " 
fi   

record_count=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
             "SELECT COUNT(*) FROM $HIVE_DATABASE_NAME_GOLD.${HIVE_TABLE_PREFIX}${table_name} ;"`

fn_log_info "Successfully transform ${table_name} in gold layer" "${log_file_path}"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "${record_count}" "${exit_code}" \
   "${source_date}" "${log_file_path}" "NA" " "
   
   

##############################################################################
#                                    End                                     #
##############################################################################