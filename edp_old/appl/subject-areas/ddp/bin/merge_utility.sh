#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_incoming_modules.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will load data in all Incoming tables.
#                 Input Arguments for this script are: "key_param_id"              
#   Author(s)   : DataMetica Team
#   Usage       : Merge_utility.sh "key_param_id" 
#   Date        : 05/02/2016
#   Log File    : .../log/ddp/EDP_DDP_600004_MERGR_UTIL.log
#   SQL File    : 
#   Error File  : .../log/ddp/EDP_DDP_600004_MERGR_UTIL.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          05/02/2016       Initial version
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
HOME=`dirname ${SUBJECT_AREAS_HOME}`

###                                                                           
# Load all environment properties files
source $HOME/etc/namespace.properties
source $HOME/etc/beeline.properties
source $HOME/etc/default.env.properties
source $HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $SUBJECT_AREA_HOME/etc/ddp.properties
source $SUBJECT_AREA_HOME/etc/oracle.properties

source $HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/ddp_functions.sh

###############################################################################
#                                                                             #
# Checking number of parameter passed                                         #
#                                                                             #
###############################################################################

if [ "$#" -ne 1 ] 
then
   echo "Illegal number of parameters. Parameters expected are [ "key_param_id" "table_name" "table_type" ]"
   exit -1
fi

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

key_param_id="$1"
job_name="EDP_DDP_600004_MERGR_UTIL"

log_file_path="${LOG_DIR}/${SUBJECT_AREA_NAME}/${job_name}.log"

#params="$(fn_get_ddp_params ${key_param_id})"


export PGPASSWORD=${POSTGRES_PASSWORD};
params=`psql -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USERNAME} \
            -P t -P format=unaligned ${POSTGRES_DATABASE_NAME} \
            -c "SELECT COALESCE(PARAM1,'all')|| '~' || COALESCE(PARAM2,TO_CHAR((CURRENT_DATE - (INTERVAL '1 MONTH')),'YYYY-MM-DD')) FROM KEY_PARAMS  \
                WHERE KEY_PARAM_ID = $key_param_id;"`
exit_code=$?

if [ -z "$params" ]
then
    fn_log_error "Invalid Key-Param Id found : $key_param_id" ${log_file_path}
    exit -1
fi

load_date=$(echo $params | cut -d'~' -f2)

if [ -z ${load_date} ]
then
   load_date=`date  --date="last month" "+%Y-%m-%d"`
fi

load_year=`echo ${load_date} | cut -c 1-4`
load_month=`echo ${load_date} | cut -c 6-7`
load_day=`echo ${load_date} | cut -c 9-10`
table_type=$(echo $params | cut -d'~' -f1)
start_time=`date +"%Y-%m-%d %H:%M:%S"`
fn_get_current_batch_id

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}"

if [ "${table_type}" == "all" ]
then
   table_list_sql="SELECT SUBSTRING(TABLE_NAME FROM 5)|| '~' ||TABLE_TYPE FROM ${DDP_AUDIT} WHERE TABLE_NAME NOT LIKE '%controlparams%';"
else
   table_list_sql="SELECT SUBSTRING(TABLE_NAME FROM 5)|| '~' ||TABLE_TYPE FROM ${DDP_AUDIT} WHERE table_type = '${table_type}';"
fi

table_list_arr=$(psql -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USERNAME} \
    -P t -P format=unaligned ${POSTGRES_DATABASE_NAME} -c "${table_list_sql};")

while read -r table
do
   table_name="incoming"_$(echo $table | cut -d'~' -f1)
   table_type=$(echo $table | cut -d'~' -f2)

   if [ "${table_type}" == "${MEDIUM_TBL}" ]
   then
      partition_cols="load_year,load_month"
      where_condition="load_year='${load_year}' and load_month='${load_month}'"
   elif [ "${table_type}" == "${LARGE_TBL}" ]
   then
      partition_cols="load_year,load_month,load_day"
      where_condition="load_year='${load_year}' and load_month='${load_month}' and load_day='${load_day}'"
   fi

##############################################################################
#																			 #
# Merging                                                                    #
#																			 #
##############################################################################

   beeline -u ${HIVESERVER2_URL} \
      -n ${HIVE_USENAME} \
      -p ${HIVE_PASSWORD} \
      -hivevar table_prefix=${HIVE_TABLE_PREFIX} \
      -hivevar target_table="${table_name}" \
      -hivevar target_database=${HIVE_DATABASE_NAME_INCOMING} \
      -hivevar where_condition="${where_condition}" \
      -hivevar partition_columns=${partition_cols} \
      -f ${HOME}/utils/hive/data_merge.hql 1>> "${log_file_path}" 2>> "${log_file_path}"
   exit_code=$?
   
   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
     then
        fn_log_error "Failed to merged files for ${table_name}" "${log_file_path}"
        fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${load_date}" "${log_file_path}" "" " "
   fi
   
   fn_log_info "Successfully executed merge util for ${table_name}" "${log_file_path}"
   
done <<< "${table_list_arr}"

###
#  Setting key_params table for next run
#  1. For ongoing process, it param2 will be set to NULL
#  2. For historical, param2 will be set to load_date + 1 month
###

next_run_date=`date "--date="${load_date}" +1 month" +%Y-%m-%d`
curr_date=`date +%Y-%m-%d`

if [ "${next_run_date}" == "${curr_date}" ]
then
   param2=NULL
else
   param2="'`date "--date="${load_date}" +1 month" +%Y-%m-%d`'"
fi

status=`psql -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USERNAME} \
   -P t -P format=unaligned ${POSTGRES_DATABASE_NAME} \
   -c "UPDATE KEY_PARAMS SET PARAM2 = ${param2} \
      WHERE KEY_PARAM_ID = $key_param_id;"`
exit_code=$?

if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
then
   fn_log_error "Failed to update key_params table for next run." "${log_file_path}"
   
   fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${load_date}" "${log_file_path}" "" " "
fi

fn_log_info "Successfully updated key_params table for next run." "${log_file_path}"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

exit_code="${EXIT_CODE_SUCCESS}"

fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${load_date}" "${log_file_path}" "" ""

##############################################################################
#                                    End                                     #
############################################################################## 