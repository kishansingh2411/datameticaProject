#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_work_ovcdr_modules.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will load data in all Incoming tables.
#                 Input Arguments for this script are: "key_param_id"              
#   Author(s)   : DataMetica Team
#   Usage       : load_work_bhv_modules.sh  
#   Date        : 02/10/2017
#   Log File    : .../log/${job_name}.log
#   SQL File    : 
#   Error File  : .../log/${job_name}.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          02/10/2017       Initial version
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
source $SUBJECT_AREA_HOME/etc/ods.properties
source $SUBJECT_AREA_HOME/etc/oracle.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/ods_functions.sh

##############################################################################
#                                                                             #
# Checking number of parameter passed                                         #
#                                                                             #
##############################################################################

if [ "$#" -ne 2 ] 
then
   echo "Illegal number of parameters. Parameters expected are [ "key_param_id" "table_name" ]"
   exit -1
fi

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

key_param_id="$1"
table_name="$2"

params="$(fn_get_ods_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

if [ "${table_name}" == "bhv_acct_phone_nbr_opt" ]
then
   job_name="EDP_ODS_${key_param_id}_LD_WRK_BHV_OPT"
elif [ "${table_name}" == "bhv_acct_phone_nbr_opt_tmp" ]
 then
  job_name="EDP_ODS_${key_param_id}_LD_WRK_BHV_OPT_TMP"
elif [ "${table_name}" == "bhv_acct_phone_nbr_sdl" ]
then
   job_name="EDP_ODS_${key_param_id}_LD_WRK_BHV_SDL"
elif [ "${table_name}" == "bhv_acct_phone_nbr_sdl_tmp" ]
 then
  job_name="EDP_ODS_${key_param_id}_LD_WRK_BHV_SDL_TMP"    
fi

log_file_dir=${LOG_DIR_SUBJECT_AREA}/customer
batch_id_dir="$BATCH_ID_DIR_SUBJECT_AREA/customer"
start_time=`date +"%Y-%m-%d %H:%M:%S"`
source_date=`echo ${params} | cut -d"~" -f1 | cut -d" " -f1`
current_batch_id_path="${batch_id_dir}/current"
BATCH_ID=`cat ${current_batch_id_path}`
log_file_path=${log_file_dir}/${job_name}.log

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}" 

target_table_work="work_${table_name}"
current_date=`date +"%Y-%m-%d %H:%M:%S"`
  
##############################################################################
#																			 #
# Load Work Table                                                            #
#																			 #
##############################################################################  

sh ${SUBJECT_AREA_HOME}/${target_table_work}_tbl/bin/module.sh "prepare" "${source_date}" "${log_file_path}"
exit_code=$?   

if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
then
	fn_log_error "Failed to load "${source_table}" in Work layer" "${log_file_path}"
	fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "
fi 

fn_log_info "Successfully loaded ${target_table_work} in Work layer" "${log_file_path}"
  
##############################################################################
#																			 #
# Computing hive count of Work table                                         #
#																			 #
##############################################################################

hive_counts=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true \
			--outputformat=tsv2 -e "SELECT COUNT(*) FROM ${HIVE_DATABASE_NAME_WORK_ODS}.${HIVE_TABLE_PREFIX}${target_table_work};"`                                             
exit_code=$?

if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
then
   fn_log_error "Failed to fetched hive record count for table ${target_table_work}" "${log_file_path}"
   fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "0" "${exit_code}" "${source_date}" "${log_file_path}" "" " "   
fi 

fn_log_info "Successfully captured hive ${target_table_work} count." "${log_file_path}"
 	
##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "${hive_counts}" "${exit_code}" "${source_date}" "${log_file_path}" "" ""

##############################################################################
#                                    End                                     #
##############################################################################