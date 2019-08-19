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
#   Purpose:    : This will load data in all Incoming layer.                            
#                 Input Arguments for this script is: key_Param_id.               
#   Author(s)   : DataMetica Team
#   Usage       : sh load_incoming_modules.sh key_Param_id table_name
#   Date        : 03/25/2017
#   Log File    : .../log/footprints/${job_name}.log
#   SQL File    :                                 
#   Error File  : .../log/footprints/${job_name}.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          03/25/2017       Initial version
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

source $PROJECT_HOME/common/etc/namespace.properties
source $PROJECT_HOME/common/etc/beeline.properties
source $PROJECT_HOME/common/etc/default.env.properties
source $PROJECT_HOME/common/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/common/bin/functions.sh
source $SUBJECT_AREA_HOME/etc/svodusagedemographics.properties
source $SUBJECT_AREA_HOME/bin/svodusagedemographics_functions.sh


###############################################################################
#                                                                             #
# Checking number of parameter passed                                         #
#                                                                             #
###############################################################################

if [ "$#" -ne 2 ]
then
  echo "Illegal number of parameters.  Parameters expected [key_param_id Table name]"
  exit 
fi

##############################################################################
#																			 #
#   Local Param 				 			     						     #
#                                    										 #
##############################################################################

key_param_id="$1"
table_name="$2"

params="$(fn_get_svod_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

source_date=$(echo $params | cut -f 1 -d '~'| cut -f 1 -d ' ')
dataset_name=$(echo $params | cut -f 3 -d '~')
dataset_name=$(echo "$dataset_name" | tr '[:upper:]' '[:lower:]')
month_id=$(echo $params | cut -f 4 -d '~') 
start_time=`date +"%Y-%m-%d %H:%M:%S"`

if [[ $table_name == "encrypted_d_ecohort_chc" ]]
then
    job_name="EDP_SVOD_${key_param_id}_ECOHORT_LD_IN"
elif [[ $table_name == "encrypted_d_ethnic_chc" ]]
then
    job_name="EDP_SVOD_${key_param_id}_ETHENIC_LD_IN"
elif [[ $table_name == "stg_d_geog_corp" ]]
then
    job_name="EDP_SVOD_${key_param_id}_GEOG_LD_IN"
elif [[ $table_name == "stg1_vod_orders_mth_corp" ]]
then
    job_name="EDP_SVOD_${key_param_id}_STG1_LD_IN"
elif [[ $table_name == "stg2_vod_orders_mth_corp" ]]
then
    job_name="EDP_SVOD_${key_param_id}_STG2_LD_IN"
elif [[ $table_name == "stg3_vod_orders_mth_corp" ]]
then
    job_name="EDP_SVOD_${key_param_id}_STG3_LD_IN" 	
else
    echo "Wrong table name provided"
    exit -1
fi

batch_id_dataset_name_dir_path=${BATCH_ID_DIR}/$dataset_name
current_batch_id_path="$batch_id_dataset_name_dir_path/current"
batch_id=`cat $current_batch_id_path`
log_file_dir="${LOG_DIR_SUBJECT_AREA}"
log_file_path="${log_file_dir}/${job_name}.log"
formatted_source_date=`echo $source_date | tr -d -`

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
# Loading Incoming table                                                     #
#																			 #
##############################################################################

source_table="${table_name}"
target_table="incoming_${table_name}"
 
        
   sh $SUBJECT_AREA_HOME/${target_table}_tbl/bin/module.sh "transform" "${log_file_path}" "$month_id"
   exit_code=$?   

   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
   then
      fn_log_error "Failed to load data for table "${target_table}" in Incoming layer" "${log_file_path}"
	  fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}"  "${source_date}" "${log_file_path}" "${source_file_name}" " "
   fi 
   fn_log_info "Successfully loaded data for table ${target_table} in Incoming layer" "${log_file_path}"


##############################################################################
#																			 #
# Capture Record count                                                       #
#																			 #
##############################################################################

record_count=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
             "SELECT COUNT(*) FROM $HIVE_DATABASE_NAME_INCOMING.${HIVE_TABLE_PREFIX}${target_table};"`
              
##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "${record_count}" "${exit_code}" "${source_date}" "${log_file_path}" "" " "

##############################################################################
#                                    End                                     #
##############################################################################