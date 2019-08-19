#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: svodusagedemo_compute_max_date.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : Generating new Batch_id for the current process.                            
#                 Input Arguments for this script are: key_param_id year_month_id.               
#   Author(s)   : DataMetica Team
#   Usage       : sh svodusagedemo_compute_max_date.sh "key_param_id" "year_month_id"
#   Date        : 03/18/2017
#   Log File    : .../log/svod/${job_name}.log
#   SQL File    :                                 
#   Error File  : .../log/svod/${job_name}.log
#   Dependency  : 
#   Disclaimer  :  
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          03/18/2017       Initial version
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

if [ "$#" -ne 1 ]
then
  echo "Illegal number of parameters.  Parameters expected [key_param_id]"
  exit 
fi

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

key_param_id="$1"
first=01
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

job_name=EDP_SVOD_"${key_param_id}"_LD_ORD_ENCPT
start_time=`date +"%Y-%m-%d %H:%M:%S"`
batch_id_dataset_name_dir_path=${BATCH_ID_DIR}/$dataset_name
current_batch_id_path="$batch_id_dataset_name_dir_path/current"
batch_id=`cat $current_batch_id_path`
log_file_dir="${LOG_DIR_SUBJECT_AREA}"
log_file_path="${log_file_dir}/${job_name}.log"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}"

##############################################################################
#																			 #
# compute max and min date	 								 	 			 #
#																			 #
##############################################################################

firstDayStr=${month_id}${first}
firstDay=$(date -d ${firstDayStr} +'%Y-%m-%d')
end_date="$(fn_get_max_date ${firstDayStr})"
echo "year_month_id=${month_id}"
echo "start_date=${firstDay}"
echo "end_date=${end_date}"

##############################################################################
#																			 #
# Load kom order encrptor	 								 	 			 #
#																			 #
##############################################################################

jar_path=${SUBJECT_AREA_HOME}/lib/SVODUsageDemographics-1.0-SNAPSHOT.jar
output_KomVodOrderEncryptor_dir=${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA}/tmp_vod_dir

fn_hadoop_delete_directory_if_exists "${output_KomVodOrderEncryptor_dir}" "${BOOLEAN_FALSE}" "${log_file_path}"
exit_code=$?
if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
    fn_log_error "Failed to delete directory ${output_KomVodOrderEncryptor_dir}" ${log_file_path}
    return $exit_code
fi
fn_log_info "Successfully deleted directory ${output_KomVodOrderEncryptor_dir}" ${log_file_path}

#hadoop jar SVODUsageDemographics-1.0-SNAPSHOT.jar com.alticeusa.ds.svodusagedemographics.encryptor.KomVodOrderEncryptor ${start_date} ${end_date} ${output_KomVodOrderEncryptor_dir}

hadoop jar ${jar_path} com.alticeusa.ds.svodusagedemographics.encryptor.KomVodOrderEncryptor ${firstDay} ${end_date} "${output_KomVodOrderEncryptor_dir}"
exit_code=$?
if [ $exit_code == $EXIT_CODE_FAIL ]
then
   fn_log_error "Failed to get VOD Order encrypted data at directory ${output_KomVodOrderEncryptor_dir}" "${log_file_path}"
   fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "" " "
fi
fn_log_info "Successfully got encrypted data at directory ${output_KomVodOrderEncryptor_dir}" ${log_file_path}


fn_alter_table_with_one_partitions "${HIVE_DATABASE_NAME_INCOMING}" \
	"${HIVE_TABLE_PREFIX}${INCOMING_KOM_VOD_ORDER_TBL}" \
	"month_id=${month_id}" \
	"${BOOLEAN_TRUE}" \
    "${HIVESERVER2_URL}" \
    "${HIVE_USENAME}" \
    "${HIVE_PASSWORD}" \
    "${log_file_path}"
  exit_code=$?
	         
  if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
  then
	  fn_log_error "Failed to alter table ${table_name} " "${log_file_path}"
	  return $exit_code
  fi
fn_log_info "Successfully added partition ${month_id} to table ${INCOMING_KOM_VOD_ORDER_TBL}" ${log_file_path}   

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "" " "

##############################################################################
#                                    End                                     #
##############################################################################