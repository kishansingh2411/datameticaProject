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
#   Date        : 01/18/2018
#   Log File    : .../log/sdm/${job_name}.log
#   SQL File    :                                 
#   Error File  : .../log/sdm/${job_name}.log
#   Dependency  : 
#   Disclaimer  :  
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          01/18/2018       Initial version
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
params="$(fn_get_svod_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi
 
source_date=$(echo $params | cut -f 1 -d '~'| cut -f 1 -d ' ')
dataset_name=$(echo $params | cut -f 3 -d '~')
dataset_name=$(echo "$dataset_name" | tr '[:upper:]' '[:lower:]')
year_month_id=$(echo $params | cut -f 4 -d '~')
 
job_name=EDP_SVOD_"${key_param_id}"_PRINT_NMBR
start_time=`date +"%Y-%m-%d %H:%M:%S"`
batch_id_dataset_name_dir_path=${BATCH_ID_DIR}/$dataset_name
current_batch_id_path="$batch_id_dataset_name_dir_path/current"
batch_id=`cat $current_batch_id_path`
log_file_dir="${LOG_DIR_SUBJECT_AREA}"
log_file_path="${log_file_dir}/${job_name}.log"

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
# Calculating final numbers                                                  #
#																			 #
#############################################################################

year=`echo "$year_month_id" | cut -c1-4`
month=`echo "$year_month_id" | cut -c5-6`


fn_log_info "get the total count for the fact table" "${log_file_path}"
#echo "get the total count for the fact table"

fact_total=$(hive -e "SELECT count(*) FROM processed.f_vod_orders_mth_corp WHERE month_id=${year_month_id};")
fact_formatted=$(printf "%'.f\n" $fact_total)

fn_log_info "get the total count for the incoming table" "${log_file_path}"
#echo "get the total count for the incoming table"
kom_f_vod_order=$(hive -e "SELECT count(*) FROM incoming.kom_f_vod_order WHERE year_id=${year_id} and month_id=${month_id};")
kom_f_vod_order_formatted=$(printf "%'.f\n" $kom_f_vod_order)

fn_log_info "get the total count for the incoming table" "${log_file_path}"
#echo "get the total count for the incoming table"
kom_vod_order=$(hive -e "SELECT count(*) FROM incoming.kom_vod_order WHERE month_id=${year_month_id};")
kom_vod_order_formatted=$(printf "%'.f\n" $kom_vod_order)


if [ -z "$fact_formatted" ]; then
        fn_log_info "total records for this monthid is empty or null" "${log_file_path}"
        #echo "total records for this monthid is empty or null"
        fact_formatted=0
fi

if [ -z "$kom_f_vod_order_formatted" ]; then
        fn_log_info "total records for this monthid is empty or null" "${log_file_path}"
        #echo "total records for this monthid is empty or null"
        kom_f_vod_order_formatted=0
fi

if [ -z "$kom_vod_order_formatted" ]; then
       fn_log_info "total records for this monthid is empty or null" "${log_file_path}"
        #echo "total records for this monthid is empty or null"
        kom_vod_order_formatted=0
fi

fn_log_info "fact_formatted=${fact_formatted}" "${log_file_path}"
fn_log_info "kom_f_vod_order_formatted=${kom_f_vod_order_formatted}" "${log_file_path}"
fn_log_info "kom_vod_order_formatted=${kom_vod_order_formatted}" "${log_file_path}"
echo "fact_formatted=${fact_formatted}"
echo "kom_f_vod_order_formatted=${kom_f_vod_order_formatted}"
echo "kom_vod_order_formatted=${kom_vod_order_formatted}"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "" " "

##############################################################################
#                                    End                                     #
##############################################################################
