#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: calculate_aggregate_visit_counts.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will calculate the Visit Counts aggregates and store them in gold layer.
#                 Input Arguments for this script are: key_Param_id,frequency(daily).               
#   Author(s)   : DataMetica Team
#   Usage       : sh calculate_aggregate_visit_counts.sh key_Param_id "daily"
#   Date        : 12/28/2015
#   Log File    : .../log/${suite_name}/${job_name}.log
#   SQL File    :                                 
#   Error File  : .../log/${suite_name}/${job_name}.log
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
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/omniture.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/omniture_functions.sh

##############################################################################
#                                                                            #
# Checking number of parameter passed                                        #
#                                                                            #
##############################################################################

if [ "$#" -ne 2 ]; then
  echo "Illegal number of parameters.Parameters expected [ key_param_id frequency(daily) ]"
  exit 
fi

##############################################################################
#																			 #
#   Local Param 				 			     						     #
#                                    										 #
##############################################################################

key_param_id="$1"
frequency=$(echo "$2" | tr '[:upper:]' '[:lower:]')

params="$(fn_get_omniture_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

source_date=$(echo $params | cut -f 1 -d '~'| cut -f 1 -d ' ')
suite_name=$(echo $params | cut -f 2 -d '~')
suite_name=$(echo "$suite_name" | tr '[:upper:]' '[:lower:]')

if [ $suite_name == "$ONET_PROD" ]
then
   job_name="ONET_600009_DLY_AGGR_VST_CNT"
elif [ $suite_name == "cablevis-uow-com" ]
then
   job_name="UOW_600002_DLY_AGGR_VST_CNT"
else
	echo "Invalid suite_name provided"
	exit
fi 

start_time=`date +"%Y-%m-%d %H:%M:%S"`
current_batch_id_path="${BATCH_ID_DIR}/$suite_name/current"
batch_id=`cat $current_batch_id_path`
log_file_path="${LOG_DIR_SUBJECT_AREA}/${suite_name}/${job_name}.log"

##############################################################################
#																			 #
# Checking mandatory Params                                                  #
#																			 #
##############################################################################

fn_check_mandatory_params "suite_name,$suite_name"
  
##############################################################################
#                                                                            #
# Checking Correct Suite Name                                                #
#                                                                            #
##############################################################################

fn_check_suite_name "$suite_name"
  
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

source_file_path=`hadoop fs -ls ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/$suite_name/*$source_date.tar* | awk '{print$8}'`
source_file_name=`basename $source_file_path`

##############################################################################
#                                                                            #
# Getting duration, period start date and period end date					 #
#                                                                            #
##############################################################################

if [ $frequency == "daily" ]
then
   date_range=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
              "SELECT DAY_TAG FROM $HIVE_DATABASE_NAME_INCOMING.${HIVE_TABLE_PREFIX}${INCOMING_PERIOD_TBL} \
              WHERE TO_DATE(PERIOD_DATE)='$source_date';"`
   exit_code="$?"
   
   duration=$date_range
   period_start_date="${source_date}"
   period_end_date="${source_date}"
else
   fn_log_info "Wrong frequency Provided as [$frequency]"  ${log_file_path}
   exit
fi

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "Failed to fetch details from d_period table for $frequency frequency, Quitting the process" "${log_file_path}"
	fn_update_job_statistics "${new_batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
	  "${source_date}" "${log_file_path}" "${omniture_file_name}_${source_date}.tar.gz" " "
fi
	
fn_log_info "Successfully fetched details from d_period for $frequency frequency" "${log_file_path}"

##############################################################################
#                                                                            #
# Checking period_start_date,period_end_date and duration value              #
#                                                                            #
##############################################################################
 
if [[ -z $period_start_date ]] || [[ -z $period_end_date ]] || [[ -z $duration ]]
then
    fn_log_error "Duration or Start Date or End Date field is NULL. Please check with provided parameters, Quitting the process" "${log_file_path}"
    fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${EXIT_CODE_FAIL}" \
      "${source_date}" "${log_file_path}" "${source_file_name}" "$args"
fi

##############################################################################
#                                                                            #
# Calculating Visit Counts aggregate for provided frequency                  #
#                                                                            #
##############################################################################

args="START_DATE=$period_start_date | END_DATE=$period_end_date | MEASURE=${MEASURE_VISIT_COUNTS} | LEVEL = $(echo "$frequency" | tr '[:lower:]' '[:upper:]') "

fn_log_info "Calculating daily aggregates visit counts for suite $suite_name and for date $source_date" \
   ${log_file}

sh $SUBJECT_AREA_HOME/gold_aggregate_visit_counts_tbl/bin/module.sh "transform" "${suite_name}" \
   "${period_start_date}" "${period_end_date}" "${frequency}" "${duration}" "${log_file_path}"
exit_code="$?"

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "Failed to calculate $frequency Visit Counts aggregates for suite $suite_nam, Quitting the process" "${log_file_path}"
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
	  "${source_date}" "${log_file_path}" "${source_file_name}" "$args"
fi

fn_log_info "Successfully calculated $frequency aggregates visit counts for gold layer" "${log_file_path}"

##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
   "${source_date}" "${log_file_path}" "${source_file_name}"  "$args"

##############################################################################
#                                    End                                     #
##############################################################################