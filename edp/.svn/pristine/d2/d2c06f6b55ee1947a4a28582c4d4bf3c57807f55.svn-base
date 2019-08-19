#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_oracle_table.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will calculate the unique visitor aggregates and store them in gold layer.
#                 Input Arguments for this script are: key_Param_id, 
#                                                      frequency(daily/weekly/monthly/quarterly/yearly).               
#   Author(s)   : DataMetica Team
#   Usage       : sh calculate_aggregate_unique_visitor.sh key_Param_id "daily"
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
source $SUBJECT_AREA_HOME/etc/oracle.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/omniture_functions.sh

##############################################################################
#                                                                            #
# Checking number of parameter passed                                        #
#                                                                            #
##############################################################################

if [ "$#" -ne 2 ]; then
  echo "Illegal number of parameters. Parameters expected [ key_param_id frequency(daily) ]"
  exit 
fi

##############################################################################
#																			 #
#   Local Param 				 			     						     #
#                                    										 #
##############################################################################

key_param_id="$1"

params="$(fn_get_omniture_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

source_date=$(echo $params | cut -f 1 -d '~'| cut -f 1 -d ' ')
suite_name=$(echo $params | cut -f 2 -d '~')
suite_name=$(echo "$suite_name" | tr '[:upper:]' '[:lower:]')
frequency=$(echo "$2" | tr '[:upper:]' '[:lower:]')
username=$USERNAME
password=$PASSWORD

case "$frequency" in
   "daily")
      job_name="UOW_600002_EXPT_DLY_UNQ_VSTR"
      ;;
   "weekly")
      job_name="UOW_600002_EXPT_WKLY_UNQ_VSTR"
      ;;
   "monthly")
      job_name="UOW_600002_EXPT_MNTH_UNQ_VSTR"
      ;;
   "quarterly")
      job_name="UOW_600002_EXPT_QTR_UNQ_VSTR"
      ;;
   "yearly")
      job_name="UOW_600002_EXPT_YEAR_UNQ_VSTR"
      ;;
esac

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
# Checking period name		                                                 #
#																			 #
##############################################################################

fn_check_period_name $frequency

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
# Getting following fields from Metadata Period table      					 #
#	- period start date 													 #
#	- period end date 														 # 
#	- duration				    									         #
#                                                                            #
##############################################################################

if [ $frequency == "daily" ]
then
   date_range=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
              " SELECT DAY_TAG FROM ${HIVE_DATABASE_NAME_INCOMING}.${HIVE_TABLE_PREFIX}${INCOMING_PERIOD_TBL} \
               WHERE TO_DATE(PERIOD_DATE)='$source_date';"`
   exit_code="$?"
   
   duration=$date_range
   period_start_date="${source_date}"
   period_end_date="${source_date}"
   
elif [ $frequency == "weekly" ]
then
   date_range=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
              "SELECT CONCAT(TO_DATE(MIN(wk.PERIOD_DATE)),'|',TO_DATE(MAX(wk.PERIOD_DATE))) \
               FROM ${HIVE_DATABASE_NAME_INCOMING}.${HIVE_TABLE_PREFIX}${INCOMING_PERIOD_TBL} AS d, \
               ${HIVE_DATABASE_NAME_INCOMING}.${HIVE_TABLE_PREFIX}${INCOMING_PERIOD_TBL} AS wk \
               WHERE TO_DATE(d.PERIOD_DATE)='$source_date' AND  d.WEEK_NBR_OVERALL=wk.WEEK_NBR_OVERALL \
               GROUP BY d.PERIOD_DATE;"`
   exit_code="$?"
   
   period_start_date=`echo $date_range |cut -d"|" -f1`
   period_start_date=`date --date=$period_start_date '+%Y-%m-%d'`
   period_end_date="${source_date}"
   duration=$date_range
   
elif [ $frequency == "monthly" ]
then
   date_range=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
              "SELECT TO_DATE(MIN(mth.PERIOD_DATE)),MONTH_OVERALL_TAG \
               FROM ${HIVE_DATABASE_NAME_INCOMING}.${HIVE_TABLE_PREFIX}${INCOMING_PERIOD_TBL} AS d, \
               ${HIVE_DATABASE_NAME_INCOMING}.${HIVE_TABLE_PREFIX}${INCOMING_PERIOD_TBL} AS mth \
               WHERE TO_DATE(d.PERIOD_DATE)='$source_date' AND  d.MONTH_NBR_OVERALL=mth.MONTH_NBR_OVERALL \
               GROUP BY d.PERIOD_DATE,d.MONTH_OVERALL_TAG;"`
   exit_code="$?"
   
   period_start_date=`echo $date_range |cut -d" " -f1`
   period_start_date=`date --date=$period_start_date '+%Y-%m-%d'`
   period_end_date="${source_date}"
   duration=`echo $date_range | cut -d " " -f2`
   
elif [ $frequency == "quarterly" ]
then
   date_range=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
              "SELECT TO_DATE(MIN(qtr.PERIOD_DATE)),QUARTER_OVERALL_TAG \
               FROM ${HIVE_DATABASE_NAME_INCOMING}.${HIVE_TABLE_PREFIX}${INCOMING_PERIOD_TBL} AS d, \
               ${HIVE_DATABASE_NAME_INCOMING}.${HIVE_TABLE_PREFIX}${INCOMING_PERIOD_TBL} AS qtr \
               WHERE TO_DATE(d.PERIOD_DATE)='$source_date' AND  d.QUARTER_NBR_OVERALL=qtr.QUARTER_NBR_OVERALL \
               GROUP BY d.PERIOD_DATE,d.QUARTER_OVERALL_TAG;"` 
   exit_code="$?"
   
   period_start_date=`echo $date_range |cut -d" " -f1`
   period_start_date=`date --date=$period_start_date '+%Y-%m-%d'`
   period_end_date="${source_date}"
   duration=`echo $date_range | cut -d " " -f2`
   
elif [ $frequency == "yearly" ]
then
   date_range=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
              "SELECT TO_DATE(MIN(yr.PERIOD_DATE)),YEAR \
               FROM ${HIVE_DATABASE_NAME_INCOMING}.${HIVE_TABLE_PREFIX}${INCOMING_PERIOD_TBL} AS d, \
               ${HIVE_DATABASE_NAME_INCOMING}.${HIVE_TABLE_PREFIX}${INCOMING_PERIOD_TBL} AS yr \
               WHERE TO_DATE(d.PERIOD_DATE)='$source_date' AND  d.YEAR=yr.YEAR
               GROUP BY d.PERIOD_DATE,d.YEAR;"`
   exit_code="$?"
   
   period_start_date=`echo $date_range |cut -d" " -f1`
   period_start_date=`date --date=$period_start_date '+%Y-%m-%d'`
   period_end_date="${source_date}"
   duration=`echo $date_range | cut -d " " -f2`
else

   fn_log_info "Wrong frequency Provided as [$frequency]"  ${log_file_path}	
   exit -1
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
# Exporting data to hdfs file at Outgoing layer				                 #
#                                                                            #
##############################################################################

args="START_DATE=$period_start_date | END_DATE=$period_end_date | MEASURE=${MEASURE_UNIQUE_VISITOR} | LEVEL = $(echo "$frequency" | tr '[:lower:]' '[:upper:]') "

sh $SUBJECT_AREA_HOME/outgoing_unique_visitor_aggregate_tbl/bin/module.sh "prepare" "${suite_name}" \
    "${frequency}" "${duration}" "${log_file_path}"
exit_code="$?"

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "Failed to write outgoing file for suite $suite_name with $frequency, Quitting the process" "${log_file_path}"	
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
	  "${source_date}" "${log_file_path}" "${source_file_name}" "$args"
fi
fn_log_info "Successfully write outgoing file for suite $suite_name with $frequency" "${log_file_path}"

##############################################################################
#                                                                            #
# Exporting data to Oracle table   							                 #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/outgoing_unique_visitor_aggregate_tbl/bin/module.sh "export" "${suite_name}" \
    "${frequency}" "${duration}" "${log_file_path}"
exit_code="$?"

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then  
	fn_log_error "Failed to sqoop data to oracle  for suite $suite_name with $frequency, Quitting the process" "${log_file_path}"
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
	  "${source_date}" "${log_file_path}" "${source_file_name}" "$args"
fi

fn_log_info "Successfully sqoop data to oracle for $frequency unique visitor aggregates " "${log_file_path}"

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