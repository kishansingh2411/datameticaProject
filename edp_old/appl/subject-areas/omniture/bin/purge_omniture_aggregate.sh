#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: purge_omniture_aggregate.sh  
#   Program type: Unix Bash Shell script
#   Purpose:    : This will Purge the historical data of given date and given suite    
#                 name and specific date.                    
#                 It will accept the params in following sequence:   						 
#                    1. suite name (vow)   														 
#                    2. aggregate_type 	("unique_visitor" , "page_view" , "visit_count" )												     
#                    3. start date  ("2015-12-28")														 
#                    4. End date	("2015-12-28")														 							
#                    5. Period (daily,weekly,monthly)                                                    
#   Author(s)   : DataMetica Team
#   Usage       : sh purge_omniture_aggregate.sh  "${suite_name}" "unique_visitor" "20151228" "20151228" "daily"
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

SUBJECT_AREA_HOME=`dirname ${SCRIPT_HOME}`
SUBJECT_AREAS_HOME=`dirname ${SUBJECT_AREA_HOME}`
PROJECT_HOME=`dirname ${SUBJECT_AREAS_HOME}`

###############################################################################
#                                                                             #
#   Sourcing reference files  		                                          #
#                                                                             #
###############################################################################   

source $PROJECT_HOME/etc/namespace.properties
source $PROJECT_HOME/etc/beeline.properties
source $PROJECT_HOME/etc/default.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/etc/omniture.properties

##############################################################################
#                                                                            #
# Checking number of parameter passed                                        #
#                                                                            #
##############################################################################

if [ $1 == "--help" ] || [ $1 == "-h" ]
then
    echo "argument to be passed in following sequence::"
    echo "<suite_name> <aggregate type> <period_start_date> <period_end_date> <period(comma separated)>"
    echo "example:: sh aggregate_purge_util.sh vow unique_visitor 20150303 20150304 daily,weekly,monthly"
    exit
fi

if [ "$#" -ne 5 ]
then
  echo "Illegal number of parameters.Parameters expected"
  echo "[Suite_name aggregate_type period_start_date period_end_date aggregate_values ]"
  exit
fi

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

current_date=`date +"%Y-%m-%d"`
suite_name=$(echo "$1" | tr '[:upper:]' '[:lower:]')
aggregate_type=$(echo "$2" | tr '[:upper:]' '[:lower:]')
period_start_date=$3
period_end_date=$4
aggregate_level_list=$5
suite_data_file_path=$SUBJECT_AREA_HOME/metadata/Optimum_Suite.txt
job_name="UOW_600002_PURGE_AGGR"
start_time=`date +"%Y-%m-%d %H:%M:%S"`
log_file_dir="${LOG_DIR_SUBJECT_AREA}/${suite_name}"
log_file_path="${log_file_dir}/${job_name}.log"

##############################################################################
#																			 #
# Checking mandatory Params                                                  #
#																			 #
##############################################################################

fn_check_mandatory_params "suite_name,$suite_name"
fn_check_mandatory_params "aggregate_type,$aggregate_type"
fn_check_mandatory_params "period_start_date,$period_start_date"
fn_check_mandatory_params "period_end_date,$period_end_date"
fn_check_mandatory_params "Aggregate_level_list,$aggregate_level_list"

#####################################################################################
#										   											#
#  check date for start and end date 			   									#
#										   											#
#####################################################################################

fromdatechck=$(date -I -d "$period_start_date") || exit -1
todatechck=$(date -I -d "$period_end_date")     || exit -1

args="SUITE_NAME=$suite_name | START_DATE=$fromdatechck | END_DATE=$todatechck | MEASURE=$aggregate_type | LEVEL=$aggregate_level_list "

#####################################################################################
#										   											#
#  check aggregate type and period   			   									#
#										   											#
#####################################################################################

IFS=","

if [ ! "$(date -d "$fromdatechck" +%Y%m%d)" -le "$(date -d "$todatechck" +%Y%m%d)" ]
then
   fn_log_info "End period date <${todatechck}> should be greater then Start period date <${fromdatechck}>" \
      "${log_file_path}"
fi
 
if [[ $aggregate_level_list == "all" ]]
then
   if [[ $aggregate_type == "page_view" ]] || \
      [[ $aggregate_type == "visit_count" ]]
   then
      aggregate_level_list=daily
   else  
      aggregate_level_list=(daily,weekly,monthly,yearly,quarterly)
   fi
fi

if [[ $aggregate_type == "page_view" ]] ||   \
   [[ $aggregate_type == "visit_count" ]] && \
   [[ $aggregate_level_list != "daily" ]]
then  
   fn_log_info "Wrong Period passed for Aggregate_type:$aggregate_type" "${log_file_path}"
   fn_log_info "expected Aggregate_level for $aggregate_type is Daily" "${log_file_path}"
   exit 1
fi
  
if [[ $aggregate_type == "page_view" ]] ||   \
   [[ $aggregate_type == "visit_count" ]] || \
   [[ $aggregate_type == "unique_visitor" ]]
then
   if [ $aggregate_type == "page_view" ]
   then 
      table_name=${GOLD_AGGREGATE_PAGEVIEW_COUNTS_TBL}
   elif [ $aggregate_type == "visit_count" ]
   then
      table_name=${GOLD_AGGREGATE_VISIT_COUNTS_TBL}
   elif  [ $aggregate_type == "unique_visitor" ]
   then
      table_name=${GOLD_AGGREGATE_UNIQUE_VISITOR_COUNT_TBL}
   fi 
else
   fn_log_info "Provide correct Aggregate_type" "${log_file_path}"
   exit
fi

##############################################################################
#																			 #
# Creating Local Log directory                                               #
#																			 #
##############################################################################

fn_local_create_directory_if_not_exists "${log_file_dir}" "${BOOLEAN_TRUE}" "${log_file_path}"

##############################################################################
#																			 #
# Initialization                                                             #
#																			 #
##############################################################################

export PGHOST=$POSTGRES_HOST;
export PGPORT=$POSTGRES_PORT;
export PGPASSWORD=$POSTGRES_PASSWORD;
batch_id=`psql -X -U $POSTGRES_USERNAME -P t -P format=unaligned $POSTGRES_DATABASE_NAME \
          -c "select nextval('generate_seq_id')"`

if [ -z "$batch_id" ]
then
   fn_log_error "Failed while creating new BatchId!!!" "${log_file_path}"
   exit -1
else
   fn_log_info "Successfully created new BatchId [$batch_id]" "${log_file_path}"
fi

##############################################################################
#									   										 #
# Capture statistics                                                         #
#									  										 #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}"

##############################################################################
#									   										 #
# Aggregate Purging                                                          #
#									  										 #
##############################################################################
 
for aggregate_level in $aggregate_level_list
do
   if [ $aggregate_level == "daily" ]
   then
      date_range=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
                 "SELECT DAY_TAG FROM ${HIVE_DATABASE_NAME_INCOMING}.${INCOMING_PERIOD_TBL} \
	              WHERE DAY_TAG BETWEEN '${period_start_date}' AND '${period_end_date}';"`   
      exit_code=$?
	  date_range=$(echo $date_range | tr '\n' ',')
		  
   elif [ $aggregate_level == "weekly" ]
   then
      date_range=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
                 "SELECT DISTINCT dr.DATERANGE FROM \
			      (SELECT CONCAT(TO_DATE(MIN(wk.PERIOD_DATE)),'|',TO_DATE(MAX(wk.PERIOD_DATE))) AS DATERANGE FROM \
				  ${HIVE_DATABASE_NAME_INCOMING}.${INCOMING_PERIOD_TBL} AS d, \
			      ${HIVE_DATABASE_NAME_INCOMING}.${INCOMING_PERIOD_TBL} AS wk \
				  WHERE d.DAY_TAG BETWEEN '${period_start_date}' AND '${period_end_date}' \
				  AND d.WEEK_NBR_OVERALL=wk.WEEK_NBR_OVERALL \
				  GROUP BY d.PERIOD_DATE \
			      ) dr ;"`
      exit_code=$?
      date_range=$(echo $date_range | tr '\n' ',')
    
   elif [ $aggregate_level == "monthly" ]
   then
      date_range=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
                 "SELECT DISTINCT MONTH_OVERALL_TAG \
                  FROM ${HIVE_DATABASE_NAME_INCOMING}.${INCOMING_PERIOD_TBL} \
	              WHERE DAY_TAG  BETWEEN '${period_start_date}' AND '${period_end_date}'"`
      exit_code=$? 
	  date_range=$(echo $date_range | tr '\n' ',')    
         
   elif [ $aggregate_level == "quarterly" ]
   then
      date_range=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
                 "SELECT DISTINCT QUARTER_OVERALL_TAG \
                  FROM ${HIVE_DATABASE_NAME_INCOMING}.${INCOMING_PERIOD_TBL} \
	              WHERE DAY_TAG BETWEEN '${period_start_date}' AND '${period_end_date}';"`
      exit_code=$? 
	  date_range=$(echo $date_range | tr '\n' ',')       
 
   elif [ $aggregate_level == "yearly" ]
   then
      date_range=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
                 "SELECT DISTINCT YEAR FROM ${HIVE_DATABASE_NAME_INCOMING}.${INCOMING_PERIOD_TBL} \
	              WHERE DAY_TAG BETWEEN '${period_start_date}' AND '${period_end_date}';"`
      exit_code=$? 
	  date_range=$(echo $date_range | tr '\n' ',')
    fi
    
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	then
    	fn_log_error "Failed to fetched details from D_PERIOD " "${log_file_path}"
        fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
         "${current_date}" "${log_file_path}" "NA" "$args"
    fi   

    fn_log_info "Successfully fetched details from D_PERIOD" "${log_file_path}"
    
          
   for daterange in $date_range
   do
      target_dir_path=${DATA_LAYER_DIR_GOLD_SUBJECT_AREA}/${table_name}/suite_name=${suite_name}/frequency=${aggregate_level}/period=${daterange}
       
	  fn_hadoop_delete_directory_if_exists "${target_dir_path}" "${BOOLEAN_FALSE}" "${log_file_path}"
	  exit_code=$?
  	  
  	  if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	  then
      		fn_log_error "Failed to delete directory $target_dir_path, Quitting the process" "${log_file_path}"
			fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
      		"${current_date}" "${log_file_path}" "NA" "$args"
	  fi 
  
      fn_alter_table_drop_three_partitions \
         "${HIVE_DATABASE_NAME_GOLD}" \
         "${table_name}" \
         "suite_name='${suite_name}'" \
         "frequency='${aggregate_level}'" \
         "period='${daterange}'" \
         "${BOOLEAN_FALSE}" \
         "${HIVESERVER2_URL}" \
    	 "${HIVE_USENAME}" \
    	 "${HIVE_PASSWORD}" \
         "${log_file_path}"
      exit_code=$?
      
      if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	  then
   		  fn_log_error "Failed to drop partition for source table "${table_name}" at data layer ${dir_name} for date ${delete_date}" "${log_file_path}"
		  fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
      		"${current_date}" "${log_file_path}" "NA" "$args"
      fi
   done		
done

fn_log_info "Successfully deleted for source table "${table_name}" at data layer ${dir_name} for date ${delete_date}" "${log_file_path}"
    
##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
   "${current_date}" "${log_file_path}" "NA" "$args"
  
##############################################################################
#                                    End                                     #
##############################################################################