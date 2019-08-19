#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: purge_utility.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will Purge the historical data of given date range for incoming, gold and archive layer
#                 It will accept the params in following sequence:   			
#                    1. Start date with Timestamp  										
#                    2. End date with Timestamp              
#   Author(s)   : DataMetica Team
#   Usage       : purge_utility.sh "201510150100" "201510150100"
#   Date        : 12/28/2015
#   Log File    : .../log/bering_media/BERING_MEDIA_PURGE_UTILITY_JOB.log
#   SQL File    : 
#   Error File  : .../log/bering_media/BERING_MEDIA_PURGE_UTILITY_JOB.log
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
source $SUBJECT_AREA_HOME/etc/bering-media.properties 

##############################################################################
#                                                                            #
# Checking number of parameter passed                                        #
#                                                                            #
##############################################################################

if [ "$#" -ne 2 ] 
then
   echo "Illegal number of parameters."
   echo "Parameters expected [Start_source_date_time<yyyMMddhhmm> End_source_date_time<yyyyMMddhhmm> ]"
   exit 
fi

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

current_date=`date +"%Y-%m-%d"`
source_timestamp=$1
end_timestamp=$2
job_name="BERING_MEDIA_PURGE_UTILITY_JOB"
start_time=`date +"%Y-%m-%d %H:%M:%S"`

start_date=`(echo $source_timestamp | cut -c1-8)`
source_time=`(echo $source_timestamp | cut -c9-12)`

end_date=`(echo $end_timestamp | cut -c1-8)`
end_time=`(echo $end_timestamp | cut -c9-12)`

log_file_dir="${LOG_DIR_SUBJECT_AREA}"
log_file_path="${LOG_DIR_SUBJECT_AREA}/${job_name}.log"

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

export PGPASSWORD=$POSTGRES_PASSWORD;
new_batch_id=`psql -X -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USERNAME \
              -P t -P format=unaligned $POSTGRES_DATABASE_NAME -c "select nextval('generate_seq_id')" 2>> "${log_file_path}"`

if [ -z "$new_batch_id" ]
then
   fn_log_error "Failed while creating new BatchId!!!" "${log_file_path}"
   exit -1
else
   fn_log_info "Successfully created new BatchId [$new_batch_id]" "${log_file_path}"
fi

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${new_batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}"

#####################################################################################
#										   											#
#  check date for start and end date 			   									#
#										   											#
#####################################################################################
   
fromdate=$(date -I -d "$start_date") || exit -1
todate=$(date -I -d "$end_date")     || exit -1
args="START_DATE=$fromdate:$source_time | END_DATE=$todate:$end_time | STAGE=incoming,gold,archive "

delete_date="$fromdate"
delete_hour=`expr $source_time - 0`   #  converting string to int
delete_hour=$(($delete_hour + 0))     #  converting string to int


while [ "$(date -d "$delete_date" +%Y%m%d)" -le "$(date -d "$todate" +%Y%m%d)" ]
do
   purge_end_time=1800
      
   if [ "$(date -d "$delete_date" +%Y%m%d)" -eq "$(date -d "$todate" +%Y%m%d)" ]
   then
      purge_end_time=$end_time
   fi
   
   while [ "$delete_hour" -le "$purge_end_time" ]
   do
      if [ "$delete_hour" -eq 0 ]
      then
         delete_hour=000$delete_hour
      elif [ "$delete_hour" -eq 600 ]
      then
         delete_hour=0$delete_hour
      fi
      
      tables=${INCOMING_EXT_T_F_BM_DHCP_IP_TBL}
      table_name=`basename ${tables}`
      target_dir_path=${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_DATA}/${table_name}/source_date=${delete_date}/hh=${delete_hour}
      
      fn_hadoop_delete_directory_if_exists "${target_dir_path}" "${BOOLEAN_FALSE}" "${log_file_path}"
      exit_code=$?   

      if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
      then
          fn_log_error "Failed to delete HDFS directory for table ${table_name}" "${log_file_path}"
       	  fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${current_date}" "${log_file_path}" "NA" "$args"
      fi 
      
      fn_alter_table_drop_two_partitions \
         "${HIVE_DATABASE_NAME_INCOMING}" \
         "${INCOMING_EXT_T_F_BM_DHCP_IP_TBL}" \
         "source_date='${delete_date}'" \
         "hh='${delete_hour}'" \
         "${BOOLEAN_FALSE}" \
         "${HIVESERVER2_URL}" \
         "${HIVE_USENAME}" \
         "${HIVE_PASSWORD}" \
         "${log_file_path}"
      exit_code=$?   

      if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
      then
          fn_log_error "Failed to alter table ${INCOMING_EXT_T_F_BM_DHCP_IP_TBL}" "${log_file_path}"
       	  fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${current_date}" "${log_file_path}" "NA" "$args"
      fi

      ###
      # Purging Archive files
      ###

      delete_date_format=`date -d ${delete_date} +%Y%m%d`${delete_hour}

      hadoop fs -test -e "${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}/beringmedia_dhcp_log_${delete_date_format}.dat"
      exit_code=$?

      if [ ${exit_code} == ${EXIT_CODE_SUCCESS} ]
      then
         fn_hadoop_delete_file "${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}/beringmedia_dhcp_log_${delete_date_format}.dat" \
            "${BOOLEAN_FALSE}" "${log_file_path}"
         exit_code=$?   

	     if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
		 then
		     fn_log_error "Failed to delete HDFS Archive directory data" "${log_file_path}"
             fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${current_date}" "${log_file_path}" "NA" "$args"
         fi
       fi

      delete_hour=`expr $delete_hour - 0`
      delete_hour=$(($delete_hour + 0))
      delete_hour=$(($delete_hour+600))
   done

################ UPDATING GOLD TABLE ################

   tables=(${GOLD_T_F_BM_DHCP_IP_TBL})
   for table in "${tables[@]}"
   do
      table_name=`basename ${table}`
      sh $SUBJECT_AREA_HOME/${table}_tbl/bin/module.sh "transform" "${source_timestamp}" "${log_file_path}"
      exit_code=$?
      
      if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	  then
		  fn_log_error "Failed to transformed "${table_name}" in gold layer" "${log_file_path}"
		  fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${current_date}" "${log_file_path}" "NA" "$args"
	  fi 
	  
	  fn_log_info "Successfully transformed ${table_name} in gold layer" "${log_file_path}"
   done

####################################################
   
   delete_date=$(date -I -d "$delete_date + 1 day")
   delete_hour=0000
   delete_hour=$(($delete_hour + 0))
done

fn_log_info "Successfully deleted for source table "${table_name}" at data layer ${dir_name} for date ${delete_date}" "${log_file_path}"

##############################################################################
#                                                                                                                                                        #
# Capture statistics                                                         #
#                                                                                                                                                        #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${current_date}" "${log_file_path}" "NA" "$args"

##############################################################################
#                                    End                                     #
##############################################################################    		      