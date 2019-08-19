#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: ingest_historic_data.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : Ingest historical BeringMedia data
#                 Input Arguments for this script are: start_date (20151015) and end_date (20151015).               
#   Author(s)   : DataMetica Team
#   Usage       : sh ingest_historic_data.sh "20151015" "20151015"
#   Date        : 12/28/2015
#   Log File    : .../log/bering_media/INGESTION_${current_timestamp}.log
#   SQL File    : 
#   Error File  : .../log/bering_media/INGESTION_${current_timestamp}.log
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
HOME=`dirname ${SUBJECT_AREAS_HOME}`

###                                                                           
# Load all environment properties files
source $HOME/etc/namespace.properties
source $HOME/etc/default.env.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $HOME/bin/functions.sh

##############################################################################
#                                                                            #
# Checking number of parameter passed                                        #
#                                                                            #
##############################################################################

if [ "$#" -ne 2 ] 
then
   echo "Illegal number of parameters..!!! Parameters expected start_date<yyyyMMdd> end_date<yyyyMMdd> ]"
   exit 
fi

##############################################################################
#                                                                            #
# Local Parameters															 #
#                                                                            #
##############################################################################

start_date="$1"
end_date="$2"
mnt_path="/mnt/pivotal_bering_media"

fromdate=$(date -I -d "$start_date") || exit -1
todate=$(date -I -d "$end_date")     || exit -1

job_name="DATA_INGESTION_JOB_FOR_HISTORICAL_BERING_MEDIA_DATA"

current_timestamp=`date +%Y%m%d%H%M%S`

ingestion_log_file="${LOG_DIR_SUBJECT_AREA}/INGESTION_$current_timestamp.log"

##############################################################################
#																			 #
# Copy data from mnt to landing                                              #
#																			 #
##############################################################################

while [ "$(date -d "$start_date" +%Y%m%d)" -le "$(date -d "$end_date" +%Y%m%d)" ]
do
   test -d "${mnt_path}/${start_date}"
   exit_code=$?

   if [ ${exit_code} == ${EXIT_CODE_FAIL} ]
   then
      failure_message="Data folder for date ${start_date} does not exist on path ${mnt_path}/"
      
      fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" \
         "${ingestion_log_file}"
   else
   {
      hdfs dfs -copyFromLocal ${mnt_path}/${start_date}/beringmedia_dhcp_log_*.dat  ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/
      exit_code=$?

      if [ ${exit_code} == ${EXIT_CODE_FAIL} ]
      then
         failure_message="Error occurred while copying the file in Bering Media landing directory"
         
         fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" \
            "${ingestion_log_file}"
      fi
      fn_log "Bering Media data files for ${start_date} copied successfully to landing directory" ${ingestion_log_file}
    }
   fi
   start_date="$(date -d "$start_date +1 day" +%Y%m%d)"
done

##############################################################################
#																			 #
# Logic for Ingestion                                                        #
#																			 #
##############################################################################

#while [ "$(date -d "$fromdate" +%Y%m%d)" -le "$(date -d "$todate" +%Y%m%d)" ]
#do
#   hour_array=(0000 0600 1200 1800)
#		
#   for hour in "${hour_array[@]}"
#   do
#      bering_media_timestamp="$(date -d "$fromdate" +%Y%m%d)"$hour
#      
#      hdfs dfs -test -e \
#         "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/beringmedia_dhcp_log_$bering_media_timestamp.dat"
#      exit_code=$?
#
#      if [ ${exit_code} == ${EXIT_CODE_FAIL} ]
#      then
#        failure_message="No files to pickup, ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING} directory does not contain file for $bering_media_timestamp"
#   		 
#   		 fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_FALSE}" \
#   		    "${ingestion_log_file}"
#      else
#         sh $SUBJECT_AREA_HOME/bin/executor.sh $bering_media_timestamp
#         exit_code=$?
#
#         success_message="Successfully ingested data for $bering_media_timestamp"  
#         failure_message="Failed to ingest data for $bering_media_timestamp" 
#		 
#		 fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" \
#		    "${ingestion_log_file}"
#	  fi
#   done
#		
#   fromdate="$(date -d "$fromdate +1 day" +%Y-%m-%d)"
#done

    sh $SUBJECT_AREA_HOME/bin/executor.sh
    exit_code=$?

    success_message="Successfully ingested data for all available files in landing directory"  
    failure_message="Failed to ingest data for files available in landing directory" 
		 
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${ingestion_log_file}"

##############################################################################
#                                    End                                     #
##############################################################################