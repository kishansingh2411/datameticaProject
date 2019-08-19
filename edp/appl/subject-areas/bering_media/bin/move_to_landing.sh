#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: move_to_landing.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will copy data file from local directory to hdfs landing directory
#                 Input Arguments for this script are: source_timestamp (201510150100).
#   Author(s)   : DataMetica Team
#   Usage       : sh move_to_landing.sh "201510150100"
#   Date        : 12/28/2015
#   Log File    : .../log/bering_media/BERING_MEDIA_COPY_TO_LANDING_JOB.log
#   SQL File    : 
#   Error File  : .../log/bering_media/BERING_MEDIA_COPY_TO_LANDING_JOB.log
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

###                                                                           
# Load all environment properties files
source $PROJECT_HOME/etc/namespace.properties
source $PROJECT_HOME/etc/default.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/etc/bering-media.properties

##############################################################################
#                                                                             #
# Checking number of parameter passed                                         #
#                                                                             #
##############################################################################

if [ "$#" -ne 1 ] 
then
  echo "Illegal number of parameters"
  exit 
fi

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

source_timestamp="$1"
source_data_date=`(echo $source_timestamp | cut -c1-8)`
source_data_hour=`(echo $source_timestamp | cut -c9,10)`
source_data_minutes=`(echo $source_timestamp | cut -c11,12)`
formatted_source_date=`date -d $source_data_date +%Y-%m-%d`
formatted_source_timestamp="$formatted_source_date $source_data_hour:$source_data_minutes:00"

current_batch_id_path="${BATCH_ID_DIR_SUBJECT_AREA}/current"
batch_id=`cat $current_batch_id_path`

job_name="BERING_MEDIA_COPY_DATA_TO_LANDING_JOB"
start_time=`date +"%Y-%m-%d %H:%M:%S"`
log_file_path="${LOG_DIR_SUBJECT_AREA}/${job_name}.log"
source_file_path="${LOCAL_DATA_DIRECTORY}/beringmedia_dhcp_log_${source_timestamp}.dat"
source_file_name=`basename ${source_file_path}`

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}"  

##############################################################################
#                                                                            #
# Copy data from local to landing                                            #
#                                                                            #
##############################################################################

hadoop fs -copyFromLocal ${LOCAL_DATA_DIRECTORY}/${source_file_name} ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/	
exit_code=$?

if [ ${exit_code} == ${EXIT_CODE_FAIL} ]
then
    fn_log_error "Error occurred while copying the file ${source_file_name} from local to hdfs landing directory" "${log_file_path}"
    fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" \
      "${formatted_source_timestamp}" "${log_file_path}" "${source_file_name}" " "
fi

fn_log_info "Bering Media data file ${source_file_name} copied successfully to landing directory" "${log_file_path}"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
   "${formatted_source_timestamp}" "${log_file_path}" "${source_file_name}" " "

##############################################################################
#                                    End                                     #
##############################################################################
