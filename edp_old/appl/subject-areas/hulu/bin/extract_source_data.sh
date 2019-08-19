#!/bin/bash

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: extract_source_data.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : UnCompress and Untar the Data located on /landing directory .                            
#                 Input Arguments for this script is: key_param_id               
#   Author(s)   : DataMetica Team
#   Usage       : sh extract_source_data.sh "key_param_id"
#   Date        : 03/30/2016
#   Log File    : .../log/hulu/HULU_600003_EXTRCT_SRC_DATA.log
#   SQL File    :                                 
#   Error File  : .../log/hulu/HULU_600003_EXTRCT_SRC_DATA.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          03/30/2016       Initial version
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
source $SUBJECT_AREA_HOME/etc/hulu.properties
source $SUBJECT_AREA_HOME/bin/hulu_functions.sh

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

key_param_id="$1"
job_name="HULU_600003_EXTRCT_SRC_DATA"

params="$(fn_get_hulu_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

source_date=$(date -d "$params" +%Y%m%d)

start_time=`date +"%Y-%m-%d %H:%M:%S"`
current_batch_id_path="${BATCH_ID_DIR}/hulu/current"
batch_id=`cat $current_batch_id_path`

log_file_path="${LOG_DIR_SUBJECT_AREA}/hulu/${job_name}.log"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}"

##############################################################################
#                                                                            #
# extract source data file                                                   #
#                                                                            #
##############################################################################

source_file_dir="${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}"
source_file_path=`hadoop fs -ls ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/HULU_OTT-${source_date}*.tgz | \
                  awk '{print$8}'`
source_file_name=`basename $source_file_path`

hadoop jar $PROJECT_HOME/modules/lib/utils-${version}.jar com.cablevision.util.UntarUtil \
   "$source_file_dir/${source_file_name}" "$source_file_dir" >> $log_file_path 2>&1
exit_code=$?

      if [ ${exit_code} == ${EXIT_CODE_FAIL} ]
      then
         fn_log_error "Failed while extracting the file ${source_file_name} present at hdfs landing directory" "${log_file_path}"
      	 fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" \
	    	 "${source_date}" "${log_file_path}" "${source_file_name}" " "
      fi

fn_log_info "Hulu data file ${source_file_name} extracted successfully to landing directory" "${log_file_path}" 

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
   "${source_date}" "${log_file_path}" "${source_file_name}" " "
   
##############################################################################
#                                    End                                     #
##############################################################################
