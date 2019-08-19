#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: validate_move_to_incoming.sh  
#   Program type: Unix Bash Shell script
#   Purpose:    : This will validate extracted file(dat file) count with count exists in ctrl file                                    
#                 Input Arguments for this script is: key_Param_id.                                                       
#   Author(s)   : DataMetica Team
#   Usage       : sh validate_move_to_incoming.sh  key_Param_id
#   Date        : 03/30/2016
#   Log File    : .../log/hulu/HULU_600003_VDATE_MV_TO_INCOM.log
#   SQL File    :                                 
#   Error File  : .../log/hulu/HULU_600003_VDATE_MV_TO_INCOM.log
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
job_name="HULU_600003_VDATE_MV_TO_INCOM"

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
# validate count                                                    #
#                                                                            #
##############################################################################
source_file_path=`hadoop fs -ls ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/HULU_OTT-${source_date}*.tgz | awk '{print$8}'`
source_file_name=`basename $source_file_path`

source_dat_file_path=`hadoop fs -ls ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/HULU_OTT-${source_date}*/HULU_OTT-REGISTRATIONS-${source_date}*.dat | awk '{print$8}'`
total_count_dat=`hadoop fs -cat ${source_dat_file_path} | wc -l`

source_ctrl_file_path=`hadoop fs -ls ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/HULU_OTT-${source_date}*.ctrl | awk '{print$8}'`
total_count_ctrl=`hadoop fs -cat $source_ctrl_file_path | cut -d'~' -f2 `

if [[ ${total_count_ctrl} != ${total_count_dat} ]]
      then
         fn_log_error "Number of count in .ctrl file does not matches with .dat file" "${log_file_path}"
         fn_hadoop_move_file_or_directory \
	        "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/HULU_OTT-${source_date}*".tgz \
	        "$DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_BADDATA/source_date=${source_date}" \
	        "${BOOLEAN_TRUE}" "${BOOLEAN_FALSE}" "${log_file_path}"
	     fn_hadoop_move_file_or_directory \
	        "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/HULU_OTT-${source_date}*".ctrl \
	        "$DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_BADDATA/source_date=${source_date}" \
	        "${BOOLEAN_TRUE}" "${BOOLEAN_FALSE}" "${log_file_path}"   
	        
		 fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${EXIT_CODE_FAIL}" \
	     "${source_date}" "${log_file_path}" "${source_file_name}" "Count mismatches between .ctrl and .dat file for ${source_date}"
	     
      fi
      
fn_log_info "Successfully validated data files, Number of count in .ctrl file matches with .dat file," "${log_file_path}"

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
