#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_outgoing_modules.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will derive outgoing_export_node_srvgrp table from gold table
#                 Input Arguments for this script are: key_Param_id 
#                                                                     
#   Author(s)   : DataMetica Team
#   Usage       : sh load_outgoing_modules.sh key_Param_id
#   Date        : 02/07/2017
#   Log File    : .../log/${job_name}.log
#   SQL File    :                                 
#   Error File  : .../log/${job_name}.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          02/07/2017      Initial version
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
source $SUBJECT_AREA_HOME/etc/remedy.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/remedy_functions.sh

##############################################################################
#                                                                            #
# Checking number of parameter passed                                        #
#                                                                            #
##############################################################################

if [ "$#" -ne 1 ]
then
  echo "Illegal number of parameters.  Parameters expected [key_param_id]"
  exit 
fi

##############################################################################
#																			 #
#   Local Param 				 			     						     #
#                                    										 #
##############################################################################

key_param_id="$1"

params="$(fn_get_remedy_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

source_date=$(echo $params | cut -f 1 -d '~'| cut -f 1 -d ' ')
dataset_name=$(echo $params | cut -f 3 -d '~')
dataset_name=$(echo "$dataset_name" | tr '[:upper:]' '[:lower:]')
start_time=`date +"%Y-%m-%d %H:%M:%S"`
job_name="EDP_RDY_${key_param_id}_NOD_SRVGP_LD_OG"
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
#                                                                            #
# Exporting data to HDFS file at Outgoing layer				                 #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/${OUTGOING_EXPORT_NODE_SRVGRP_TBL}_tbl/bin/module.sh "prepare" "${source_date}" "${log_file_path}"
exit_code="$?"

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "Failed to insert data at Outgoing Layer ${OUTGOING_EXPORT_NODE_SRVGRP_TBL}, Quitting the process" "${log_file_path}"	
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "" ""
fi
fn_log_info "Successfully inserted data at Outgoing Layer ${OUTGOING_EXPORT_NODE_SRVGRP_TBL}" "${log_file_path}"

##############################################################################
#																			 #
# Capture Record count                                                       #
#																			 #
##############################################################################

source_date=`echo $source_date | tr -d -`
record_count=`hadoop fs -cat ${DATA_LAYER_DIR_OUTGOING_SUBJECT_AREA}/${OUTGOING_EXPORT_NODE_SRVGRP_TBL}/p_yyyymmdd=${source_date}/* \
              | wc -l`

##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "${record_count}" "${exit_code}" "${source_date}" "${log_file_path}" ""  ""
 
##############################################################################
#                                    End                                     #
##############################################################################