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
#   Purpose:    : UnCompress and Untar the Data located on /landing/<suite_name> directory .                            
#                 Input Arguments for this script is: key_Param_id              
#   Author(s)   : DataMetica Team
#   Usage       : sh extract_source_data.sh key_Param_id
#   Date        : 09/12/2016
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
#    1.0     DataMetica Team          09/12/2016       Initial version
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
source $PROJECT_HOME/etc/default.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $SUBJECT_AREA_HOME/etc/optimum_app.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/omniture_functions.sh

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
#                                                                            #
# Local Params                                                               #
#                                                                            #
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

job_name="$(fn_generate_job_name "$key_param_id" "$suite_name" "EXTRCT_SRC_DATA")"

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
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}" 

##############################################################################
#                                                                            #
# Extracting files in /landing/<suite_name> directory                        #
#                                                                            #
##############################################################################

suite_name=$(echo $suite_name | tr '[:upper:]' '[:lower:]')
source_file_dir="$DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING/$suite_name"

source_file_path=`hadoop fs -ls ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/$suite_name/*$source_date*.gz | \
                  awk '{print$8}'`

source_file_name=`basename $source_file_path`

fn_get_omniture_source_file_name "$suite_name" "$SUBJECT_AREA_HOME/metadata/Optimum_Suite.txt"

source_file=${OMNITURE_SOURCE_FILE_NAME}_${source_date}.tar.gz

hadoop jar $PROJECT_HOME/modules/lib/utils-${version}.jar com.cablevision.util.UntarUtil \
   "$source_file_dir/${source_file}" "$source_file_dir" >> $log_file_path 2>&1
exit_code=$?

if [ $exit_code == $EXIT_CODE_FAIL ]
then
   fn_log_error "For suite $suite_name, failed while extracting file ${source_file_name},Quitting the Process" "${log_file_path}"
   fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name}" " "
fi

fn_log_info "For suite $suite_name, successfully extracted file ${source_file_name}" "${log_file_path}"

##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name}" " "

##############################################################################
#                                    End                                     #
##############################################################################