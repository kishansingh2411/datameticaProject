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
#   Purpose:    : This will move the Source Data into Landing directory 
#                 Input Arguments for this script is: key_Param_id.                                                       
#   Author(s)   : DataMetica Team
#   Usage       : sh move_to_landing.sh  key_Param_id
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
suite_data_file_path=$SUBJECT_AREA_HOME/metadata/Optimum_Suite.txt

params="$(fn_get_omniture_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

source_date=$(echo $params | cut -f 1 -d '~'| cut -f 1 -d ' ')
suite_name=$(echo $params | cut -f 2 -d '~')
suite_name=$(echo "$suite_name" | tr '[:upper:]' '[:lower:]')

job_name="$(fn_generate_job_name "$key_param_id" "$suite_name" "MV_DATA_TO_LANDING")"

start_time=`date +"%Y-%m-%d %H:%M:%S"`
current_batch_id_path="${BATCH_ID_DIR}/$suite_name/current"
batch_id=`cat $current_batch_id_path`
log_file_path="${LOG_DIR_SUBJECT_AREA}/${suite_name}/${job_name}.log"

##############################################################################
#                                                                            #
# Checking mandatory Params                                                  #
#                                                                            #
##############################################################################

fn_check_mandatory_params "suite_name,$suite_name"

##############################################################################
#                                                                            #
# Checking Correct Suite Name                                                #
#                                                                            #
##############################################################################

fn_check_suite_name "$suite_name"

##############################################################################
#                                                                            #
# Checking date format                                                       #
#                                                                            #
##############################################################################

fn_check_date_format "$source_date"

##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}"

##############################################################################
#                                                                            #
# Get Omniture File name                                                     #
#                                                                            #
##############################################################################

suite_name_in_upper_case=`echo "$suite_name" | tr '[:lower:]' '[:upper:]'`
while read -r line
do
   suite_name_file=$(echo $line | cut -f 2 -d ',')
   omniture_file_name=$(echo $line | cut -f 4 -d ',')
   if [ "$suite_name_file" == "$suite_name_in_upper_case" ]
   then
        omniture_file_name=$(echo $line | cut -f 4 -d ',')
        break
    fi
done< $suite_data_file_path

##############################################################################
#                                                                            #
# Creating HDFS directory /landing/<suite_name>                              #
#                                                                            #
##############################################################################

landing_directory_path="${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/$(echo $suite_name | tr '[:upper:]' '[:lower:]')"

fn_hadoop_create_directory_if_not_exists "${landing_directory_path}" "${BOOLEAN_FALSE}" "${log_file_path}"
exit_code=$?

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "For suite $suite_name, failed to create hadoop directory ${landing_directory_path} for suite $suite_name, Quitting the process" "${log_file_path}"
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
		"${source_date}" "${log_file_path}" "${source_file_name}" " "
fi

###############################################################################
#                                                                             #
#  Copy .tar.gz Files From /landing To /landing/<suite_name>/ directory       #
#                                                                             #
###############################################################################

source_file_path="${LOCAL_DATA_DIRECTORY}/${omniture_file_name}_${source_date}.tar.gz"
source_file_name=`basename ${source_file_path}`

fn_hadoop_delete_file "${landing_directory_path}/*" "${BOOLEAN_FALSE}" "${log_file_path}"
exit_code=$?

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "For suite $suite_name, failed to delete files at ${landing_directory_path} for suite $suite_name, Quitting the process" "${log_file_path}"
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
		"${source_date}" "${log_file_path}" "${source_file_name}" " "
fi

fn_log_info "For suite $suite_name, successfully deleted all files from ${landing_directory_path}/*" "${log_file_path}"

fn_copy_from_local "${source_file_path}" "${landing_directory_path}" "${BOOLEAN_FALSE}" \
   "${BOOLEAN_FALSE}" "${log_file_path}"

hdfs dfs -test -e "${landing_directory_path}/${source_file_name}"
exit_code=$?

if [ ${exit_code} == ${EXIT_CODE_FAIL} ]
then
	fn_log_error "For suite $suite_name, failed to copy source data file ${source_file_path} from local to HDFS landing directory ${landing_directory_path} , Quitting the process." "${log_file_path}"
    fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
      "${source_date}" "${log_file_path}" "${source_file_name}" " "
fi

fn_log_info "For suite $suite_name, successfully copied source data file ${source_file_path} from local to HDFS landing directory ${landing_directory_path} " "${log_file_path}"

##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name}" " "

##############################################################################
#                                    End                                     #
##############################################################################