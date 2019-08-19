#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: executor.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : Execute all steps one by one for Channel Tunning Ingestion Process
#   Author(s)   : DataMetica Team
#   Usage       : sh executor.sh 
#   Date        : 04/10/2016
#   Log File    : .../log/channel_tuning/EXECUTOR.log
#   SQL File    : 
#   Error File  : .../log/channel_tuning/EXECUTOR.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          04/10/2016       Initial version
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
source $HOME/etc/beeline.properties
source $HOME/etc/default.env.properties
source $HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $SUBJECT_AREA_HOME/etc/channel_tuning.properties
source $SUBJECT_AREA_HOME/etc/netezza.properties
source $HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/channel_tuning_functions.sh

##############################################################################
# Local Params				 								 	 			 #
##############################################################################

key_param_id=$1
executor_log_file="${LOG_DIR}/EXECUTOR.log"

params="$(fn_get_channel_tuning_params ${key_param_id})"

if [ -z "$params" ]
then
    fn_log_error "Invalid Key-Param Id found : $key_param_id" "$executor_log_file" 
    exit -1
fi

##############################################################################
#																			 #
# Generating batch_id for the process                    					 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/generate_batch_id.sh $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully generated batch id for source date $source_date"  
failure_message="Failed to generate batch id for source date $source_date, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work_nz_t_f_split_channel_tuning_rst_tbl table                #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_modules.sh $key_param_id nz_t_f_split_channel_tuning_rst 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to work_nz_t_f_split_channel_tuning_rst_tbl table"  
failure_message="Failed while loading data to work_nz_t_f_split_channel_tuning_rst_tbl table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"


##############################################################################
#																			 #
# Load data to incoming_nz_t_f_split_channel_tuning_rst_tbl table            #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id nz_t_f_split_channel_tuning_rst 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming_nz_t_f_split_channel_tuning_rst_tbl table"  
failure_message="Failed while loading data to incoming_nz_t_f_split_channel_tuning_rst_tbl table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work_t_cvb_chdv_a_hh_series_summary_tbl table                 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_modules.sh $key_param_id t_cvb_chdv_a_hh_series_summary 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to work_t_cvb_chdv_a_hh_series_summary_tbl table"  
failure_message="Failed while loading data to work_t_cvb_chdv_a_hh_series_summary_tbl table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming_t_cvb_chdv_a_hh_series_summary_tbl table             #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id t_cvb_chdv_a_hh_series_summary 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming_t_cvb_chdv_a_hh_series_summary_tbl table"  
failure_message="Failed while loading data to incoming_t_cvb_chdv_a_hh_series_summary_tbl table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work_t_cvb_f_stb_device_state_tbl table                       #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_modules.sh $key_param_id t_cvb_f_stb_device_state 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to work_t_cvb_f_stb_device_state_tbl table"  
failure_message="Failed while loading data to work_t_cvb_f_stb_device_state_tbl table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming_t_cvb_f_stb_device_state_tbl table                   #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id t_cvb_f_stb_device_state 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming_t_cvb_f_stb_device_state_tbl table"  
failure_message="Failed while loading data to incoming_t_cvb_f_stb_device_state_tbl table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work_t_f_household_device_tbl table                           #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_modules.sh $key_param_id t_f_household_device 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to work_t_f_household_device_tbl table"  
failure_message="Failed while loading data to work_t_f_household_device_tbl table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming_t_f_household_device_tbl table                       #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id t_f_household_device 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming_t_f_household_device_tbl table"  
failure_message="Failed while loading data to incoming_t_f_household_device_tbl table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                    End                                     #
##############################################################################