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
#   Purpose:    : Execute all steps one by one for process 
#                 Input Arguments for this script are: key_param_id from postgress.                                  	                       
#   Author(s)   : DataMetica Team
#   Usage       : sh executor.sh key_Param_id
#   Date        : 12/14/2016
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
#    1.0     DataMetica Team          12/14/2016       Initial version
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
source $HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $HOME/bin/functions.sh
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
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

key_param_id=$1
fail_on_error=${BOOLEAN_TRUE}
executor_log_file="${LOG_DIR_SUBJECT_AREA}/EXECUTOR.log"

params="$(fn_get_ovcdr_params ${key_param_id})"

if [ -z "$params" ]
then
    fn_log "Invalid Key-Param Id found : $key_param_id" "$executor_log_file" 
    exit 
fi

##############################################################################
#																			 #
# Generating batch_id for the process                    					 #
#																			 #
##############################################################################

fn_log "Process started at `date +"%Y-%m-%d %H:%M:%S"`" ${executor_log_file}

sh $SUBJECT_AREA_HOME/bin/generate_batch_id.sh $key_param_id
exit_code=$?

success_message="Successfully generated batch id for suite $suite_name"  
failure_message="Failed to generate batch id for suite $suite_name, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Loading Source Data and move to Incoming                                   #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id
exit_code=$?

success_message="Successfully loaded data in incoming"
failure_message="Failed while loading data in Incoming, Quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Loading work CHC Data                                                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_chc_modules.sh $key_param_id
exit_code=$?

success_message="Successfully loaded data in work chc"
failure_message="Failed while loading data in Work chc, Quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Loading Work Data                                                          #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_modules.sh $key_param_id "insert"
exit_code=$?

success_message="Successfully loaded data in work"
failure_message="Failed while loading data in Work, Quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Loading Gold Data                                                          #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id
exit_code=$?

success_message="Successfully loaded data in gold"
failure_message="Failed while loading data in Gold, Quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Loading Work Data                                                          #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_modules.sh $key_param_id "update"
exit_code=$?

success_message="Successfully loaded data in work"
failure_message="Failed while loading data in Work, Quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Loading Gold Data                                                          #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id
exit_code=$?

success_message="Successfully loaded data in gold"
failure_message="Failed while loading data in Gold, Quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"


##############################################################################
#																			 #
# Loading Archive Layer      				                                 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_archive_modules.sh $key_param_id  
exit_code=$?

success_message="Successfully loaded archive layer"
failure_message="Failed while loading archive layer, quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

fn_log "Process ended at `date +"%Y-%m-%d %H:%M:%S"`" ${executor_log_file}

##############################################################################
#                                    End                                     #
##############################################################################