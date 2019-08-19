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
#   Date        : 12/28/2015
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

##key_param_id is mandatory field
key_param_id=$1
fail_on_error=${BOOLEAN_TRUE}
executor_log_file="${LOG_DIR_SUBJECT_AREA}/EXECUTOR.log"

params="$(fn_get_omniture_params ${key_param_id})"

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
#                                                                            #
# Copy data from local to landing                                            #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/move_to_landing.sh $key_param_id
exit_code=$?

success_message="Successfully copy source data files from local to Landing directory"  
failure_message="Failed to copy source data files from local to landing directory, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Extract Source Data                                                        #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/extract_source_data.sh $key_param_id
exit_code=$?

success_message="Successfully extracted source data file."  
failure_message="Failed while extracting source data file, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Validate Source Data and move to Incoming                                  #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/validate_move_to_incoming.sh $key_param_id
exit_code=$?

success_message="Successfully validated extracted source data file and moved to incoming"  
failure_message="Failed validation on extracted source data file, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Loading Source Data and move to Incoming                                  #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id
exit_code=$?

success_message="Successfully loaded data in incoming"
failure_message="Failed while loading data in Incoming, Quitting the process."
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
# Calculating daily aggregates pageview counts                               #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/calculate_aggregate_pageview_counts.sh $key_param_id 'daily'
exit_code=$?

success_message="Successfully calculated daily aggregates pageview counts"
failure_message="Failed while calculating daily aggregates pageview counts, quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Calculating daily aggregates visit counts                               #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/calculate_aggregate_visit_counts.sh $key_param_id 'daily'
exit_code=$?

success_message="Successfully calculated daily aggregates visit counts"
failure_message="Failed while calculating daily aggregates visit counts, quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Calculating unique visitor daily aggregates                                #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/calculate_aggregate_unique_visitor.sh $key_param_id 'daily'
exit_code=$?

success_message="Successfully calculated unique visitor daily aggregates"
failure_message="Failed while calculating unique visitor daily aggregates, quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Calculating unique visitor weekly aggregates                               #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/calculate_aggregate_unique_visitor.sh $key_param_id 'weekly'
exit_code=$?

success_message="Successfully calculated unique visitor weekly aggregates"
failure_message="Failed while calculating unique visitor weekly aggregates, quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Calculating unique visitor monthly aggregates                               #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/calculate_aggregate_unique_visitor.sh $key_param_id 'monthly'
exit_code=$?

success_message="Successfully calculated unique visitor monthly aggregates"
failure_message="Failed while calculating unique visitor monthly aggregates, quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Calculating unique visitor quarterly aggregates                               #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/calculate_aggregate_unique_visitor.sh $key_param_id 'quarterly'
exit_code=$?

success_message="Successfully calculated unique visitor quarterly aggregates"
failure_message="Failed while calculating unique visitor quarterly aggregates, quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Calculating unique visitor yearly aggregates                               #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/calculate_aggregate_unique_visitor.sh $key_param_id 'yearly'
exit_code=$?

success_message="Successfully calculated unique visitor yearly aggregates"
failure_message="Failed while calculating unique visitor yearly aggregates, quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Loading Smith Layer      				                                     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_smith_modules.sh $key_param_id 
exit_code=$?

success_message="Successfully loaded smith layer"
failure_message="Failed while loading smith layer, quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"


##############################################################################
#																			 #
# Loading daily data to oracle table       				                     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_oracle_table.sh $key_param_id 'daily'  
exit_code=$?

success_message="Successfully sqoop data to oracle for daily unique visitor aggregates"
failure_message="Failed while sqoop daily unique visitor aggregates data, quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Loading weekly data to oracle table       				                 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_oracle_table.sh $key_param_id 'weekly'  
exit_code=$?

success_message="Successfully sqoop data to oracle for weekly unique visitor aggregates"
failure_message="Failed while sqoop weekly unique visitor aggregates data, quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Loading monthly data to oracle table       				                 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_oracle_table.sh $key_param_id 'monthly'  
exit_code=$?

success_message="Successfully sqoop data to oracle for monthly unique visitor aggregates"
failure_message="Failed while sqoop monthly unique visitor aggregates data, quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Loading quarterly data to oracle table       				                 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_oracle_table.sh $key_param_id 'quarterly'  
exit_code=$?

success_message="Successfully sqoop data to oracle for quarterly unique visitor aggregates"
failure_message="Failed while sqoop quarterly unique visitor aggregates data, quitting the process."
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Loading yearly data to oracle table       				                 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_oracle_table.sh $key_param_id 'yearly'  
exit_code=$?

success_message="Successfully sqoop data to oracle for yearly unique visitor aggregates"
failure_message="Failed while sqoop yearly unique visitor aggregates data, quitting the process."
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