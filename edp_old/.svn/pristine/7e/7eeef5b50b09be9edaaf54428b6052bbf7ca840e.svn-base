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
#   Purpose:    : Execute all steps one by one for BeringMedia Ingestion Process
#   Author(s)   : DataMetica Team
#   Usage       : sh executor.sh 
#   Date        : 04/10/2016
#   Log File    : .../log/ddp/EXECUTOR.log
#   SQL File    : 
#   Error File  : .../log/ddp/EXECUTOR.log
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
source $SUBJECT_AREA_HOME/etc/ddp.properties
source $SUBJECT_AREA_HOME/etc/oracle.properties

source $HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/ddp_functions.sh

##############################################################################
# Local Params				 								 	 			 #
##############################################################################

key_param_id=$1
executor_log_file="${LOG_DIR}/EXECUTOR.log"

params="$(fn_get_ddp_params ${key_param_id})"

if [ -z "$params" ]
then
    fn_log_error "Invalid Key-Param Id found : $key_param_id" "$executor_log_file" 
    exit 
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
# Load data to incoming auxcust table                                        #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id auxcust medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming auxcust table"  
failure_message="Failed while loading data to incoming auxcust table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming auxhouse table                                       #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id auxhouse medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming auxhouse table"  
failure_message="Failed while loading data to incoming auxhouse table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming boxinvtry table                                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id boxinvtry large 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming boxinvtry table"  
failure_message="Failed while loading data to incoming boxinvtry table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming code36 table                                         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id code36 medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming code36 table"  
failure_message="Failed while loading data to incoming code36 table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming code95 table                                         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id code95 medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming code95 table"  
failure_message="Failed while loading data to incoming code95 table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming code999 table                                        #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id code999 medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming code999 table"  
failure_message="Failed while loading data to incoming code999 table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming controlparams table                                  #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id controlparams low  1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming controlparams table"  
failure_message="Failed while loading data to incoming controlparams table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming custbilladdr table                                   #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id custbilladdr medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully loaded data to incoming custbilladdr table"  
failure_message="Failed while loading data to incoming custbilladdr table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming custmaster table                                     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id custmaster large 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming custmaster table"  
failure_message="Failed while loading data to incoming custmaster table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming custrates table                                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id custrates large 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming custrates table"  
failure_message="Failed while loading data to incoming custrates table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming housemaster table                                    #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id housemaster large 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming housemaster table"  
failure_message="Failed while loading data to incoming housemaster table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming ratecodes table                                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id ratecodes medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming ratecodes table"  
failure_message="Failed while loading data to incoming ratecodes table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming ratepricearea table                                  #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id ratepricearea medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming ratepricearea table"  
failure_message="Failed while loading data to incoming ratepricearea table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming ratepricelevel table                                 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id ratepricelevel medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming ratepricelevel table"  
failure_message="Failed while loading data to incoming ratepricelevel table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming raterptctrs table                                    #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id raterptctrs medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming raterptctrs table"  
failure_message="Failed while loading data to incoming raterptctrs table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming rptctrs table                                        #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id rptctrs medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming rptctrs table"  
failure_message="Failed while loading data to incoming rptctrs table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming slsmn table                                          #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id slsmn medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming slsmn table"  
failure_message="Failed while loading data to incoming slsmn table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming techs table                                          #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id techs medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming techs table"  
failure_message="Failed while loading data to incoming techs table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming wipcustrate table                                    #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id wipcustrate large 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming wipcustrate table"  
failure_message="Failed while loading data to incoming wipcustrate table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming wipmaster table                                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id wipmaster large 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming wipmaster table"  
failure_message="Failed while loading data to incoming wipmaster table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming wipoutlet table                                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id wipoutlet large 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming wipoutlet table"  
failure_message="Failed while loading data to incoming wipoutlet table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming zipmaster table                                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id zipmaster medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming zipmaster table"  
failure_message="Failed while loading data to incoming zipmaster table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Setting key_params table for next run.                                     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/update_key_params.sh $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully updated key_params table for next run."  
failure_message="Failed while updating key_params table for next run, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                    End                                     #
##############################################################################