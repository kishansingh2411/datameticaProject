#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: executor_ovcdr.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : Execute all steps one by one for OVCDR ODS Ingestion Process
#   Author(s)   : DataMetica Team
#   Usage       : sh executor_ovcdr.sh "table_name"
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
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $HOME/bin/functions.sh

##############################################################################
#                                                                            #
# Checking number of parameter passed                                        #
#                                                                            #
##############################################################################

if [ "$#" -ne 2 ] 
then
   echo "Illegal number of parameters. Parameters expected are [ "key param id" , "source date" ]"
   exit -1
fi

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

key_param_id=$1
source_date=$2
current_timestamp=`date +%Y%m%d%H%M%S`
fail_on_error=${BOOLEAN_TRUE}
executor_log_file="${LOG_DIR_SUBJECT_AREA}/EXECUTOR_$current_timestamp.log"

##############################################################################
#																			 #
# Generating batch_id for the process                    					 #
#																			 #
##############################################################################

fn_log "Process started at `date +"%Y-%m-%d %H:%M:%S"`" ${executor_log_file}

sh $SUBJECT_AREA_HOME/bin/generate_batch_id.sh "${key_param_id}"
exit_code=$?

success_message="Successfully generated batch id for suite $suite_name"  
failure_message="Failed to generate batch id for ODS, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Load work CUSTOMER_ACCOUNT                       					         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_ovcdr_modules.sh "${key_param_id}" customer_account
exit_code=$?

success_message="Successfully loaded work CUSTOMER_ACCOUNT table."  
failure_message="Failed to load data for work CUSTOMER_ACCOUNT table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Load incoming CUSTOMER_ACCOUNT                       					     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_ovcdr_modules.sh "${key_param_id}" customer_account
exit_code=$?

success_message="Successfully loaded incoming CUSTOMER_ACCOUNT table."  
failure_message="Failed to load data for incoming CUSTOMER_ACCOUNT table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Load work OV_TELEPHONE_NUMBER                       					     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_ovcdr_modules.sh "${key_param_id}" ov_telephone_number 
exit_code=$?

success_message="Successfully loaded work OV_TELEPHONE_NUMBER table."  
failure_message="Failed to load data for work OV_TELEPHONE_NUMBER table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Load incoming OV_TELEPHONE_NUMBER                       					 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_ovcdr_modules.sh "${key_param_id}" ov_telephone_number 
exit_code=$?

success_message="Successfully loaded incoming OV_TELEPHONE_NUMBER table."  
failure_message="Failed to load data for incoming OV_TELEPHONE_NUMBER table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Load incoming IP_SIP_DID_PHONE_NBR                       				     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_ovcdr_modules.sh "${key_param_id}" ip_sip_did_phone_nbr 
exit_code=$?

success_message="Successfully loaded incoming IP_SIP_DID_PHONE_NBR table."  
failure_message="Failed to load data for incoming IP_SIP_DID_PHONE_NBR table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Load incoming IP_SIP_PILOT_PHONE_NBR                       			     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_ovcdr_modules.sh "${key_param_id}" ip_sip_pilot_phone_nbr 
exit_code=$?

success_message="Successfully loaded incoming IP_SIP_PILOT_PHONE_NBR table."  
failure_message="Failed to load data for incoming IP_SIP_PILOT_PHONE_NBR table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# BHV JOBS                       			                                 #
#																			 #
##############################################################################

##############################################################################
#																			 #
# Load incoming ACCOUNT_INFO                       			                 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_bhv_modules.sh "${key_param_id}" account_info 
exit_code=$?

success_message="Successfully loaded incoming ACCOUNT_INFO table."  
failure_message="Failed to load data for incoming ACCOUNT_INFO table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Load incoming ACCOUNT_TNS                       			                 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_bhv_modules.sh "${key_param_id}" account_tns 
exit_code=$?

success_message="Successfully loaded incoming ACCOUNT_TNS table."  
failure_message="Failed to load data for incoming ACCOUNT_TNS table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Load incoming CUST_TELEPHONE_NBR                       			         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_bhv_modules.sh "${key_param_id}" cust_telephone_nbr 
exit_code=$?

success_message="Successfully loaded incoming CUST_TELEPHONE_NBR table."  
failure_message="Failed to load data for incoming CUST_TELEPHONE_NBR table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Load incoming CUSTOMER_TELEPHONE                       			         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_bhv_modules.sh "${key_param_id}" customer_telephone 
exit_code=$?

success_message="Successfully loaded incoming CUSTOMER_TELEPHONE table."  
failure_message="Failed to load data for incoming CUSTOMER_TELEPHONE table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Load work bhv_acct_phone_nbr_opt                       			         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_bhv_modules.sh "${key_param_id}" bhv_acct_phone_nbr_opt 
exit_code=$?

success_message="Successfully loaded work BHV_ACCT_PHONE_NBR_OPT table."  
failure_message="Failed to load data for work BHV_ACCT_PHONE_NBR_OPT table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Load work bhv_acct_phone_nbr_sdl                       			         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_bhv_modules.sh "${key_param_id}" bhv_acct_phone_nbr_sdl 
exit_code=$?

success_message="Successfully loaded work BHV_ACCT_PHONE_NBR_SDL table."  
failure_message="Failed to load data for work BHV_ACCT_PHONE_NBR_SDL table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Load work bhv_acct_phone_nbr_opt_tmp                       			     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_bhv_modules.sh "${key_param_id}" bhv_acct_phone_nbr_opt_tmp 
exit_code=$?

success_message="Successfully loaded work BHV_ACCT_PHONE_NBR_OPT_TMP table."  
failure_message="Failed to load data for work BHV_ACCT_PHONE_NBR_OPT_TMP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Load work bhv_acct_phone_nbr_sdl_tmp                       			     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_bhv_modules.sh "${key_param_id}" bhv_acct_phone_nbr_sdl_tmp 
exit_code=$?

success_message="Successfully loaded work BHV_ACCT_PHONE_NBR_SDL_TMP table."  
failure_message="Failed to load data for work BHV_ACCT_PHONE_NBR_SDL_TMP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Load gold bhv_acct_phone_nbr_opt                       			         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_bhv_modules.sh "${key_param_id}" bhv_acct_phone_nbr_opt 
exit_code=$?

success_message="Successfully loaded gold BHV_ACCT_PHONE_NBR_OPT table."  
failure_message="Failed to load data for gold BHV_ACCT_PHONE_NBR_OPT table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Load gold bhv_acct_phone_nbr_sdl                       			         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_bhv_modules.sh "${key_param_id}" bhv_acct_phone_nbr_sdl 
exit_code=$?

success_message="Successfully loaded gold BHV_ACCT_PHONE_NBR_SDL table."  
failure_message="Failed to load data for gold BHV_ACCT_PHONE_NBR_SDL table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Compute statistics for gold bhv_acct_phone_nbr_opt       			         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/compute_statistics.sh "${key_param_id}" gold_bhv_acct_phone_nbr_opt 
exit_code=$?

success_message="Successfully computed statistics for table gold BHV_ACCT_PHONE_NBR_OPT table."  
failure_message="Failed to compute statistics for gold BHV_ACCT_PHONE_NBR_OPT table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Compute statistics for gold bhv_acct_phone_nbr_sdl       			         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/compute_statistics.sh "${key_param_id}" gold_bhv_acct_phone_nbr_sdl 
exit_code=$?

success_message="Successfully computed statistics for table gold BHV_ACCT_PHONE_NBR_SDL table."  
failure_message="Failed to compute statistics for gold BHV_ACCT_PHONE_NBR_SDL table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Load gold bhv_acct_phone_nbr_opt_sdl                    			         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_bhv_modules.sh "${key_param_id}" bhv_acct_phone_nbr_opt_sdl 
exit_code=$?

success_message="Successfully loaded gold BHV_ACCT_PHONE_NBR_OPT_SDL table."  
failure_message="Failed to load data for gold BHV_ACCT_PHONE_NBR_OPT_SDL table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#																			 #
# Update key_params for next run                        			         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/update_key_params.sh "${key_param_id}" 
exit_code=$?

success_message="Successfully updated key_params table for next run."  
failure_message="Failed to update key_params table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#                                    End                                     #
##############################################################################