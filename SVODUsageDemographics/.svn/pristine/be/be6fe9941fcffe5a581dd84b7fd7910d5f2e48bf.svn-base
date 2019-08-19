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
#   Date        : 03/18/2017
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
#    1.0     DataMetica Team          03/18/2017       Initial version
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

source $PROJECT_HOME/common/etc/namespace.properties
source $PROJECT_HOME/common/etc/beeline.properties
source $PROJECT_HOME/common/etc/default.env.properties
source $PROJECT_HOME/common/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/common/bin/functions.sh
source $SUBJECT_AREA_HOME/etc/svodusagedemographics.properties
source $SUBJECT_AREA_HOME/bin/svodusagedemographics_functions.sh

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

params="$(fn_get_svod_params ${key_param_id})"

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

success_message="Successfully generated batch id for SDM ingestion"  
failure_message="Failed to generate batch id for SDM ingestion, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}" "${executor_log_file}"

##############################################################################
#                                                                            #
# Execute Incoming table kom_order                                           #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_komvodorderencryptor.sh $key_param_id
exit_code=$?

success_message="Successfully executed  KomVodOrderEncryptor at Incoming layer"  
failure_message="Failed to execute KomVodOrderEncryptor at Incoming layer, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# Execute Incoming table customer_account                                    #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_komcustomeraccountbulkencryptor.sh $key_param_id
exit_code=$?

success_message="Successfully executed komcustomeraccountbulkencryptor at Incoming layer"  
failure_message="Failed to load data execute komcustomeraccountbulkencryptor at Incoming layer, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# Load Gold table d_vod_studio                                               #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id d_vod_studio
exit_code=$?

success_message="Successfully loaded table d_vod_studio at Gold layer"  
failure_message="Failed to load data in table d_vod_studio at Gold layer, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# Load Gold table D_Vod_Title                                                #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id d_vod_title
exit_code=$?

success_message="Successfully loaded table D_Vod_Title at Gold layer"  
failure_message="Failed to load data in table D_Vod_Title at Gold layer, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# Load Gold table D_Vod_Genre                                                #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id d_vod_genre
exit_code=$?

success_message="Successfully loaded table D_Vod_Genre at Gold layer"  
failure_message="Failed to load data in table D_Vod_Genre at Gold layer, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# Load Gold table D_Vod_Subscription_Name                                    #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id d_vod_subscription_name
exit_code=$?

success_message="Successfully loaded table D_Vod_Subscription_Name at Gold layer"  
failure_message="Failed to load data in table D_Vod_Subscription_Name at Gold layer, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# Load Incoming table encrypted_d_ecohort_chc                                #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id encrypted_d_ecohort_chc
exit_code=$?

success_message="Successfully loaded table encrypted_d_ecohort_chc at Incoming layer"  
failure_message="Failed to load data in table encrypted_d_ecohort_chc at Incoming layer, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# Load Incoming table encrypted_d_ethnic_chc                                 #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id encrypted_d_ethnic_chc
exit_code=$?

success_message="Successfully loaded table encrypted_d_ethnic_chc at Incoming layer"  
failure_message="Failed to load data in table encrypted_d_ethnic_chc at Incoming layer, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# Load Incoming table stg_d_geog_corp                                        #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id stg_d_geog_corp
exit_code=$?

success_message="Successfully loaded table stg_d_geog_corp at Incoming layer"  
failure_message="Failed to load data in table stg_d_geog_corp at Incoming layer, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# Load Incoming table Stg1_Vod_Orders_Mth_Corp                               #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id stg1_vod_orders_mth_corp
exit_code=$?

success_message="Successfully loaded table Stg1_Vod_Orders_Mth_Corp at Incoming layer"  
failure_message="Failed to load data in table Stg1_Vod_Orders_Mth_Corp at Incoming layer, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# Load Incoming table stg2_vod_orders_mth_corp                               #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id stg2_vod_orders_mth_corp
exit_code=$?

success_message="Successfully loaded table stg2_vod_orders_mth_corp at Incoming layer"  
failure_message="Failed to load data in table stg2_vod_orders_mth_corp at Incoming layer, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# Load Incoming table stg3_vod_orders_mth_corp                               #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id stg3_vod_orders_mth_corp
exit_code=$?

success_message="Successfully loaded table stg3_vod_orders_mth_corp at Incoming layer"  
failure_message="Failed to load data in table stg3_vod_orders_mth_corp at Incoming layer, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# Load Gold table f_vod_orders_mth_corp                                      #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id f_vod_orders_mth_corp
exit_code=$?

success_message="Successfully loaded table f_vod_orders_mth_corp at Gold layer"  
failure_message="Failed to load data in table f_vod_orders_mth_corp at Incoming layer, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# copy data script                                                           #
#                                                                            #
##############################################################################

sh /UTIL/app/util/dtu/bin/aws_master_shell.ksh 100020
exit_code=$?

success_message="Successfully copied data "  
failure_message="Failed to copy the data, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# copy data script                                                           #
#                                                                            #
##############################################################################

sh /UTIL/app/util/dtu/bin/aws_master_shell.ksh 100019
exit_code=$?

success_message="Successfully copied data "  
failure_message="Failed to copy the data, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# copy data script                                                           #
#                                                                            #
##############################################################################

sh /UTIL/app/util/dtu/bin/aws_master_shell.ksh 100018
exit_code=$?

success_message="Successfully copied data "  
failure_message="Failed to copy the data, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# copy data script                                                           #
#                                                                            #
##############################################################################

sh /UTIL/app/util/dtu/bin/aws_master_shell.ksh 100021
exit_code=$?

success_message="Successfully copied data "  
failure_message="Failed to copy the data, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# copy data script                                                           #
#                                                                            #
##############################################################################

sh /UTIL/app/util/dtu/bin/aws_master_shell.ksh 100022
exit_code=$?

success_message="Successfully copied data "  
failure_message="Failed to copy the data, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# copy data script                                                           #
#                                                                            #
##############################################################################

sh /UTIL/app/util/dtu/bin/aws_master_shell.ksh 100023
exit_code=$?

success_message="Successfully copied data "  
failure_message="Failed to copy the data, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# copy data script                                                           #
#                                                                            #
##############################################################################

sh /UTIL/app/util/dtu/bin/aws_master_shell.ksh 100024
exit_code=$?

success_message="Successfully copied data "  
failure_message="Failed to copy the data, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# print final numbers                                                        #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/svodusagedemo_print_final_numbers.sh $key_param_id
exit_code=$?

success_message="Successfully copied data "  
failure_message="Failed to copy the data, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# Update key-params table                                                    #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/update_key_params.sh $key_param_id
exit_code=$?

success_message="Successfully updated key-params table"  
failure_message="Failed while updating key-params table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                    End                                     #
##############################################################################