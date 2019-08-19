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

success_message="Successfully generated batch id for channel Tuning Dimension table"  
failure_message="Failed to generate batch id for channel Tuning Dimension table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Move data from Redshift to S3 from aws_job_id 100190                   	 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/prior_move_redshift_to_s3.sh 100190 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities before moving data from Redshift to S3 for aws_job_id 100190"  
failure_message="Failed while performing activities before moving data from Redshift to S3 for aws_job_id 100190, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/load_500601_archive_data_redshift_to_s3.ksh 100190 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully move data from Redshift to S3 for aws_job_id 100190"  
failure_message="Failed while moving data from Redshift to S3 for aws_job_id 100190, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/post_move_redshift_to_s3.sh 100190 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities after moving data from Redshift to S3 for aws_job_id 100190"  
failure_message="Failed while performing activities after moving data from Redshift to S3 for aws_job_id 100190, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Move data from Redshift to S3 from aws_job_id 100191                   	 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/prior_move_redshift_to_s3.sh 100191 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities before moving data from Redshift to S3 for aws_job_id 100191"  
failure_message="Failed while performing activities before moving data from Redshift to S3 for aws_job_id 100191, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/load_500601_archive_data_redshift_to_s3.ksh 100191 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully move data from Redshift to S3 for aws_job_id 100191"  
failure_message="Failed while moving data from Redshift to S3 for aws_job_id 100191, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/post_move_redshift_to_s3.sh 100191 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities after moving data from Redshift to S3 for aws_job_id 100191"  
failure_message="Failed while performing activities after moving data from Redshift to S3 for aws_job_id 100191, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Move data from Redshift to S3 from aws_job_id 100192                   	 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/prior_move_redshift_to_s3.sh 100192 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities before moving data from Redshift to S3 for aws_job_id 100192"  
failure_message="Failed while performing activities before moving data from Redshift to S3 for aws_job_id 100192, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/load_500601_archive_data_redshift_to_s3.ksh 100192 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully move data from Redshift to S3 for aws_job_id 100192"  
failure_message="Failed while moving data from Redshift to S3 for aws_job_id 100192, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/post_move_redshift_to_s3.sh 100192 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities after moving data from Redshift to S3 for aws_job_id 100192"  
failure_message="Failed while performing activities after moving data from Redshift to S3 for aws_job_id 100192, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Move data from Redshift to S3 from aws_job_id 100193                   	 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/prior_move_redshift_to_s3.sh 100193 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities before moving data from Redshift to S3 for aws_job_id 100193"  
failure_message="Failed while performing activities before moving data from Redshift to S3 for aws_job_id 100193, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/load_500601_archive_data_redshift_to_s3.ksh 100193 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully move data from Redshift to S3 for aws_job_id 100193"  
failure_message="Failed while moving data from Redshift to S3 for aws_job_id 100193, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/post_move_redshift_to_s3.sh 100193 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities after moving data from Redshift to S3 for aws_job_id 100193"  
failure_message="Failed while performing activities after moving data from Redshift to S3 for aws_job_id 100193, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Move data from Redshift to S3 from aws_job_id 100194                   	 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/prior_move_redshift_to_s3.sh 100194 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities before moving data from Redshift to S3 for aws_job_id 100194"  
failure_message="Failed while performing activities before moving data from Redshift to S3 for aws_job_id 100194, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/load_500601_archive_data_redshift_to_s3.ksh 100194 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully move data from Redshift to S3 for aws_job_id 100194"  
failure_message="Failed while moving data from Redshift to S3 for aws_job_id 100194, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/post_move_redshift_to_s3.sh 100194 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities after moving data from Redshift to S3 for aws_job_id 100194"  
failure_message="Failed while performing activities after moving data from Redshift to S3 for aws_job_id 100194, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Move data from Redshift to S3 from aws_job_id 100195                   	 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/prior_move_redshift_to_s3.sh 100195 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities before moving data from Redshift to S3 for aws_job_id 100195"  
failure_message="Failed while performing activities before moving data from Redshift to S3 for aws_job_id 100195, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/load_500601_archive_data_redshift_to_s3.ksh 100195 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully move data from Redshift to S3 for aws_job_id 100195"  
failure_message="Failed while moving data from Redshift to S3 for aws_job_id 100195, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/post_move_redshift_to_s3.sh 100195 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities after moving data from Redshift to S3 for aws_job_id 100195"  
failure_message="Failed while performing activities after moving data from Redshift to S3 for aws_job_id 100195, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming_t_cvb_f_split_dvr_usage table                		 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_am_modules.sh $key_param_id t_cvb_f_split_dvr_usage 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_cvb_f_split_dvr_usage table"  
failure_message="Failed while loading data to t_cvb_f_split_dvr_usage table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming_t_cvb_f_split_mrdvr_usage table                		 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_am_modules.sh $key_param_id t_cvb_f_split_mrdvr_usage 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_cvb_f_split_mrdvr_usage table"  
failure_message="Failed while loading data to t_cvb_f_split_mrdvr_usage table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming_nz_t_f_split_channel_tuning_6mth table               #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_am_modules.sh $key_param_id nz_t_f_split_channel_tuning_6mth 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to nz_t_f_split_channel_tuning_6mth table"  
failure_message="Failed while loading data to nz_t_f_split_channel_tuning_6mth table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming_nz_t_f_split_channel_tuning_curr table               #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_am_modules.sh $key_param_id nz_t_f_split_channel_tuning_curr 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to nz_t_f_split_channel_tuning_curr table"  
failure_message="Failed while loading data to nz_t_f_split_channel_tuning_curr table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming_t_f_split_ipchannel_tuning table                	 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_am_modules.sh $key_param_id t_f_split_ipchannel_tuning 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_f_split_ipchannel_tuning table"  
failure_message="Failed while loading data to t_f_split_ipchannel_tuning table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold_t_cvb_f_split_dvr_usage table                			 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id t_cvb_f_split_dvr_usage 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_cvb_f_split_dvr_usage table"  
failure_message="Failed while loading data to t_cvb_f_split_dvr_usage table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold_t_cvb_f_split_mrdvr_usage table                			 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id t_cvb_f_split_mrdvr_usage 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_cvb_f_split_mrdvr_usage table"  
failure_message="Failed while loading data to t_cvb_f_split_mrdvr_usage table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold_nz_t_f_split_channel_tuning_6mth table                	 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id nz_t_f_split_channel_tuning_6mth 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to nz_t_f_split_channel_tuning_6mth table"  
failure_message="Failed while loading data to nz_t_f_split_channel_tuning_6mth table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold_nz_t_f_split_channel_tuning_curr table                	 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id nz_t_f_split_channel_tuning_curr 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to nz_t_f_split_channel_tuning_curr table"  
failure_message="Failed while loading data to nz_t_f_split_channel_tuning_curr table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold_t_f_split_ipchannel_tuning table                		 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id t_f_split_ipchannel_tuning 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_f_split_ipchannel_tuning table"  
failure_message="Failed while loading data to t_f_split_ipchannel_tuning table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold_nz_t_f_split_channel_tuning_rst table                	 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id nz_t_f_split_channel_tuning_rst 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to nz_t_f_split_channel_tuning_rst table"  
failure_message="Failed while loading data to nz_t_f_split_channel_tuning_rst table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Updating edp key_params                                                    #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/update_edp_key_params.sh $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully updated KEY_PARAM table"  
failure_message="Failed while updating KEY_PARAM table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Updating aws key_params                                                    #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/prior_update_aws_key_param.sh $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully perform activities before updating AWS_KEY_PARAM table"  
failure_message="Failed perform activities before updating AWS_KEY_PARAM table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/update_aws_key_params.ksh $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully updated AWS_KEY_PARAM table"  
failure_message="Failed while updating AWS_KEY_PARAM table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/post_update_aws_key_param.sh $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully perform activities after updating AWS_KEY_PARAM table"  
failure_message="Failed perform activities after updating AWS_KEY_PARAM table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"


##############################################################################
#                                    End                                     #
##############################################################################