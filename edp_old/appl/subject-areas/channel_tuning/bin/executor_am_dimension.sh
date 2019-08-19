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
# Move data from Redshift to S3 from aws_job_id 100183                       #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/prior_move_redshift_to_s3.sh 100183 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities before moving data from Redshift to S3"  
failure_message="Failed while performing activities before moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/load_500600_archive_data_redshift_to_s3.ksh 100183 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully move data from Redshift to S3"  
failure_message="Failed while moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/post_move_redshift_to_s3.sh 100183 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities after moving data from Redshift to S3"  
failure_message="Failed while performing activities after moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Move data from Redshift to S3 from aws_job_id 100184                       #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/prior_move_redshift_to_s3.sh 100184 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities before moving data from Redshift to S3"  
failure_message="Failed while performing activities before moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/load_500600_archive_data_redshift_to_s3.ksh 100184 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully move data from Redshift to S3"  
failure_message="Failed while moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/post_move_redshift_to_s3.sh 100184 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities after moving data from Redshift to S3"  
failure_message="Failed while performing activities after moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Move data from Redshift to S3 from aws_job_id 100185                       #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/prior_move_redshift_to_s3.sh 100185 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities before moving data from Redshift to S3"  
failure_message="Failed while performing activities before moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/load_500600_archive_data_redshift_to_s3.ksh 100185 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully move data from Redshift to S3"  
failure_message="Failed while moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/post_move_redshift_to_s3.sh 100185 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities after moving data from Redshift to S3"  
failure_message="Failed while performing activities after moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Move data from Redshift to S3 from aws_job_id 100186                       #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/prior_move_redshift_to_s3.sh 100186 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities before moving data from Redshift to S3"  
failure_message="Failed while performing activities before moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/load_500600_archive_data_redshift_to_s3.ksh 100186 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully move data from Redshift to S3"  
failure_message="Failed while moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/post_move_redshift_to_s3.sh 100186 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities after moving data from Redshift to S3"  
failure_message="Failed while performing activities after moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Move data from Redshift to S3 from aws_job_id 100187                       #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/prior_move_redshift_to_s3.sh 100187 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities before moving data from Redshift to S3"  
failure_message="Failed while performing activities before moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/load_500600_archive_data_redshift_to_s3.ksh 100187 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully move data from Redshift to S3"  
failure_message="Failed while moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/post_move_redshift_to_s3.sh 100187 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities after moving data from Redshift to S3"  
failure_message="Failed while performing activities after moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Move data from Redshift to S3 from aws_job_id 100188                       #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/prior_move_redshift_to_s3.sh 100188 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities before moving data from Redshift to S3"  
failure_message="Failed while performing activities before moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/load_500600_archive_data_redshift_to_s3.ksh 100188 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully move data from Redshift to S3"  
failure_message="Failed while moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/post_move_redshift_to_s3.sh 100188 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities after moving data from Redshift to S3"  
failure_message="Failed while performing activities after moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Move data from Redshift to S3 from aws_job_id 100189                       #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/prior_move_redshift_to_s3.sh 100189 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities before moving data from Redshift to S3"  
failure_message="Failed while performing activities before moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/load_500600_archive_data_redshift_to_s3.ksh 100189 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully move data from Redshift to S3"  
failure_message="Failed while moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

sh $SUBJECT_AREA_HOME/bin/post_move_redshift_to_s3.sh 100189 $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully perform activities after moving data from Redshift to S3"  
failure_message="Failed while performing activities after moving data from Redshift to S3, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming_t_d_channel_name table                				 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_am_modules.sh $key_param_id t_d_channel_name 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_d_channel_name table"  
failure_message="Failed while loading data to t_d_channel_name table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming_t_d_period table                                     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_am_modules.sh $key_param_id t_d_period 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_d_period table"  
failure_message="Failed while loading data to t_d_period table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming_t_d_stb_collection_task table                        #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_am_modules.sh $key_param_id t_d_stb_collection_task 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_d_stb_collection_task table"  
failure_message="Failed while loading data to t_d_stb_collection_task table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming_t_d_household_device table                           #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_am_modules.sh $key_param_id t_d_household_device 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_d_household_device table"  
failure_message="Failed while loading data to t_d_household_device table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming_t_d_household_attribute table                        #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_am_modules.sh $key_param_id t_d_household_attribute 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_d_household_attribute table"  
failure_message="Failed while loading data to t_d_household_attribute table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming_t_d_res_demographic table                            #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_am_modules.sh $key_param_id t_d_res_demographic 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_d_res_demographic table"  
failure_message="Failed while loading data to t_d_res_demographic table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming_t_d_tms_program table                                #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_am_modules.sh $key_param_id t_d_tms_program 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_d_tms_program table"  
failure_message="Failed while loading data to t_d_tms_program table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to smith_t_d_channel_name table                				     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_smith_modules.sh $key_param_id t_d_channel_name 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_d_channel_name table"  
failure_message="Failed while loading data to t_d_channel_name table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to smith_t_d_household_attribute table                           #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_smith_modules.sh $key_param_id t_d_household_attribute 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_d_household_attribute table"  
failure_message="Failed while loading data to t_d_household_attribute table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to smith_t_d_household_device table                              #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_smith_modules.sh $key_param_id t_d_household_device 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_d_household_device table"  
failure_message="Failed while loading data to t_d_household_device table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to smith_t_d_period table                                        #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_smith_modules.sh $key_param_id t_d_period 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_d_period table"  
failure_message="Failed while loading data to t_d_period table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to smith_t_d_res_demographic table                               #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_smith_modules.sh $key_param_id t_d_res_demographic 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_d_res_demographic table"  
failure_message="Failed while loading data to t_d_res_demographic table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to smith_t_d_stb_collection_task table                           #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_smith_modules.sh $key_param_id t_d_stb_collection_task 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_d_stb_collection_task table"  
failure_message="Failed while loading data to t_d_stb_collection_task table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to smith_t_d_tms_program table                                   #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_smith_modules.sh $key_param_id t_d_tms_program 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to t_d_tms_program table"  
failure_message="Failed while loading data to t_d_tms_program table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                    End                                     #
##############################################################################