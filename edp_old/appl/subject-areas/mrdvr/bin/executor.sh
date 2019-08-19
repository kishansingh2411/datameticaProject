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
#   Log File    : .../log/mrdvr/EXECUTOR.log
#   SQL File    : 
#   Error File  : .../log/mrdvr/EXECUTOR.log
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
source $SUBJECT_AREA_HOME/etc/mrdvr.properties
source $SUBJECT_AREA_HOME/etc/mysql.properties

source $HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/mrdvr_functions.sh

##############################################################################
# Local Params				 								 	 			 #
##############################################################################

key_param_id=$1
executor_log_file="${LOG_DIR}/${SUBJECT_AREA_NAME}/EXECUTOR.log"

params="$(fn_get_mrdvr_params ${key_param_id})"

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
# Load data to work rsdvr_preferences table                                  #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_modules.sh $key_param_id  rsdvr_preferences 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_PREFERENCES table"  
failure_message="Failed while loading data to WORK_RSDVR_PREFERENCES table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming rsdvr_preferences table                              #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id  rsdvr_preferences 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to INCOMING_RSDVR_PREFERENCES table"  
failure_message="Failed while loading data to INCOMING_RSDVR_PREFERENCES table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_preferences tmp table                              #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_tmp_modules.sh $key_param_id rsdvr_preferences 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_PREFERENCES_TMP table"  
failure_message="Failed while loading data to WORK_RSDVR_PREFERENCES_TMP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_preferences sequence table                         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_sequence_modules.sh $key_param_id rsdvr_preferences 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_PREFERENCES_SEQ table"  
failure_message="Failed while loading data to WORK_RSDVR_PREFERENCES_SEQ table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_preferences dedup table                            #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_dedup_modules.sh $key_param_id rsdvr_preferences 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_PREFERENCES_DEDUP table"  
failure_message="Failed while loading data to WORK_RSDVR_PREFERENCES_DEDUP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_preferences delta table                            #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_preferences_delta transform 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_PREFERENCES_DELTA table"  
failure_message="Failed while loading data to GOLD_RSDVR_PREFERENCES_DELTA table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_preferences last_rec table                         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_preferences_last_rec transform 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_PREFERENCES_LAST_REC table"  
failure_message="Failed while loading data to GOLD_RSDVR_PREFERENCES_LAST_REC table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_preferences delete table                           #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_delete_modules.sh $key_param_id rsdvr_preferences 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_PREFERENCES_DELETE table"  
failure_message="Failed while loading data to WORK_RSDVR_PREFERENCES_DELETE table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_preferences delete dedup table                     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_delete_dedup_modules.sh $key_param_id rsdvr_preferences 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_PREFERENCES_DELETE_DEDUP table"  
failure_message="Failed while loading data to WORK_RSDVR_PREFERENCES_DELETE_DEDUP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_preferences last_rec table                         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_preferences_last_rec delete 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_PREFERENCES_LAST_REC table"  
failure_message="Failed while loading data to GOLD_RSDVR_PREFERENCES_LAST_REC table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Delete from gold rsdvr_preferences_delta table                             #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_preferences_delta delete 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_PREFERENCES_DELTA table"  
failure_message="Failed while loading data to GOLD_RSDVR_PREFERENCES_DELTA table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Compute statistics For Preference table                                    #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/compute_statistics.sh $key_param_id rsdvr_preferences 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data after computing statistics for RSDVR_PREFERENCES table"  
failure_message="Failed while loading data for computed statistics for RSDVR_PREFERENCES table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            # 
#               Successfully Finished Preferences Table                      #
#                                                                            #
##############################################################################

##############################################################################
#																			 #
# Load data to work rsdvr_recordings table                                   #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_modules.sh $key_param_id  rsdvr_recordings 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_RECORDINGS table"  
failure_message="Failed while loading data to WORK_RSDVR_RECORDINGS table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming rsdvr_recordings table                               #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id rsdvr_recordings 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to INCOMING_RSDVR_RECORDINGS table"  
failure_message="Failed while loading data to INCOMING_RSDVR_RECORDINGS table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_recordings tmp table                               #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_tmp_modules.sh $key_param_id rsdvr_recordings 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_RECORDINGS_TMP table"  
failure_message="Failed while loading data to WORK_RSDVR_RECORDINGS_TMP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_recordings sequence table                          #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_sequence_modules.sh $key_param_id rsdvr_recordings 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_RECORDINGS_SEQ table"  
failure_message="Failed while loading data to WORK_RSDVR_RECORDINGS_SEQ table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_recordings dedup table                             #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_dedup_modules.sh $key_param_id rsdvr_recordings 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_RECORDINGS_DEDUP table"  
failure_message="Failed while loading data to WORK_RSDVR_RECORDINGS_DEDUP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_recordings delta table                             #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_recordings_delta transform 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_RECORDINGS_DELTA table"  
failure_message="Failed while loading data to GOLD_RSDVR_RECORDINGS_DELTA table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_recordings last_rec table                          #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_recordings_last_rec transform 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_RECORDINGS_LAST_REC table"  
failure_message="Failed while loading data to GOLD_RSDVR_RECORDINGS_LAST_REC table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_recordings delete table                            #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_delete_modules.sh $key_param_id rsdvr_recordings 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_RECORDINGS_DELETE table"  
failure_message="Failed while loading data to WORK_RSDVR_RECORDINGS_DELETE table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_recordings delete dedup table                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_delete_dedup_modules.sh $key_param_id rsdvr_recordings 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_RECORDINGS_DELETE_DEDUP table"  
failure_message="Failed while loading data to WORK_RSDVR_RECORDINGS_DELETE_DEDUP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_recordings last_rec table                          #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_recordings_last_rec delete 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_RECORDINGS_LAST_REC table"  
failure_message="Failed while loading data to GOLD_RSDVR_RECORDINGS_LAST_REC table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Delete from gold rsdvr_recordings_delta table                             #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_recordings_delta delete 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_RECORDINGS_DELTA table"  
failure_message="Failed while loading data to GOLD_RSDVR_RECORDINGS_DELTA table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Compute statistics For rsdvr_recordings table                              #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/compute_statistics.sh $key_param_id rsdvr_recordings 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data after computing statistics for RSDVR_RECORDINGS table"  
failure_message="Failed while loading data for computed statistics for RSDVR_RECORDINGS table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            # 
#               Successfully Finished Recordings Table                       #
#                                                                            #
##############################################################################


##############################################################################
#																			 #
# Load data to work rsdvr_schedule table                                     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_modules.sh $key_param_id  rsdvr_schedule 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_SCHEDULE table"  
failure_message="Failed while loading data to WORK_RSDVR_SCHEDULE table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming rsdvr_schedule table                                 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id rsdvr_schedule 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to INCOMING_RSDVR_SCHEDULE table"  
failure_message="Failed while loading data to INCOMING_RSDVR_SCHEDULE table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_schedule tmp table                                 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_tmp_modules.sh $key_param_id rsdvr_schedule 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_SCHEDULE_TMP table"  
failure_message="Failed while loading data to WORK_RSDVR_SCHEDULE_TMP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_schedule sequence table                            #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_sequence_modules.sh $key_param_id rsdvr_schedule 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_SCHEDULE_SEQ table"  
failure_message="Failed while loading data to WORK_RSDVR_SCHEDULE_SEQ table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_schedule dedup table                               #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_dedup_modules.sh $key_param_id rsdvr_schedule 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_SCHEDULE_DEDUP table"  
failure_message="Failed while loading data to WORK_RSDVR_SCHEDULE_DEDUP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_schedule delta table                               #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_schedule_delta transform 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_SCHEDULE_DELTA table"  
failure_message="Failed while loading data to GOLD_RSDVR_SCHEDULE_DELTA table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_schedule last_rec table                            #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_schedule_last_rec transform 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_SCHEDULE_LAST_REC table"  
failure_message="Failed while loading data to GOLD_RSDVR_SCHEDULE_LAST_REC table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_schedule delete table                              #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_delete_modules.sh $key_param_id rsdvr_schedule 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_SCHEDULE_DELETE table"  
failure_message="Failed while loading data to WORK_RSDVR_SCHEDULE_DELETE table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_schedule delete dedup table                        #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_delete_dedup_modules.sh $key_param_id rsdvr_schedule 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_SCHEDULE_DELETE_DEDUP table"  
failure_message="Failed while loading data to WORK_RSDVR_SCHEDULE_DELETE_DEDUP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_schedule last_rec table                            #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_schedule_last_rec delete 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_SCHEDULE_LAST_REC table"  
failure_message="Failed while loading data to GOLD_RSDVR_SCHEDULE_LAST_REC table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Delete from gold rsdvr_schedule_delta table                                #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_schedule_delta delete 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_SCHEDULE_LAST_REC table"  
failure_message="Failed while loading data to GOLD_RSDVR_SCHEDULE_LAST_REC table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Compute statistics For rsdvr_schedule table                                #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/compute_statistics.sh $key_param_id rsdvr_schedule 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data after computing statistics for RSDVR_SCHEDULE table"  
failure_message="Failed while loading data for computed statistics for RSDVR_SCHEDULE table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            # 
#               Successfully Finished Schedule Table                         #
#                                                                            #
##############################################################################

##############################################################################
#																			 #
# Load data to work rsdvr_requests table                                     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_modules.sh $key_param_id  rsdvr_requests 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_REQUESTS table"  
failure_message="Failed while loading data to WORK_RSDVR_REQUESTS table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming rsdvr_requests table                                 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id rsdvr_requests 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to INCOMING_RSDVR_REQUESTS table"  
failure_message="Failed while loading data to INCOMING_RSDVR_REQUESTS table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_requests tmp table                                 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_tmp_modules.sh $key_param_id rsdvr_requests 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_REQUESTS_TMP table"  
failure_message="Failed while loading data to WORK_RSDVR_REQUESTS_TMP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_requests sequence table                            #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_sequence_modules.sh $key_param_id rsdvr_requests 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_REQUESTS_SEQ table"  
failure_message="Failed while loading data to WORK_RSDVR_REQUESTS_SEQ table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_requests dedup table                               #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_dedup_modules.sh $key_param_id rsdvr_requests 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_REQUESTS_DEDUP table"  
failure_message="Failed while loading data to WORK_RSDVR_REQUESTS_DEDUP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_requests delta table                               #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_requests_delta transform 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_REQUESTS_DELTA table"  
failure_message="Failed while loading data to GOLD_RSDVR_REQUESTS_DELTA table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_requests last_rec table                            #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_requests_last_rec transform 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_REQUESTS_LAST_REC table"  
failure_message="Failed while loading data to GOLD_RSDVR_REQUESTS_LAST_REC table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_requests delete table                              #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_delete_modules.sh $key_param_id rsdvr_requests 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_REQUESTS_DELETE table"  
failure_message="Failed while loading data to WORK_RSDVR_REQUESTS_DELETE table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_requests delete dedup table                        #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_delete_dedup_modules.sh $key_param_id rsdvr_requests 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_REQUESTS_DELETE_DEDUP table"  
failure_message="Failed while loading data to WORK_RSDVR_REQUESTS_DELETE_DEDUP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_requests last_rec table                            #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_requests_last_rec delete 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_REQUESTS_LAST_REC table"  
failure_message="Failed while loading data to GOLD_RSDVR_REQUESTS_LAST_REC table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Delete from gold rsdvr_requests_delta table                                #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_requests_delta delete 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_REQUESTS_DELTA table"  
failure_message="Failed while loading data to GOLD_RSDVR_REQUESTS_DELTA table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Compute statistics For rsdvr_requests table                                #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/compute_statistics.sh $key_param_id rsdvr_requests 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data after computing statistics for RSDVR_REQUESTS table"  
failure_message="Failed while loading data for computed statistics for RSDVR_REQUESTS table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            # 
#               Successfully Finished Requests Table                         #
#                                                                            #
##############################################################################

##############################################################################
#																			 #
# Load data to work rsdvr_recordings_archive table                           #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_modules.sh $key_param_id  rsdvr_recordings_archive 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_RECORDINGS_ARCHIVE table"  
failure_message="Failed while loading data to WORK_RSDVR_RECORDINGS_ARCHIVE table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming rsdvr_recordings_archive table                       #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id rsdvr_recordings_archive 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to incoming RSDVR_RECORDINGS_ARCHIVE table"  
failure_message="Failed while loading data to incoming RSDVR_RECORDINGS_ARCHIVE table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_recordings_archive tmp table                       #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_tmp_modules.sh $key_param_id rsdvr_recordings_archive 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_RECORDINGS_ARCHIVE_TMP table"  
failure_message="Failed while loading data to WORK_RSDVR_RECORDINGS_ARCHIVE_TMP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_recordings_archive sequence table                  #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_sequence_modules.sh $key_param_id rsdvr_recordings_archive 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_RECORDINGS_ARCHIVE_SEQ table"  
failure_message="Failed while loading data to WORK_RSDVR_RECORDINGS_ARCHIVE_SEQ table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_recordings_archive dedup table                     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_dedup_modules.sh $key_param_id rsdvr_recordings_archive 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_RECORDINGS_ARCHIVE_DEDUP table"  
failure_message="Failed while loading data to WORK_RSDVR_RECORDINGS_ARCHIVE_DEDUP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_recordings_archive delta table                     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_recordings_archive_delta transform 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_RECORDINGS_ARCHIVE_DELTA table"  
failure_message="Failed while loading data to GOLD_RSDVR_RECORDINGS_ARCHIVE_DELTA table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_recordings_archive last_rec table                  #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_recordings_archive_last_rec transform 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC table"  
failure_message="Failed while loading data to GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_recordings_archive delete table                    #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_delete_modules.sh $key_param_id rsdvr_recordings_archive 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_RECORDINGS_ARCHIVE_DELETE table"  
failure_message="Failed while loading data to WORK_RSDVR_RECORDINGS_ARCHIVE_DELETE table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_recordings_archive delete dedup table              #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_delete_dedup_modules.sh $key_param_id rsdvr_recordings_archive 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_RECORDINGS_ARCHIVE_DELETE_DEDUP table"  
failure_message="Failed while loading data to WORK_RSDVR_RECORDINGS_ARCHIVE_DELETE_DEDUP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_recordings_archive last_rec table                  #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_recordings_archive_last_rec delete 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC table"  
failure_message="Failed while loading data to GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Delete from gold rsdvr_recordings_archive_delta table                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_recordings_archive_delta delete 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_RECORDINGS_ARCHIVE_DELTA table"  
failure_message="Failed while loading data to GOLD_RSDVR_RECORDINGS_ARCHIVE_DELTA table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Compute statistics For rsdvr_recordings_archive table                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/compute_statistics.sh $key_param_id rsdvr_recordings_archive 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data after computing statistics for RSDVR_RECORDINGS_ARCHIVE table"  
failure_message="Failed while loading data for computed statistics for RSDVR_RECORDINGS_ARCHIVE table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            # 
#               Successfully Finished Recordings_Archive Table               #
#                                                                            #
##############################################################################

##############################################################################
#																			 #
# Load data to work rsdvr_requests_archive table                             #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_modules.sh $key_param_id  rsdvr_requests_archive 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_REQUESTS_ARCHIVE table"  
failure_message="Failed while loading data to WORK_RSDVR_REQUESTS_ARCHIVE table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming rsdvr_requests_archive table                         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $key_param_id rsdvr_requests_archive 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to INCOMING_RSDVR_REQUESTS_ARCHIVE table"  
failure_message="Failed while loading data to INCOMING_RSDVR_REQUESTS_ARCHIVE table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_requests_archive tmp table                         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_tmp_modules.sh $key_param_id rsdvr_requests_archive 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_REQUESTS_ARCHIVE_TMP table"  
failure_message="Failed while loading data to WORK_RSDVR_REQUESTS_ARCHIVE_TMP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_requests_archive sequence table                    #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_sequence_modules.sh $key_param_id rsdvr_requests_archive 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_REQUESTS_ARCHIVE_SEQ table"  
failure_message="Failed while loading data to WORK_RSDVR_REQUESTS_ARCHIVE_SEQ table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_requests_archive dedup table                       #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_dedup_modules.sh $key_param_id rsdvr_requests_archive 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_REQUESTS_ARCHIVE_DEDUP table"  
failure_message="Failed while loading data to WORK_RSDVR_REQUESTS_ARCHIVE_DEDUP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_requests_archive delta table                       #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_requests_archive_delta transform 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_REQUESTS_ARCHIVE_DELTA table"  
failure_message="Failed while loading data to GOLD_RSDVR_REQUESTS_ARCHIVE_DELTA table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_requests_archive last_rec table                    #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_requests_archive_last_rec transform 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC table"  
failure_message="Failed while loading data to GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_requests_archive delete table                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_delete_modules.sh $key_param_id rsdvr_requests_archive 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_REQUESTS_ARCHIVE_DELETE table"  
failure_message="Failed while loading data to WORK_RSDVR_REQUESTS_ARCHIVE_DELETE table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to work rsdvr_requests_archive delete dedup table                #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_work_delete_dedup_modules.sh $key_param_id rsdvr_requests_archive 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to WORK_RSDVR_REQUESTS_ARCHIVE_DELETE_DEDUP table"  
failure_message="Failed while loading data to WORK_RSDVR_REQUESTS_ARCHIVE_DELETE_DEDUP table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to gold rsdvr_requests_archive last_rec table                    #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_requests_archive_last_rec delete 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC table"  
failure_message="Failed while loading data to GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Delete from gold rsdvr_requests_archive_delta table                        #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $key_param_id rsdvr_requests_archive_delta delete 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data to GOLD_RSDVR_REQUESTS_ARCHIVE_DELTA table"  
failure_message="Failed while loading data to GOLD_RSDVR_REQUESTS_ARCHIVE_DELTA table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Compute statistics For rsdvr_requests_archive table                        #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/compute_statistics.sh $key_param_id rsdvr_requests_archive 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
success_message="Successfully loaded data after computing statistics for RSDVR_REQUESTS_ARCHIVE table"  
failure_message="Failed while loading data for computed statistics for RSDVR_REQUESTS_ARCHIVE table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            # 
#               Successfully Finished Requests_Archive Table                 #
#                                                                            #
##############################################################################

##############################################################################
#																			 #
#          Update Key_Params for all tables                                  #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/update_key_params.sh $key_param_id 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully updated key_params table"
failure_message="Failed while updating key_params table, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                    End                                     #
##############################################################################