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
#   Date        : 12/28/2015
#   Log File    : .../log/bering_media/EXECUTOR.log
#   SQL File    : 
#   Error File  : .../log/bering_media/EXECUTOR.log
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
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $HOME/bin/functions.sh

###############################################################################
#                                                                             #
# Checking number of parameter passed                                         #
#                                                                             #
###############################################################################

#if [ "$#" -ne 1 ]
#then
#  echo "Illegal number of parameters"
#  exit 
#fi

##############################################################################
# Local Params				 								 	 			 #
##############################################################################

current_timestamp=`date +%Y%m%d%H%M%S`
executor_log_file="${LOG_DIR_SUBJECT_AREA}/EXECUTOR.log"

ls ${LOCAL_DATA_DIRECTORY}/beringmedia_dhcp_log_*.dat | \
while read f
do 
	file_path=`ls $f`
	filename=`basename $file_path`
	if [ ! -z $filename ]
	then
		fn_log_info "Processing file [$filename]" ${executor_log_file}
		source_date=${filename:21:12}
		fn_log_info "Processing for source date [$source_date]" ${executor_log_file}

##############################################################################
#																			 #
# Generating batch_id for the process                    					 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/generate_batch_id.sh $source_date 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully generated batch id for source date $source_date"  
failure_message="Failed to generate batch id for source date $source_date, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#                                                                            #
# Copy data from local to landing                                            #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/move_to_landing.sh $source_date 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully copy source data files from local to landing directory"  
failure_message="Failed to copy source data files from local to landing directory, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Move to Incoming layer                                                     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules.sh $source_date 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully moved data to Incoming Layer"  
failure_message="Failed while moving data to Incoming Layer, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Move to Gold layer                                                         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_gold_modules.sh $source_date 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully moved data to Gold Layer"  
failure_message="Failed while moving data to Gold Layer, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

##############################################################################
#																			 #
# Move to Archive layer			                                             #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_archive_modules.sh ${source_date} 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

success_message="Successfully moved data to Archive Layer"  
failure_message="Failed while moving data to Archive Layer, Quitting the process." 
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${executor_log_file}"

fi
done
##############################################################################
#                                    End                                     #
##############################################################################