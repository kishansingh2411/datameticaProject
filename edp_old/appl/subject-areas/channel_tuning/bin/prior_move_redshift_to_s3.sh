#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_incoming_modules.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will load data in all Incoming tables.
#                 Input Arguments for this script are: "aws_job_id" "key_param_id"               
#   Author(s)   : DataMetica Team
#   Usage       : prior_move_redshift_to_s3.sh "aws_job_id" "key_param_id"  
#   Date        : 04/12/2016
#   Log File    : .../log/channel_tuning/${job_name}.log
#   SQL File    : 
#   Error File  : .../log/channel_tuning/${job_name}.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          04/12/2016       Initial version
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
#                                                                             #
# Checking number of parameter passed                                         #
#                                                                             #
##############################################################################

if [ "$#" -ne 2 ] 
then
   echo "Illegal number of parameters. Parameters expected are [ "aws_job_id" "key_param_id" ]"
   exit -1
fi

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

aws_job_id="$1"
key_param_id="$2"
job_name="CHN_"${key_param_id}"_"${aws_job_id}"_RDSHT_TO_S3"
start_time=`date +"%Y-%m-%d %H:%M:%S"`
fn_get_current_batch_id
log_file_path="${LOG_DIR}/${SUBJECT_AREA_NAME}/${job_name}.log"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

args="Need to verify AM AWS job "${job_name}" manually and inform if this job fails"

fn_insert_job_statistics "${BATCH_ID}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}" 

export PGPASSWORD=${POSTGRES_PASSWORD};
psql $POSTGRES_DATABASE_NAME $POSTGRES_USERNAME 1>> ${log_file_path} 2>> ${log_file_path} << EOF
	update job_statistics set args='$args' 
	where (batch_id='${BATCH_ID}' and job_name='${job_name}' and start_time='${start_time}');
EOF

##############################################################################
#                                    End                                     #
##############################################################################