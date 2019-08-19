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
#   Purpose:    : This will load data in Incoming table incoming_hulu_avn_tbl.                           
#                 Input Arguments for this script is: key_Param_id.               
#   Author(s)   : DataMetica Team
#   Usage       : sh load_incoming_modules.sh  "key_Param_id"
#   Date        : 03/30/2016
#   Log File    : .../log/hulu/HULU_600003_LD_INCOM_MDULS.log
#   SQL File    :                                 
#   Error File  : .../log/hulu/HULU_600003_LD_INCOM_MDULS.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          03/30/2016       Initial version
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
source $PROJECT_HOME/etc/namespace.properties
source $PROJECT_HOME/etc/default.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/etc/hulu.properties
source $SUBJECT_AREA_HOME/bin/hulu_functions.sh

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
#   Local Param 				 			     						     #
#                                    										 #
##############################################################################

key_param_id="$1"
job_name="HULU_600003_LD_INCOM_MDULS"

params="$(fn_get_hulu_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

source_date=$(date -d "$params" +%Y%m%d)

start_time=`date +"%Y-%m-%d %H:%M:%S"`
current_batch_id_path="${BATCH_ID_DIR}/hulu/current"
batch_id=`cat $current_batch_id_path`

log_file_path="${LOG_DIR_SUBJECT_AREA}/hulu/${job_name}.log"

source_file_path=`hadoop fs -ls ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/HULU_OTT-${source_date}*.tgz | awk '{print$8}'`
source_file_name=`basename $source_file_path`

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}"

##############################################################################
#																			 #
# Load incoming tables                                                       #
#																			 #
##############################################################################

tables=(${INCOMING_HULU_AVN_TBL})
for table in "${tables[@]}"; 
do
    table_name=`basename ${table}`
    source_file_path=${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/HULU_OTT-${source_date}*/HULU_OTT-REGISTRATIONS-${source_date}*.dat    
    sh $SUBJECT_AREA_HOME/${table}_tbl/bin/module.sh "transform" "${source_date}" "${source_file_path}" \
     "${log_file_path}"
    exit_code=$?   
    
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	 then
	    fn_log_error "Failed while transforming "${table_name}" in Incoming layer" "${log_file_path}"
		fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
	      "${source_date}" "${log_file_path}" "${source_file_name}" " "
	 fi 
    fn_log_info "Successfully transformed ${table_name} in Incoming layer" "${log_file_path}"

done

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

record_count=`hadoop fs -cat \
              ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/HULU_OTT-${source_date}*/HULU_OTT-REGISTRATIONS-${source_date}*.dat \
              | wc -l`

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "${record_count}" "${exit_code}" \
   "${source_date}" "${log_file_path}" "${source_file_name}" " "

##############################################################################
#                                    End                                     #
##############################################################################