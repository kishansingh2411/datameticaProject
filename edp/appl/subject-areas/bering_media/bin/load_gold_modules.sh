#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_gold_modules.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will move source data file into gold directory
#                 Input Arguments for this script are: source_timestamp (201510150100).               
#   Author(s)   : DataMetica Team
#   Usage       : sh load_gold_modules.sh "201510150100" 
#   Date        : 12/28/2015
#   Log File    : .../log/bering_media/BERING_MEDIA_MOVE_TO_GOLD_JOB.log
#   SQL File    : 
#   Error File  : .../log/bering_media/BERING_MEDIA_MOVE_TO_GOLD_JOB.log
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
PROJECT_HOME=`dirname ${SUBJECT_AREAS_HOME}`

###                                                                           
# Load all environment properties files
source $PROJECT_HOME/etc/namespace.properties
source $PROJECT_HOME/etc/beeline.properties
source $PROJECT_HOME/etc/default.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/etc/bering-media.properties

##############################################################################
#                                                                             #
# Checking number of parameter passed                                         #
#                                                                             #
##############################################################################


if [ "$#" -ne 1 ] 
then
   echo "Illegal number of parameters"
   exit 
fi

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

source_timestamp="$1"
source_data_date=`(echo $source_timestamp | cut -c1-8)`
source_data_hour=`(echo $source_timestamp | cut -c9,10)`
source_data_minutes=`(echo $source_timestamp | cut -c11,12)`
formatted_source_date=`date -d $source_data_date +%Y-%m-%d`
formatted_source_timestamp="$formatted_source_date $source_data_hour:$source_data_minutes:00"

current_batch_id_path="${BATCH_ID_DIR_SUBJECT_AREA}/current"
batch_id=`cat $current_batch_id_path`
job_name="BERING_MEDIA_MOVE_TO_GOLD_JOB"
start_time=`date +"%Y-%m-%d %H:%M:%S"`
log_file_path="${LOG_DIR_SUBJECT_AREA}/${job_name}.log"
source_file_path="${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/beringmedia_dhcp_log_${source_timestamp}.dat"
source_file_name=`basename ${source_file_path}`

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}" 

##############################################################################
#																			 #
# Loading Gold tables                                                        #
#																			 #
##############################################################################

tables=(${GOLD_T_F_BM_DHCP_IP_TBL})
for table in "${tables[@]}" 
do
   table_name=`basename ${table}`
        
   sh $SUBJECT_AREA_HOME/${table}_tbl/bin/module.sh "transform" "${source_timestamp}" "${log_file_path}"
   exit_code=$?   

   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
   then
      fn_log_error "Failed to transformed "${table_name}" in gold layer" "${log_file_path}"
	  fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "0" "${exit_code}" \
	     "${formatted_source_timestamp}" "${log_file_path}" "${source_file_name}" " "
   fi 

    fn_log_info "Successfully transformed ${table_name} in gold layer" "${log_file_path}"

done

##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

record_count=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e "SELECT COUNT(*) FROM $HIVE_DATABASE_NAME_GOLD.${HIVE_TABLE_PREFIX}${GOLD_T_F_BM_DHCP_IP_TBL} \
              WHERE TO_DATE(SOURCE_DATE)='${formatted_source_date}';"`

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "$record_count" "${exit_code}" \
   "${formatted_source_timestamp}" "${log_file_path}" "${source_file_name}" " "

##############################################################################
#                                    End                                     #
##############################################################################