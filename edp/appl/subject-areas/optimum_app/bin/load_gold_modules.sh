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
#   Purpose:    : This will load data in all Gold layer.                            
#                 Input Arguments for this script is: key_Param_id.               
#   Author(s)   : DataMetica Team
#   Usage       : sh load_gold_modules.sh  key_Param_id
#   Date        : 09/12/2016
#   Log File    : .../log/${suite_name}/${job_name}.log
#   SQL File    :                                 
#   Error File  : .../log/${suite_name}/${job_name}.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          09/12/2016       Initial version
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

######################################################################################
#                                                                                     #
#   Sourcing reference files                                                              #
#                                                                                     #
#######################################################################################
###                                                                           
# Load all environment properties files
source $PROJECT_HOME/etc/namespace.properties
source $PROJECT_HOME/etc/beeline.properties
source $PROJECT_HOME/etc/default.env.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/optimum_app.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/omniture_functions.sh

##############################################################################
#																			 #
#   Local Param 				 			     						     #
#                                    										 #
##############################################################################

key_param_id="$1"
suite_data_file_path=$SUBJECT_AREA_HOME/metadata/Optimum_Suite.txt

params="$(fn_get_omniture_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

source_date=$(echo $params | cut -f 1 -d '~'| cut -f 1 -d ' ')
suite_name=$(echo $params | cut -f 2 -d '~')
suite_name=$(echo "$suite_name" | tr '[:upper:]' '[:lower:]')

job_name="$(fn_generate_job_name "$key_param_id" "$suite_name" "LD_GOLD_MDULS")"

start_time=`date +"%Y-%m-%d %H:%M:%S"`
current_batch_id_path="${BATCH_ID_DIR}/$suite_name/current"
batch_id=`cat $current_batch_id_path`
log_file_path="${LOG_DIR_SUBJECT_AREA}/${suite_name}/${job_name}.log"

##############################################################################
#																			 #
# Checking mandatory Params                                                  #
#																			 #
##############################################################################

fn_check_mandatory_params "suite_name,$suite_name"
  
##############################################################################
#                                                                            #
# Checking Correct Suite Name                                                #
#                                                                            #
##############################################################################

fn_check_suite_name "$suite_name"
  
##############################################################################
#																			 #
# Checking date format		                                                 #
#																			 #
##############################################################################

fn_check_date_format $source_date

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}" 

##############################################################################
#                                                                            #
# Get suite id                                                               #
#                                                                            #
##############################################################################

while read -r line
do
   suite_name_file=$(echo $line | cut -f 2 -d ',')
   suite_name_file=$(echo $suite_name_file | tr '[:upper:]' '[:lower:]')
   if [ "$suite_name_file" == "$suite_name" ]
   then
        suite_id=$(echo $line | cut -f 1 -d ',')
        break
    fi
done< $suite_data_file_path

##############################################################################
#																			 #
# Loading Gold table                                                         #
#																			 #
##############################################################################

source_file_path=`hadoop fs -ls ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/$suite_name/*$source_date.tar* | awk '{print$8}'`
source_file_name=`basename $source_file_path`

fn_log_info "Executing for suite_name $suite_name" "${log_file_path}"

if [[ $suite_name == "$STOCK_97" ]] || [[ $suite_name == "$STOCK_98" ]] ||
     [[ $suite_name == "$STOCK_99" ]] || [[ $suite_name == "$STOCK_PCMAC" ]] ||
     [[ $suite_name == "$ACCESS" ]]
then
    table_name=${GOLD_OPT_APP_HIT_DATA_TBL}
    
	sh $SUBJECT_AREA_HOME/${table_name}_tbl/bin/module.sh "transform" "${suite_name}" \
	   "${suite_id}" "${source_date}" "${log_file_path}"
	exit_code=$?	
else
	fn_log_info "Invalid suite_name $suite_name provided!!!"  ${log_file_path}
	exit -1
fi

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
   fn_log_error "For suite $suite_name, failed to load gold table ${table_name}, Quitting the process" "${log_file_path}"
   fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name}" " "
fi   

fn_log_info "For suite $suite_name, successfully loaded table ${table_name}" "${log_file_path}"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

record_count=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
             "SELECT COUNT(*) FROM $HIVE_DATABASE_NAME_GOLD.${HIVE_TABLE_PREFIX}${table_name} \
              WHERE SUITE_NAME='${suite_name}'AND SOURCE_DATE='$source_date';"`
                           
fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "${record_count}" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name}" " "

##############################################################################
#                                    End                                     #
##############################################################################