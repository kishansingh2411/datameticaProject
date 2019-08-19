#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_smith_modules.sh 
#   Program type: Unix Bash Shell script
#   Purpose:    : This will load data in all Smith layer.
#                 Input Arguments for this script is: key_Param_id.                                                       
#   Author(s)   : DataMetica Team
#   Usage       : sh load_smith_modules.sh  key_Param_id
#   Date        : 12/28/2015
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

# Set module, project, subject area home paths.
SUBJECT_AREA_HOME=`dirname ${SCRIPT_HOME}`
SUBJECT_AREAS_HOME=`dirname ${SUBJECT_AREA_HOME}`
PROJECT_HOME=`dirname ${SUBJECT_AREAS_HOME}`

# Load all environment properties files
source $PROJECT_HOME/etc/namespace.properties
source $PROJECT_HOME/etc/default.env.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/omniture.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/omniture_functions.sh

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

if [ $suite_name == "$CHANNEL900" ]
then
   job_name="STK36_600005_LD_SMITH_MDULS"
elif [ $suite_name == "cablevis-uow-com" ]
then
   job_name="UOW_600002_LD_SMITH_MDULS"
else
	echo "Invalid suite_name provided"
	exit
fi

start_time=`date +"%Y-%m-%d %H:%M:%S"`
current_batch_id_path="${BATCH_ID_DIR}/$suite_name/current"
batch_id=`cat $current_batch_id_path`
executor_log_file=${LOG_DIR_SUBJECT_AREA}/${suite_name}/EXECUTOR_${key_param_id}.log
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

source_file_path=`hadoop fs -ls \
                  ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/$suite_name/*$source_date.tar* | \
                  awk '{print$8}'`

source_file_name=`basename $source_file_path`

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
#                                                                            #
# Populate smith fact tables                                                 #
#                                                                            #
##############################################################################

fn_log_info "Executing for suite_name $suite_name" "${log_file_path}"

if [[ $suite_name == "vow" ]]
then
    table_name=${SMITH_VOW_OPTIMUM_USAGE_TBL}
elif [[ $suite_name == "vow-esp" ]]
then
    table_name=${SMITH_VOW_ESP_OPTIMUM_USAGE_TBL}
elif [[ $suite_name == "onet_prod" ]]
then
    table_name=${SMITH_ONET_PROD_OPTIMUM_USAGE_TBL}
elif [[ $suite_name == "onet_prod_esp" ]]
then
    table_name=${SMITH_ONET_PROD_ESP_OPTIMUM_USAGE_TBL}
elif [[ $suite_name == "cablevis-uow-com" ]]
then
    table_name=("${SMITH_KOM_UNIFIED_OPTIMUM_USAGE_TBL}" \
                "${SMITH_MKTG_UNIFIED_OPTIMUM_USAGE_TBL}"
                )
elif [[ $suite_name == "$CHANNEL900" ]]
then
    table_name=${SMITH_KOM_STOCK36_OPTIMUM_USAGE_TBL}
else
	fn_log_info "Invalid suite_name $suite_name provided!!!"  ${log_file_path}
fi
for table in "${table_name[@]}"
do 
	sh $SUBJECT_AREA_HOME/${table}_tbl/bin/module.sh "transform" "${suite_id}" "${source_date}" "${log_file_path}" "${suite_name}"
	exit_code="$?"
	
	if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	then
	   fn_log_error "For suite $suite_name, failed to load smith fact table ${table}, Quitting the process" "${log_file_path}"
	   fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" \
	   "${log_file_path}" "${source_file_name}" " "
	fi
	fn_log_info "For suite $suite_name, successfully loaded smith fact table ${table}" "${log_file_path}"
done

fn_log_info "For suite $suite_name, successfully loaded all smith fact tables" "${log_file_path}"

##############################################################################
#                                                                            #
# Add partition to Smith Fact tables                                         #
#                                                                            #
##############################################################################

sh $SUBJECT_AREA_HOME/${table_name}_tbl/bin/module.sh "prepare" "${source_date}" "${log_file_path}"
exit_code="$?"

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "Failed to add partition in smith fact table ${table_name}, quitting the process" "${log_file_path}"
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" \
	  "${log_file_path}" "${source_file_name}" " "
fi

fn_log_info "For suite $suite_name, successfully added partition in smith fact table ${table_name}" "${log_file_path}"

##############################################################################
#																			 #
# Load smith lookup tables     					                             #
#																			 #
##############################################################################

if [ $suite_name == "$CHANNEL900" ]
then
	tables=(${SMITH_BROWSER_TBL} \
	        ${SMITH_CONNECTION_TYPE_TBL} \
	        ${SMITH_COUNTRY_TBL} \
	        ${SMITH_JAVASCRIPT_VERSION_TBL} \
	        ${SMITH_LANGUAGES_TBL} \
	        ${SMITH_OPERATING_SYSTEMS_TBL} \
	        ${SMITH_PLUGINS_TBL} \
	        ${SMITH_SEARCH_ENGINES_TBL} \
	)
else
	tables=(${SMITH_BROWSER_TBL} \
	        ${SMITH_CONNECTION_TYPE_TBL} \
	        ${SMITH_COUNTRY_TBL} \
	        ${SMITH_JAVASCRIPT_VERSION_TBL} \
	        ${SMITH_LANGUAGES_TBL} \
	        ${SMITH_OPERATING_SYSTEMS_TBL} \
	        ${SMITH_PLUGINS_TBL} \
	        ${SMITH_REFERRER_TYPE_TBL} \
	        ${SMITH_SEARCH_ENGINES_TBL} \
	        )
fi
	
for table in "${tables[@]}"
do
   table_name=`basename ${table}`
   
   sh $SUBJECT_AREA_HOME/${table}_tbl/bin/module.sh "transform" "${suite_id}" "${log_file_path}"
   exit_code=$?   
    
   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
   then
      fn_log_error "Failed to load $table_name table, quitting the process" "${log_file_path}"
	  fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name}" " "
	fi
	fn_log_info "For suite $suite_name, successfully loaded $table_name table " "${log_file_path}"	    
done

fn_log_info "For suite $suite_name, successfully loaded all smith lookup tables" "${log_file_path}"

##############################################################################
#																			 #
# Load smith derived lookup tables											 #	
#																			 #	
##############################################################################

table=${SMITH_DERIVED_LOOKUP_TBL}

sh $SUBJECT_AREA_HOME/${table}_tbl/bin/module.sh "transform" "${suite_name}" "${suite_id}" "${source_date}" "${log_file_path}"
exit_code=$?
    
if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then
	fn_log_error "For suite $suite_name, failed to load smith derived lookup tables, Quitting the process" "${log_file_path}"
	fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name}" " "
fi

fn_log_info "For suite $suite_name, successfully loaded all smith derived lookup tables" "${log_file_path}"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name}" " "

##############################################################################
#                                    End                                     #
##############################################################################