#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: update_unified_gold_data.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : This will load data in all Gold layer.                            
#                 Input Arguments for this script is: key_Param_id.               
#   Author(s)   : DataMetica Team
#   Usage       : sh load_gold_modules.sh  key_Param_id start_date end_date
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

###                                                                           
# Load all environment properties files
source $PROJECT_HOME/etc/namespace.properties
source $PROJECT_HOME/etc/beeline.properties
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

if [ "$#" -ne 3 ]
then
  echo "Illegal number of parameters.  Parameters expected [key_param_id start_date end_date]"
  exit 
fi

##############################################################################
#																			 #
#   Local Param 				 			     						     #
#                                    										 #
##############################################################################

key_param_id="$1"
start_date="$2"
end_date="$3"

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

job_name="UOW_600002_UPDATE_GOLD_MDULS"
suite_data_file_path=$SUBJECT_AREA_HOME/metadata/Optimum_Suite.txt

start_time=`date +"%Y-%m-%d %H:%M:%S"`
log_file_path="${LOG_DIR_SUBJECT_AREA}/${suite_name}/${job_name}.log"

if [[ $suite_name == "cablevis-uow-com" ]]
then
	table_name=${GOLD_UNIFIED_HIT_DATA_TBL}
else
	fn_log_info "Invalid suite_name $suite_name provided!!!"  ${log_file_path}
	exit -1
fi	    

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
# Updating Gold Unified table                                                #
#																			 #
##############################################################################

fromdate=$(date -I -d "$start_date") || exit -1

todate=$(date -I -d "$end_date")     || exit -1

update_date="$fromdate"

while [ "$(date -d "$update_date" +%Y%m%d)" -le "$(date -d "$todate" +%Y%m%d)" ]
do
	  
	sh $SUBJECT_AREA_HOME/${table_name}_tbl/bin/backfill_corp_house.sh "transform" "${suite_id}" "${suite_name}" "${update_date}" "${log_file_path}" 
	exit_code=$?	
	
	if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	then
	   fn_log_error "Failed to update table ${table_name} for partition ${update_date}, Quitting the process" "${log_file_path}"
	   exit -1
	fi   
	
	fn_log_info "For suite $suite_name, successfully updated table ${table_name} for partition ${update_date}" "${log_file_path}"
   
   update_date=$(date -I -d "$update_date + 1 day")
done

fn_log_info " Script completed successfully.." "${log_file_path}"

##############################################################################
#                                    End                                     #
##############################################################################