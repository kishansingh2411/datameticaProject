#!/bin/bash

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: omniture_functions.sh  
#   Program type: Unix Bash Shell script
#   Purpose:    : This script consists of all the utility functions that are used by    
#                 omniture module scripts. Modules shall not call and hadoop related commands    
#                 directly but instead write a wrapper function in this script with     
#                 proper handling of exit codes of the commands.  
#                  Note:                                                                        
#                       1) Variables defined as final in the document must not be modified    
#                          by the module script code.                                         
#                       2) If function argument ends with * then it means that required       
#                          argument.                                                          
#                       3) If function argument ends with ? then it means that optional       
#                          argument.                                                          
#   Author(s)   : DataMetica Team
#   Usage       : 
#   Date        : 09/12/2016
#   Log File    : 
#   SQL File    :                                 
#   Error File  : 
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

###                                                                           
source $PROJECT_HOME/bin/functions.sh

################################################################################
#                                                                              #
# Function to check Suite Name                                                 #
#                                                                              #
################################################################################

function fn_check_suite_name(){
suite_name=$1

   if [[ $suite_name == vow ]] || 
      [[ $suite_name == vow-esp ]] || 
      [[ $suite_name == onet_prod ]] || 
      [[ $suite_name == onet_prod_esp ]] || 
      [[ $suite_name == cablevis-uow-com ]] ||
      [[ $suite_name == $CHANNEL900 ]] ||
      [[ $suite_name == stock-97 ]] ||
      [[ $suite_name == stock-98 ]] ||
      [[ $suite_name == stock-99 ]] ||
      [[ $suite_name == stock-pcmac ]] ||
      [[ $suite_name == $ACCESS ]] ||
      [[ $suite_name == opt_app_to_netezza_export ]]
      
    then
          echo "Correct suite name '$suite_name' provided"
    else
         echo "Suite name '$suite_name' is incorrect."
         echo "Suite name must be one of [vow or vow-esp or onet_prod or onet_prod_esp or cablevis-uow-com or stock-36]"
    exit
   fi
}


################################################################################
#                                                                              #
# Function to create job name from suite name                                  #
#                                                                              #
################################################################################

function fn_generate_job_name(){
key_param_id=$1
suite_name="$2"
job_name=$3

   if [[ $suite_name == vow ]] ||
      [[ $suite_name == vow-esp ]] ||
      [[ $suite_name == onet_prod ]] ||
      [[ $suite_name == onet_prod_esp ]] ||
      [[ $suite_name == cablevis-uow-com ]] ||
      [[ $suite_name == $CHANNEL900 ]] ||
      [[ $suite_name == stock-97 ]] ||
      [[ $suite_name == stock-98 ]] ||
      [[ $suite_name == stock-99 ]] ||
      [[ $suite_name == stock-pcmac ]] ||
      [[ $suite_name == $ACCESS ]] ||
      [[ $suite_name == opt_app_to_netezza_export ]]
    then
  suite_name=$(echo "$suite_name" | tr '[:lower:]' '[:upper:]')
  echo "APP_"$key_param_id""_""$suite_name""_""$job_name""
  else
         echo "Suite name '$suite_name' is incorrect."
         echo "Suite name must be one of [vow or vow-esp or onet_prod or onet_prod_esp or cablevis-uow-com or stock-36]"
    exit
   fi
}


#########################################################################################
#                                                                                       #
# Verify if the variable is not-set/empty or not. In case it is non-set or empty,       #
# exit with failure message.															#
#																						#
# Arguments: variable_name* - name of the variable to be check for being not-set/empty  #
#   	     variable_value* - value of the variable									#
#                                                                                       #
#########################################################################################

function assert_variable_is_set(){
  variable_name=$1
  variable_value=$2

  if [ "${variable_value}" == "" ]
  then
    exit_code=${EXIT_CODE_VARIABLE_NOT_SET}

    failure_messages="Value for ${variable_name} variable is not set"
    fn_exit_with_failure_message "${exit_code}" "${failure_messages}" "${log_file}"
  fi
}

################################################################################
#                                                                              #
# Function to get omniture file name                                           #
#                                                                              #
################################################################################

function fn_get_omniture_source_file_name(){

	suite_data_file_path=$2
	suite_name_in_upper_case=`echo "$1" | tr '[:lower:]' '[:upper:]'`
	while read -r line
	do
	   suite_name_file=$(echo $line | cut -f 2 -d ',')
	   omniture_file_name=$(echo $line | cut -f 4 -d ',')
	   if [ "$suite_name_file" == "$suite_name_in_upper_case" ]
	   then
	        OMNITURE_SOURCE_FILE_NAME=$(echo $line | cut -f 4 -d ',')
	        break
	    fi
	done< $suite_data_file_path
}

################################################################################
#                                                                              #
# Function to get omniture params                                              #
#                                                                              #
################################################################################

function fn_get_omniture_params(){

   key_param_id=$1

   if [ ! -z "${key_param_id}" ]
   then
      export PGPASSWORD=${POSTGRES_PASSWORD};
      get_params=`psql -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USERNAME} \
         -P t -P format=unaligned ${POSTGRES_DATABASE_NAME} -c "SELECT PARAM_DTM_START|| '~' ||PARAM1 FROM KEY_PARAMS \
            WHERE KEY_PARAM_ID = $key_param_id;"`
      exit_code=$?
      
	  if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
	  then
		   return $exit_code
	  else
	      echo "${get_params}"
	  fi 
   fi  
}

################################################################################
#                                                                              #
# Function to update omniture params                                           #
#                                                                              #
################################################################################

function fn_update_omniture_params(){

key_param_id=$1

if [ ! -z "${key_param_id}" ]
then
export PGPASSWORD=${POSTGRES_PASSWORD};
export PGHOST=${POSTGRES_HOST};
export PGPORT=${POSTGRES_PORT};
psql ${POSTGRES_DATABASE_NAME} ${POSTGRES_USERNAME} << EOF
   UPDATE KEY_PARAMS SET PARAM_DTM_START = PARAM_DTM_START + (INCREMENTAL_IN_HRS * INTERVAL '1 HOUR'), \
   PARAM_DTM_END = DATE_TRUNC('SECOND',NOW()::TIMESTAMP), \
   DTM_LAST_UPDATED = DATE_TRUNC('SECOND',NOW()::TIMESTAMP) \
   WHERE KEY_PARAM_ID = $key_param_id;
EOF
exit_code=$?

fi

	return $exit_code
}

##############################################################################
#																			 #
# Function to validate the column header for hit_data						 #
#																			 #
# Parameter list    													     #
# 	1. Path for hit_data_file							                     #
# 	2. Path for column header file								             #
# 	3. suite name							                                 #
# 	4. source date								                             #
# 	5. log file path								                         #
#																			 #
##############################################################################

function fn_validate_column_header(){
suite_name=$1
path_for_hit_data_file=$2
path_for_column_header_file=$3
source_date=$4
log_file_path=$5

upper_suite_name=$(echo $suite_name | tr '[:lower:]' '[:upper:]')
data_hit_file_header_array=`hadoop fs -cat ${path_for_hit_data_file}/*column_headers* | sed 's/\t/,/g'`
column_header_array=`hadoop fs -cat ${path_for_column_header_file}/incoming_opt_tab_cols/*.txt | grep $upper_suite_name| sed 's/'"$upper_suite_name,"'/''/' | head -1`

for column_header in "${column_header_array[@]}" 
do
{
   for hit_data_header in "${data_hit_file_header_array[@]}"
   do
   {
      if [ ! $column_header == $hit_data_header ] 
      then             
         fn_hadoop_move_file_or_directory "${path_for_hit_data_file}.tar.gz" \
            "$DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_BADDATA" \
            "${BOOLEAN_TRUE}" "${BOOLEAN_FALSE}" "${log_file_path}"
            
         return 1
      fi
   }
   done
}
done
}

################################################################################
#                                     End                                      #
################################################################################