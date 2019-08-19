#!/bin/bash

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: hulu_functions.sh  
#   Program type: Unix Bash Shell script
#   Purpose:    : This script consists of all the utility functions that are used by    
#                 hulu module scripts. Modules shall not call any hadoop related commands    
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
#   Date        : 03/30/2016
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
#    1.0     DataMetica Team          03/30/2016       Initial version
#
#
#####################################################################################################################

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

###
# Verify if the variable is not-set/empty or not. In case it is non-set or empty,
# exit with failure message.
#
# Arguments: variable_name* - name of the variable to be check for being not-set/empty
#   	     variable_value* - value of the variable
#

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
# Function to get key params for hulu 										   #
#                                                                              #
################################################################################

function fn_get_hulu_params(){

   key_param_id=$1

   if [ ! -z "${key_param_id}" ]
   then
      export PGPASSWORD=${POSTGRES_PASSWORD};
      get_params=`psql -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USERNAME} \
         -P t -P format=unaligned ${POSTGRES_DATABASE_NAME} -c "SELECT PARAM_DTM_START FROM KEY_PARAMS \
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
# Function to update key params for hulu 										   #
#                                                                              #
################################################################################

function fn_update_hulu_params(){

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

################################################################################
#                                     End                                      #
################################################################################