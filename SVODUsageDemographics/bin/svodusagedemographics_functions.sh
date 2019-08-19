#!/bin/bash
######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: cpe_functions.sh  
#   Program type: Unix Bash Shell script
#   Purpose:    : This script consists of all the utility functions that are used by    
#                 svod module scripts. Modules shall not call and hadoop related commands    
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
#   Date        : 03/20/2017
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
#    1.0     DataMetica Team          03/20/2017      Initial version
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
#source $SUBJECT_AREA_HOME/bin/svod_functions.sh
#source $PROJECT_HOME/common/etc/namespace.properties                                                            
#source $PROJECT_HOME/common/etc/default.env.properties

################################################################################
#                                                                              #
# Function to update svod params                                                #
#                                                                              #
################################################################################

function fn_update_svod_params(){

key_param_id=$1
param2=$2

if [ ! -z "${key_param_id}" ]
then
export PGPASSWORD=${POSTGRES_PASSWORD};
export PGHOST=${POSTGRES_HOST};
export PGPORT=${POSTGRES_PORT};
psql ${POSTGRES_DATABASE_NAME} ${POSTGRES_USERNAME} << EOF
   UPDATE KEY_PARAMS SET PARAM_DTM_START = PARAM_DTM_START + (INCREMENTAL_IN_HRS * INTERVAL '1 HOUR'), \
   PARAM_DTM_END = PARAM_DTM_START + (INCREMENTAL_IN_HRS * INTERVAL '1 HOUR'), \
   DTM_LAST_UPDATED = DATE_TRUNC('SECOND',NOW()::TIMESTAMP), \
    param2=$param2 \
   WHERE KEY_PARAM_ID = $key_param_id;
EOF
exit_code=$?

fi

	return $exit_code
}

################################################################################
#                                                                              #
# Function to get max and min date                       					   #
#                                                                              #
################################################################################

function fn_get_max_date(){
firstDayStr=$1
firstDay=$(date -d ${firstDayStr} +'%Y-%m-%d')
current_date=$(date +'%Y-%m-%d')
lastDay=$(date -d "${firstDay} +1 month - 1 day" +%Y-%m-%d)

#hdfs dfs -rm -r /incoming/svodusagedemo/encrypted_kom_vod_order/month_id=${month_id}
#hive -e "ALTER TABLE incoming.kom_vod_order ADD IF NOT EXISTS PARTITION(month_id=${month_id});"
##start_date=$(hive -e "SELECT TO_DATE(MAX(dtm_created)) FROM incoming.kom_vod_order WHERE month_id=${month_id};")  2>&1

#if [ -z "$start_date" ]; then
 #       echo "start date or max date from kom_vod_order for the given month id is empty or null"
  #      start_date=${firstDay}
#else
 #       echo "start date is not empty or null"
#fi



#end_date=$(date +%Y-%m-%d)
end_date=${lastDay}

echo $end_date
}

################################################################################
#                                                                              #
# Function to get key params for svod                     					   #
#                                                                              #
################################################################################

function fn_get_svod_params(){
   key_param_id=$1

   if [ ! -z "${key_param_id}" ]
   then
      export PGPASSWORD=${POSTGRES_PASSWORD};
      get_params=`psql -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USERNAME} \
                  -P t -P format=unaligned ${POSTGRES_DATABASE_NAME} \
                  -c "SELECT PARAM_DTM_START::TIMESTAMP::DATE|| '~' ||(PARAM_DTM_START + (INCREMENTAL_IN_HRS * INTERVAL '1 HOUR'))::DATE|| '~'||PARAM1|| '~'||PARAM2 \
                      FROM KEY_PARAMS \
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
