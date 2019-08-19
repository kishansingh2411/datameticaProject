#!/bin/bash

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: mrdvr_functions.sh  
#   Program type: Unix Bash Shell script
#   Purpose:    : This script consists of all the utility functions that are used by    
#                 channel tunning module scripts. Modules shall not call and hadoop related commands    
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
#   Date        : 04/10/2016
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
#    1.0     DataMetica Team          04/10/2016       Initial version
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
# Function to import data directly to hive table                               #
#                                                                              #
################################################################################

function fn_import_table_to_hive_from_netezza(){
   connection_url=$1
   query="$2"
   column_name=$3
   target_dir=$4
   log_file_path=$5
   mappers=$6
   netezza_driver=$7

	sqoop import \
	  --direct \
      --connect "${connection_url}" \
      --username "${USERNAME}" \
      --password "${PASSWORD}" \
      --driver "${netezza_driver}" \
      --query "${query}" \
      --target-dir "${target_dir}" \
      --fields-terminated-by '~' \
      --split-by "${column_name}" \
      -m "${mappers}" >> ${log_file_path} 2>&1
   exit_code="$?"      
   return ${exit_code}  
}


################################################################################
#                                                                              #
# Function to build query for sqoop import                                     #
#                                                                              #
################################################################################

function fn_build_query_for_import(){
	source_table=$1
	database=$2
	column_name=$3	
	start_date=$4
	end_date=$5
	
	   query="SELECT \
                 * \
              FROM  \
              ${database}.${source_table}  \
              WHERE \
                ${column_name} >= '${start_date}' and \
                ${column_name} < '${end_date}' \
                 AND \$CONDITIONS"
	echo "$query"
}
################################################################################
#                                                                              #
# Function to get key params for mrdvr 										   #
#                                                                              #
################################################################################

function fn_get_channel_tuning_params(){
   key_param_id=$1

   if [ ! -z "${key_param_id}" ]
   then
      export PGPASSWORD=${POSTGRES_PASSWORD};
      get_params=`psql -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USERNAME} \
                  -P t -P format=unaligned ${POSTGRES_DATABASE_NAME} \
                  -c "SELECT PARAM_DTM_START::TIMESTAMP::DATE|| '~' ||(PARAM_DTM_START + (COALESCE(INCREMENTAL_IN_HRS,0) * INTERVAL '1 HOUR'))::DATE FROM KEY_PARAMS \
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
# Function to update key params for Channel_tuning							   #
#                                                                              #
################################################################################

function fn_update_channel_tuning_params(){
	key_param_id=$1
	
if [ ! -z "${key_param_id}" ]
then
   export PGPASSWORD=${POSTGRES_PASSWORD};
   export PGHOST=${POSTGRES_HOST};
   export PGPORT=${POSTGRES_PORT};
		psql ${POSTGRES_DATABASE_NAME} ${POSTGRES_USERNAME} << EOF
		   UPDATE KEY_PARAMS SET PARAM_DTM_START = PARAM_DTM_START + (COALESCE(INCREMENTAL_IN_HRS,0) * INTERVAL '1 HOUR'), \
		   PARAM_DTM_END = PARAM_DTM_END + (COALESCE(INCREMENTAL_IN_HRS,0) * INTERVAL '1 HOUR'), \
		   DTM_LAST_UPDATED = DATE_TRUNC('SECOND',NOW()::TIMESTAMP) \
		   WHERE KEY_PARAM_ID = $key_param_id;
EOF
exit_code=$?

fi

return $exit_code
}
        

################################################################################
#                                                                              #
# Function to get netezza count   											   #
#                                                                              #
################################################################################

function fn_validate_counts(){

   source_table=$1
   database=$2
   start_date=$3
   end_date=$4
   coulumn_name=$5
   netezza_database=$6
   netezza_driver=$7
	
	   query="SELECT \
              COUNT(*) AS REC_COUNT \
              FROM  \
              ${database}.${source_table}  \
              WHERE \
                ${column_name} >= '${start_date}' and \
                ${column_name} < '${end_date}'"

                                       
   netezza_count=`sqoop eval --connect "${DRIVER}://${HOST}:${PORT}/${netezza_database}" \
            					--username "${USERNAME}" \
      							--password "${PASSWORD}" \
      							--driver "${netezza_driver}" \
      							--query "${query}"`
   exit_code=$?
   
   if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   then
	   return $exit_code
   fi
   
   netezza_count=`echo ${netezza_count} | awk -F "REC_COUNT" '{print$2}' | cut -d' ' -f5`
   echo "${netezza_count}"
   
}

################################################################################
#                                                                              #
# Function to load target tables    										   #
#                                                                              #
################################################################################

fn_load_target_tables(){

	target_hive_database=$1
    target_table=$2
    src_hive_database=$3
    src_table=$4
    hive_script_path=$5
    log_file_path=$6
    hiveserver2_url=$7
    hive_usename=$8
    hive_password=$9
    filter_column=${10}
    
    beeline -u "${hiveserver2_url}" -n ${hive_usename} -p ${hive_password} \
      -hivevar filter_column=${filter_column} \
      -hivevar src_hive_database=${src_hive_database} \
      -hivevar target_hive_database=${target_hive_database} \
      -hivevar src_table="${src_table}" \
      -hivevar target_table=${target_table} \
      -f ${hive_script_path} >> ${log_file_path} 2>&1;
      exit_code=$?
      
      return $exit_code
}

################################################################################
#                                     End                                      #
################################################################################