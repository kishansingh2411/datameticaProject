#!/bin/bash
######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: sdm_functions.sh  
#   Program type: Unix Bash Shell script
#   Purpose:    : This script consists of all the utility functions that are used by    
#                 sdm module scripts. Modules shall not call and hadoop related commands    
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
#   Date        : 01/24/2017
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
#    1.0     DataMetica Team          01/24/2017       Initial version
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
source $PROJECT_HOME/etc/namespace.properties  
source $PROJECT_HOME/etc/postgres.properties                                                           
source $PROJECT_HOME/etc/default.env.properties

################################################################################
#                                                                              #
# Function to import data directly to hive table                               #
#                                                                              #
################################################################################

function fn_import_table_to_hive_from_oracle(){
   connection_url=$1
   database=$2
   table_name=$3
   query="$4"
   split_by_column=$5
   log_file_path=$6
   mappers=$7
   
   fail_on_error=${BOOLEAN_FALSE}

	sqoop import --connect "${connection_url}" \
      --username "${USERNAME}" \
      --password "${PASSWORD}" \
      --query "${query}" \
      --hcatalog-database ${database} \
      --hcatalog-table ${table_name} \
      --split-by "${split_by_column}" \
      -m "${mappers}" >> ${log_file_path} 2>&1
   exit_code="$?"      
   return ${exit_code}  
}


################################################################################
#                                                                              #
# Function to import data directly to hive table with boundary condition       #
#                                                                              #
################################################################################

function fn_import_table_to_hive_from_oracle_with_boundary_condition(){
   connection_url=$1
   database=$2
   table_name=$3
   query="$4"
   split_by_column=$5
   log_file_path=$6
   mappers=$7
   
   fail_on_error=${BOOLEAN_FALSE}

	sqoop import --connect "${connection_url}" \
      --username "${USERNAME}" \
      --password "${PASSWORD}" \
      --query "${query}" \
      --hcatalog-database ${database} \
      --hcatalog-table ${table_name} \
      --split-by "${split_by_column}" \
      --boundary-query "select min(${split_by_column}),max(${split_by_column}) from ${SRC_WORK_ORDER_MASTER_TBL}" \
      -m "${mappers}" >> ${log_file_path} 2>&1
   exit_code="$?"      
   return ${exit_code}  
}


################################################################################
#                                                                              #
# Function to get key params for SDM 										   #
#                                                                              #
################################################################################

function fn_get_sdm_params(){
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

################################################################################
#                                                                              #
# Function to update sdm params                                                #
#                                                                              #
################################################################################

function fn_update_sdm_params(){

key_param_id=$1

if [ ! -z "${key_param_id}" ]
then
export PGPASSWORD=${POSTGRES_PASSWORD};
export PGHOST=${POSTGRES_HOST};
export PGPORT=${POSTGRES_PORT};
psql ${POSTGRES_DATABASE_NAME} ${POSTGRES_USERNAME} << EOF
   UPDATE KEY_PARAMS SET PARAM_DTM_START = PARAM_DTM_START + (INCREMENTAL_IN_HRS * INTERVAL '1 HOUR'), \
   PARAM_DTM_END = PARAM_DTM_START + (INCREMENTAL_IN_HRS * INTERVAL '1 HOUR'), \
   DTM_LAST_UPDATED = DATE_TRUNC('SECOND',NOW()::TIMESTAMP) \
   WHERE KEY_PARAM_ID = $key_param_id;
EOF
exit_code=$?

fi

	return $exit_code
}


################################################################################
#                                                                              #
# Function to get max value from edp_audit table                               #
#                                                                              #
################################################################################

function fn_get_max_value(){
   source_table=$1
   data_source=$2
   
   source_table=$(echo "$source_table" | tr '[:upper:]' '[:lower:]')
 
export PGPASSWORD=${POSTGRES_PASSWORD};
   get_max_val=`psql -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USERNAME} \
    -P t -P format=unaligned ${POSTGRES_DATABASE_NAME} -c "SELECT load_end_date FROM ${EDP_AUDIT} \
     WHERE table_name='$source_table' and data_source='${data_source}';"`
   exit_code=$?
      
   if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   then
	  return $exit_code
   else
      echo "${get_max_val}"
   fi
}

################################################################################
#                                                                              #
# Function to update edp_audit table for respective table data                 #
#                                                                              #
################################################################################

function fn_update_edp_audit_value(){

        table_name=$(echo "$1" | tr '[:upper:]' '[:lower:]')
        data_source=$2
        min_seq=0
        max_seq=0
        load_start_date=$3
        load_end_date=$4
        log_file_path=$5
        last_modified=`date +"%Y-%m-%d %H:%M:%S"`
        
        export PGPASSWORD=$POSTGRES_PASSWORD;
        export PGHOST=$POSTGRES_HOST;
        export PGPORT=$POSTGRES_PORT;
        psql $POSTGRES_DATABASE_NAME $POSTGRES_USERNAME 1>> ${log_file_path} 2>> ${log_file_path} << EOF
                update ${EDP_AUDIT} set last_modified='$last_modified', min_sequence=$min_seq, max_sequence=$max_seq,
                load_start_date='$load_start_date', load_end_date='$load_end_date'
                where (table_name='$table_name' and data_source='$data_source');
EOF
exit_code=$?

return $exit_code

}

################################################################################
#                                                                              #
# Function to get last value for incremental load                              #
#                                                                              #
################################################################################

function fn_get_last_value(){

data_source=$1
table_name=$2

if [ ! -z "${key_param_id}" ]
then
export PGPASSWORD=${POSTGRES_PASSWORD};
export PGHOST=${POSTGRES_HOST};
export PGPORT=${POSTGRES_PORT};
psql ${POSTGRES_DATABASE_NAME} ${POSTGRES_USERNAME} << EOF
   SELECT * FROM ${EDP_AUDIT} WHERE data_source = $data_source and table_name=$table_name;
EOF
exit_code=$?

fi

	return $exit_code
}

################################################################################
#                                                                              #
# Function to execute hive queries                                             #
#                                                                              #
################################################################################

function fn_execute_hive(){

  source_date=$1
  incoming_table=$2
  gold_table=$3
  hql_file=$4
  log_file_path=$5
  
  source_date=`echo $source_date | tr -d -`
 beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} \
   -hivevar source_date=${source_date} \
   -hivevar incoming_database="${HIVE_DATABASE_NAME_INCOMING}" \
   -hivevar gold_database="${HIVE_DATABASE_NAME_GOLD}" \
   -hivevar table_prefix="${HIVE_TABLE_PREFIX}" \
   -hivevar incoming_table="${incoming_table}" \
   -hivevar gold_table="${gold_table}" \
   -f "${hql_file}" 1>> "${log_file_path}" 2>> "${log_file_path}" 
   exit_code=$?
   
   return $exit_code
}
