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
#                 mrdvr module scripts. Modules shall not call and hadoop related commands    
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

function fn_import_table_to_hive_from_mysql(){
   connection_url=$1
   database=$2
   table_name=$3
   query="$4"
   column_name=$5
   log_file_path=$6
   mappers=$7
   
   sqoop import -D mapreduce.output.fileoutputformat.compress=true \
      -D mapreduce.output.fileoutputformat.compress.type=BLOCK \
      --connect "${connection_url}" \
      --username "${MYSQL_USERNAME}" \
      --password "${MYSQL_PASSWORD}" \
      --driver "${MYSQL_DRIVER}" \
      --query "${query}" \
      --hcatalog-database ${database} \
      --hcatalog-table ${table_name} \
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
	column_list="$2"
	source_date=$3
	
    table_name=$(echo "$source_table" | tr '[:lower:]' '[:upper:]')
         
		   query="SELECT \
	                 "${column_list}" \
	              FROM  \
	              ${MYSQL_DATABASE}.${table_name} \
	              WHERE \
	                 \$CONDITIONS"
       
    echo "$query"
}

################################################################################
#                                                                              #
# Function to get key params for mrdvr 										   #
#                                                                              #
################################################################################

function fn_get_mrdvr_params(){
   key_param_id=$1

   if [ ! -z "${key_param_id}" ]
   then
      export PGPASSWORD=${POSTGRES_PASSWORD};
      get_params=`psql -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USERNAME} \
                  -P t -P format=unaligned ${POSTGRES_DATABASE_NAME} \
                  -c "SELECT PARAM_DTM_START::TIMESTAMP::DATE|| '~' ||(PARAM_DTM_START + (INCREMENTAL_IN_HRS * INTERVAL '1 HOUR'))::DATE FROM KEY_PARAMS \
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
# Function to update key params for mrdvr									   #
#                                                                              #
################################################################################

function fn_update_mrdvr_params(){
	key_param_id=$1
	
if [ ! -z "${key_param_id}" ]
then
   export PGPASSWORD=${POSTGRES_PASSWORD};
   export PGHOST=${POSTGRES_HOST};
   export PGPORT=${POSTGRES_PORT};
		psql ${POSTGRES_DATABASE_NAME} ${POSTGRES_USERNAME} << EOF
		   UPDATE KEY_PARAMS SET PARAM_DTM_START = PARAM_DTM_START + (INCREMENTAL_IN_HRS * INTERVAL '1 HOUR'), \
		   PARAM_DTM_END = PARAM_DTM_START + (2 * INCREMENTAL_IN_HRS * INTERVAL '1 HOUR'), \
		   DTM_LAST_UPDATED = DATE_TRUNC('SECOND',NOW()::TIMESTAMP) \
		   WHERE KEY_PARAM_ID = $key_param_id;
EOF
exit_code=$?

fi

return $exit_code
}

################################################################################
#                                                                              #
# Function to get mysql count   											   #
#                                                                              #
################################################################################

function fn_get_mysql_count(){

   source_table=`echo $1 | tr '[:lower:]' '[:upper:]'`
   
   query="SELECT COUNT(*) AS REC_COUNT FROM ${MYSQL_DATABASE}.${source_table}"
                                      
   mysql_count=`sqoop eval --connect "${MYSQL_CONN}://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE}" \
                    --username ${MYSQL_USERNAME} \
                    --password ${MYSQL_PASSWORD} \
                    --driver "${MYSQL_DRIVER}" \
                    --query "${query}"`
   exit_code=$?
   
   if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   then
       return $exit_code
   fi
                    
   mysql_count=`echo ${mysql_count} | awk -F "REC_COUNT" '{print$2}' | cut -d' ' -f5`

   echo "${mysql_count}"
}

################################################################################
#                                                                              #
# Function to load incoming table data from work using pig script			   #
#                                                                              #
################################################################################

function fn_mrdvr_execute_pig(){  
  hive_database_name_work=$1
  work_data_tbl=$2
  hive_database_name_incoming=$3
  incoming_data_tbl=$4
  udf_jar_path=$5
  cvs_jar_path=$6 
  source_date=$7
  version=$8
  pig_script_path=$9
  dataset=${10}
  username=${11}
  namenode_service=${12}
  log_file=${13}
  
  pig -useHCatalog \
       -p udf_jar_path="${udf_jar_path}" \
       -p cvs_jar_path="${cvs_jar_path}" \
       -p version="${version}" \
       -p dataset="${dataset}" \
       -p username="${username}" \
       -p namenode_service="${namenode_service}" \
       -p hive_database_name_incoming=${hive_database_name_incoming} \
       -p hive_database_name_work=${hive_database_name_work} \
       -p incoming_data_tbl="${incoming_data_tbl}" \
       -p work_data_tbl="${work_data_tbl}" \
       -p load_date="${source_date}" \
       -f $pig_script_path 2>> ${log_file_path};
    exit_code=$?    

  success_message="Successfully executed ${module_name} pig script "
  failure_message="Pig script ${module_name} failed"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_FALSE}" "${log_file}"
  return $exit_code
}

################################################################################
#                                                                              #
# Function to import data directly to hive table                               #
#                                                                              #
################################################################################

function fn_import_table_to_hive_from_netezza(){
   connection_url=$1
   query="$2"
   database=$3
   table_name=$4
   column_name=$5
   mappers=$6
   log_file_path=$7
   

	sqoop import \
      --connect "${connection_url}" \
      --username "${NETEZZA_USERNAME}" \
      --password "${NETEZZA_PASSWORD}" \
      --driver "${NETEZZA_DRIVER}" \
      --query "${query}" \
      --hcatalog-database ${database} \
      --hcatalog-table ${table_name} \
      --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec \
      --split-by "${column_name}" \
      -m "${mappers}" >> ${log_file_path} 2>&1
   exit_code="$?"      
   return ${exit_code}  
}


################################################################################
#                                                                              #
# Function to build query for sqoop import from Netezza                        #
#                                                                              #
################################################################################

function fn_build_query_for_import_from_netezza(){
	
	source_table=$1
	column_list=$2
    table_name=$(echo "$source_table" | tr '[:upper:]' '[:lower:]')
    
    if [[ $source_table == "t_rsdvr_preferences_delta" ]] || 
       [[ $source_table == "t_rsdvr_recording_archive_delta" ]] ||
       [[ $source_table == "t_rsdvr_requests_archive_delta" ]] ||
       [[ $source_table == "t_rsdvr_recordings_delta" ]] ||
       [[ $source_table == "t_rsdvr_requests_delta" ]] ||
       [[ $source_table == "t_rsdvr_schedule_delta" ]]
    then
	   query="SELECT \
	          "${column_list}", \
	           TO_CHAR(LOAD_DATE,'YYYY-MM-DD') AS LOAD_DATE \
              FROM ${NETEZZA_DATABASE}.${table_name} \
              WHERE \$CONDITIONS"
	else 
      query="SELECT a.* \
              FROM ${NETEZZA_DATABASE}.${table_name} a \
              WHERE \$CONDITIONS"
	fi           
    echo "$query"
}


################################################################################
#                                                                              #
# Function to get hive for the specified table                                 #
#                                                                              #
################################################################################

function fn_get_hive_count(){
	
	HIVESERVER2_URL=$1
    HIVE_USENAME=$2
    HIVE_PASSWORD=$3
    hive_database=$4
	hive_table=$5
	operation=$6
	source_date=$7


   if [[ ${hive_table} == *'delta'* ]]
   then
      if [[ ${operation} == 'transform' ]]
      then
         hive_counts=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true \
	        --outputformat=tsv2 -e "SELECT COUNT(*) FROM ${hive_database}.${hive_table} \
	           WHERE CAST(DTM_CREATED AS DATE) = CURRENT_DATE \
	           AND OP_TYPE IN ('I' , 'U');"`
         exit_code=$?
      elif [[ ${operation} == 'delete' ]]
      then
         hive_counts=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true \
	        --outputformat=tsv2 -e "SELECT COUNT(*) FROM ${hive_database}.${hive_table} \
	           WHERE CAST(DTM_CREATED AS DATE) = CURRENT_DATE \
	           AND OP_TYPE IN ('D');"`
         exit_code=$?
      fi
   elif [[ ${hive_table} == *'last_rec'* ]]
   then
      if [[ ${operation} == 'transform' ]]
      then
         hive_counts=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true \
	        --outputformat=tsv2 -e "SELECT COUNT(*) FROM ${hive_database}.${hive_table} \
	           WHERE CAST(DTM_LAST_MODIFIED AS DATE) = CURRENT_DATE \
	           AND OP_TYPE IN ('I' , 'U');"`
         exit_code=$?
      elif [[ ${operation} == 'delete' ]]
      then
         hive_counts=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true \
	        --outputformat=tsv2 -e "SELECT COUNT(*) FROM ${hive_database}.${hive_table} \
	           WHERE CAST(DTM_LAST_MODIFIED AS DATE) = CURRENT_DATE \
	           AND OP_TYPE IN ('D');"`
         exit_code=$?
      fi
   elif [[ ${hive_table} == *'incoming'* ]]
   then
      hive_counts=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true \
	        --outputformat=tsv2 -e "SELECT COUNT(*) FROM ${hive_database}.${hive_table} \
	           WHERE LOAD_DATE = '${source_date}';"`
         exit_code=$?
   else
      hive_counts=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true \
	  --outputformat=tsv2 -e "SELECT COUNT(*) FROM ${hive_database}.${hive_table};"`
      exit_code=$?
   fi

   if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   then
      return $exit_code
   fi

   echo "${hive_counts}"
}

################################################################################
#                                                                              #
# Function to get count of records from tables for given criteria              #
#                                                                              #
################################################################################

function fn_get_count(){
	
	HIVESERVER2_URL=$1
    HIVE_USENAME=$2
    HIVE_PASSWORD=$3
    hive_database=$4
	hive_table=$5
	action=$6
	source_date=$7
    
    if [[ $action == "I" ]] || [[ $action == "U" ]] || [[ $action == "D" ]]
    then
	    record_count=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 \
	          -e "SELECT count(*) FROM ${hive_database}.${hive_table} where op_type='${action}' and CAST(dtm_last_modified as DATE)= CURRENT_DATE ;" 2>> ${log_file_path}`
	    exit_code=$?
    elif [[ $action == "DD" ]] 
	then
	    record_count=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 \
	          -e "SELECT count(*) FROM ${hive_database}.${hive_table}" 2>> ${log_file_path}`
	    exit_code=$?
	elif [[ $action == "TOTAL" ]] 
	then
	    record_count=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 \
	          -e "SELECT count(*) FROM ${hive_database}.${hive_table} ;" 2>> ${log_file_path}`
	    exit_code=$?
	else
    	echo "Wrong action provided to compute record count"
    	exit -1
	fi
    	
  	if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
  	then
  	 	return $exit_code
  	fi
  	
  	echo "${record_count}"
}

function fn_update_mrdvr_params(){
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
#                                     End                                      #
################################################################################