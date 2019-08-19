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
#                 ods module scripts. Modules shall not call and hadoop related commands    
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
   
function fn_execute_sqoop_export_to_oracle(){
   
   connection_url=$1
   target_table=$2
   dir_path=$3
   log_file_path=$4
   fail_on_error=${BOOLEAN_FALSE}
 
   sqoop export --connect "${connection_url}" \
      --username "${USERNAME_CHNN}" \
      --password "${PASSWORD_CHNN}" \
      --table ${target_table} \
      --fields-terminated-by '~' \
      --export-dir "${dir_path}" \
      --input-null-string "\\\\N" \
      --input-null-non-string "\\\\N" \
      -m "${NUMBER_OF_MAPPERS}" >> ${log_file_path} 2>&1
   exit_code="$?" 
   return ${exit_code}    
}

################################################################################
#                                                                              #
# Function to get max sequence id for respective table from edp_audit table    #
#                                                                              #
################################################################################

function fn_get_max_cust_services_delta_seq(){
   source_table=$1
 
export PGPASSWORD=${POSTGRES_PASSWORD};
   get_max_cust_services_delta_seq=`psql -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USERNAME} \
    -P t -P format=unaligned ${POSTGRES_DATABASE_NAME} -c "SELECT COALESCE(max_sequence,0) FROM ${EDP_AUDIT} \
     WHERE table_name='$source_table';"`
   exit_code=$?
      
   if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   then
	  return $exit_code
   else
      echo "${get_max_cust_services_delta_seq}"
   fi
}

################################################################################
#                                                                              #
# Function to get last updated date for respective table from edp_audit table  #
#                                                                              #
################################################################################

function fn_get_last_update_date(){
   source_table=$1
 
   export PGPASSWORD=${POSTGRES_PASSWORD};
   get_last_updated_date=`psql -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USERNAME} \
    -P t -P format=unaligned ${POSTGRES_DATABASE_NAME} -c "SELECT load_start_date FROM ${EDP_AUDIT} \
     WHERE table_name='$source_table';"`
   exit_code=$?
      
   if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   then
	  return $exit_code
   else
      echo "${get_last_updated_date}"
   fi
}

################################################################################
#                                                                              #
# Function to update edp_audit table for respective table data                 #
#                                                                              #
################################################################################

function fn_update_edp_audit_last_modified_date(){

        table_name=$(echo "$1" | tr '[:upper:]' '[:lower:]')
        min_seq=0
        max_seq=0
        current_date=$2
        log_file_path=$3
        last_modified=`date +"%Y-%m-%d %H:%M:%S"`

        
        export PGPASSWORD=$POSTGRES_PASSWORD;
        export PGHOST=$POSTGRES_HOST;
        export PGPORT=$POSTGRES_PORT;
        psql $POSTGRES_DATABASE_NAME $POSTGRES_USERNAME 1>> ${log_file_path} 2>> ${log_file_path} << EOF
                update ${EDP_AUDIT} set last_modified='$last_modified', min_sequence=$min_seq, max_sequence=$max_seq,
                load_start_date='$current_date'
                where (table_name='$table_name');
EOF
exit_code=$?

return $exit_code

}



################################################################################
#                                                                              #
# Function to get min sequence id for respective table from edp_audit table    #
#                                                                              #
################################################################################

function fn_get_min_cust_services_delta_seq(){
   source_table=$1
 
   export PGPASSWORD=${POSTGRES_PASSWORD};
   get_min_cust_services_delta_seq=`psql -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USERNAME} \
    -P t -P format=unaligned ${POSTGRES_DATABASE_NAME} -c "SELECT COALESCE(Min_sequence,0) FROM ${EDP_AUDIT} \
     WHERE table_name='$source_table';"`
   exit_code=$?
      
   if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   then
	  return $exit_code
   else
      echo "${get_min_cust_services_delta_seq}"
   fi
}


################################################################################
#                                                                              #
# Function to build query for sqoop import                                     #
#                                                                              #
################################################################################

function fn_build_query_for_import(){
	source_table=$1
	database=$2
	max_seq=$3

	   query="SELECT \
            ID_SERVICE_REC, \
            ID_CUST, \
			CH_ACCOUNT_NUMBER, \
			ID_SERVICE_TYPE, \
			ID_SYS_INFO, \
			ID_STATUS, \
			CH_HOME_PH, \
			CH_WORK_PH, \
			CH_EMAIL_ADD, \
			CH_USERNAME, \
			CH_PASSWD, \
			DT_START_DATE, \
			DT_END_DATE, \
			DT_LAS_MOD_DATE, \
			CH_HOMEAREA_CD, \
			CH_RATE,CH_BDATE, \
			CH_BILLTHRUDATE, \
			CH_CYCLE, \
			CH_DRSN, \
			CH_HOLD, \
			CH_COLLECT_STATUS, \
			CH_ACCT_STATUS_QUALIFIER, \
			DT_ACCT_STATUS_QUALIFIER_DATE, \
			DT_STATUS_DATE, \
			OP_TYPE, \
			LOAD_DATE, \
			CUST_SERVICES_DELTA_SEQ \
			FROM ${database}.${source_table} a \
			WHERE CUST_SERVICES_DELTA_SEQ > '${max_seq}' \
			AND \$CONDITIONS "
            echo "$query" 
}

################################################################################
#                                                                              #
# Function to get oracle count   											   #
#                                                                              #
################################################################################

function fn_validate_counts(){

   source_table=$1
   database=$2
   cust_services_delta_seq=$3
	
	   query="SELECT \
              COUNT(*) AS REC_COUNT \
              FROM  \
              ${database}.${source_table} \
              WHERE CUST_SERVICES_DELTA_SEQ > ${cust_services_delta_seq}"
                
   oracle_count=`sqoop eval --connect "${DRIVER}${HOST_CHNN}:${PORT_CHNN}:${SCHEMA_CHNN}" \
                    --username ${USERNAME_CHNN} \
                    --password ${PASSWORD_CHNN} \
                    --query "${query}"`
   exit_code=$?
   
   if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   then
	   return $exit_code
   fi
   
   oracle_count=`echo ${oracle_count} | awk -F "REC_COUNT" '{print$2}' | cut -d' ' -f5`
   echo "${oracle_count}"
   
}


################################################################################
#                                                                              #
# Function to load incoming table data from work using pig script			   #
#                                                                              #
################################################################################

function fn_ods_execute_pig(){  
  hive_database_name_work=$1
  work_data_tbl=$2
  hive_database_name_incoming=$3
  incoming_data_tbl=$4
  source_date=$5
  dataset=$6
  udf_jar_path=$7
  cvs_jar_path=$8
  version=$9
  username=${10}
  pig_script_path=${11}
  namenode_service=${12}
  log_file=${13}
  
  pig -useHCatalog \
       -p udf_jar_path="${udf_jar_path}" \
       -p cvs_jar_path="${cvs_jar_path}" \
       -p version="${version}" \
       -p username="${username}" \
       -p dataset="${dataset}" \
       -p namenode_service="${namenode_service}" \
       -p hive_database_name_incoming=${hive_database_name_incoming} \
       -p hive_database_name_work=${hive_database_name_work} \
       -p incoming_data_tbl="${incoming_data_tbl}" \
       -p work_data_tbl="${work_data_tbl}" \
       -p source_date="${source_date}" \
       -f $pig_script_path 2>> ${log_file_path};
    exit_code=$?    

  success_message="Successfully executed ${module_name} pig script "
  failure_message="Pig script ${module_name} failed"
  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_FALSE}" "${log_file}"
  return $exit_code
}

################################################################################
#                                                                              #
# Function to get ods params                                                   #
#                                                                              #
################################################################################

function fn_get_ods_params(){

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
# Function to update ods params                                           #
#                                                                              #
################################################################################

function fn_update_ods_params(){

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


function fn_import_from_oracle_asitis(){

   connection_url="$1"
   src_table_name="$2"
   target_table_name="$3"
   src_database_name="$4"
   hive_database_name="$5"
   user_name="$6"
   password="$7"
   mappers=$8
   split_by_column=$9
   partition_key="${10}"
   partition_value="${11}"
   log_file_path="${12}"

   sqoop import \
      --connect "${connection_url}" \
      --username "${user_name}" \
      --password "${password}" \
      --table "${src_database_name}"."${src_table_name}" \
      --hcatalog-database "${hive_database_name}" \
      --hcatalog-table "${target_table_name}" \
      --hive-partition-key "${partition_key}" \
      --hive-partition-value "${partition_value}" \
      --split-by "${split_by_column}" \
      -m ${mappers} >> ${log_file_path} 2>&1
   exit_code=$?    
   
  return $exit_code
}

function fn_get_count(){
	
	HIVESERVER2_URL=$1
    HIVE_USENAME=$2
    HIVE_PASSWORD=$3
    hive_database=$4
	hive_table=$5
	action=$6
	source_date=$7
    
    if [[ $action == "I" ]] 
    then
	    record_count=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 \
	          -e "SELECT count(*) FROM ${hive_database}.${hive_table} where CAST(dtm_created as DATE) = CURRENT_DATE ;" 2>> ${log_file_path}`
	    exit_code=$?
    elif [[ $action == "U" ]] 
	then
	    record_count=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 \
	          -e "SELECT count(*) FROM ${hive_database}.${hive_table} where CAST(dtm_created as DATE) != CURRENT_DATE and \
	           CAST(dtm_last_modified as DATE)= CURRENT_DATE ;" 2>> ${log_file_path}`
	    exit_code=$?
	    
	 elif [[ $action == "T" ]] 
	then
	    record_count=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 \
	          -e "SELECT count(*) FROM ${hive_database}.${hive_table} where  CAST(dtm_last_modified as DATE)= CURRENT_DATE ;" 2>> ${log_file_path}`
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

################################################################################
#                                                                              #
# Function to update key params for ods										   #
#                                                                              #
################################################################################

function fn_update_ods_params(){
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
#                                     End                                      #
################################################################################
