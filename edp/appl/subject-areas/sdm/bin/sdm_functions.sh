#!/bin/bash
######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: ovcdr_functions.sh  
#   Program type: Unix Bash Shell script
#   Purpose:    : This script consists of all the utility functions that are used by    
#                 ovcdr module scripts. Modules shall not call and hadoop related commands    
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
#   Date        : 05/02/2016
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
#    1.0     DataMetica Team          05/02/2016       Initial version
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
   seq_column=`echo "${table_name}" | cut -d'_' -f2 | tr '[:lower:]' '[:upper:]'`_SEQ
   query="$4"
   log_file_path=$5
   mappers=$6
   
   fail_on_error=${BOOLEAN_FALSE}

	sqoop import --connect "${connection_url}" \
      --username "${USERNAME}" \
      --password "${PASSWORD}" \
      --query "${query}" \
      --hcatalog-database ${database} \
      --hcatalog-table ${table_name} \
      --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec \
      --split-by "${seq_column}" \
      -m "${mappers}" >> ${log_file_path} 2>&1
   exit_code="$?"      
   return ${exit_code}  
}

###############################################################################
#                                                                              #
# Function to build query for sqoop import                                     #
#                                                                              #
################################################################################

function fn_build_query_for_import(){
	
	source_table=$1
	column_list="$2"
	
	
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
# Function to get key params for OVCDR 										   #
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
# Function to create job name from suite name                                  #
#                                                                              #
################################################################################

function fn_generate_job_name(){
key_param_id=$1
dataset_name="$2"
job_name=$3

   if [[ $dataset_name == dom_nsn_in ]] ||
      [[ $dataset_name == dom_nsn_out ]] ||
      [[ $dataset_name == int_nsn ]] ||
      [[ $dataset_name == int_rad ]] ||
      [[ $dataset_name == vma_nsn ]]
    then
  dataset_name=$(echo "$dataset_name" | tr '[:lower:]' '[:upper:]')
  echo "EDP_"$key_param_id"_"$dataset_name"_"$job_name
  else
         echo "DataSet name '$dataset_name' is incorrect."
         echo "DataSet name must be one of [dom_nsn_in or dom_nsn_out or int_nsn or int_rad or vma_nsn]"
    exit
   fi
}

################################################################################
#                                                                              #
# Function to update ovcdr params                                              #
#                                                                              #
################################################################################

function fn_update_ovcdr_params(){

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
# Function to check method to be use                                           #
#                                                                              #
################################################################################


function fn_check_operation_name(){
operation=$1

   if [[ $operation == insert ]] || 
      [[ $operation == update ]] ||
      [[ $operation == delete ]]
          
    then
          echo "Correct method name '$operation' provided"
    else
         echo "Period name '$operation' is incorrect."
         echo " Period name must be one of [insert or update or delete ]"
    exit
   fi
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
# Function to get usage_date   from tables for given criteria                   #
#                                                                              #
################################################################################

function fn_get_usage_date(){
	
	HIVESERVER2_URL=$1
    HIVE_USENAME=$2
    HIVE_PASSWORD=$3
    hive_database=$4
	hive_table=$5
	
		    usage_date=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 \
	          -e "SELECT DISTINCT usage_date FROM ${hive_database}.${hive_table} ;" 2>> ${log_file_path}`
	    exit_code=$?
	      	
  	if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
  	then
  	 	return $exit_code
  	fi
  	
  	echo "${usage_date}"
}

function fn_execute_pig_chc(){

   incoming_agg_data=$1
   source_date=$2
   gold_agg_data=$3
   work_agg_chc=$4
   log_file_path=$5
   pig_script_path=$6
   param_numeric=$7
   dtm_start=$8

   pig -useHCatalog \
      -p hive_database_name_incoming=${HIVE_DATABASE_NAME_INCOMING} \
      -p incoming_agg_data=${incoming_agg_data} \
      -p source_date=${source_date} \
      -p hive_database_name_gold=${HIVE_DATABASE_NAME_GOLD} \
      -p gold_agg_data=${gold_agg_data} \
      -p hive_database_name_ods_incoming=${HIVE_DATABASE_NAME_INCOMING_ODS} \
      -p ov_telephone_number=${INCOMING_OV_TELEPHONE_NUMBER_TBL} \
      -p customer_account=${INCOMING_CUSTOMER_ACCOUNT_TBL} \
      -p ip_sip_did_phone_nbr=${INCOMING_IP_SIP_DID_PHONE_NBR_TBL} \
      -p ip_sip_pilot_phone_nbr=${INCOMING_IP_SIP_PILOT_PHONE_NBR_TBL} \
      -p hive_database_name_work=${HIVE_DATABASE_NAME_WORK} \
      -p work_agg_chc=${work_agg_chc} \
      -p param_numeric=${param_numeric} \
      -p dtm_start=${dtm_start} \
      -f $pig_script_path 2>> ${log_file_path};
   exit_code=$?
  
   return $exit_code
}


function fn_execute_hive(){

  source_date=$1
  incoming_table=$2
  work_table=$3
  gold_table=$4
  chc_table=$5
  metadata_table=$6
  file_name=$7
  hql_file=$8
  log_file_path=$9
  param_numeric="${10}"
  
 beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} \
   -hivevar source_date=${source_date} \
   -hivevar work_database="${HIVE_DATABASE_NAME_WORK}" \
   -hivevar incoming_database="${HIVE_DATABASE_NAME_INCOMING}" \
   -hivevar gold_database="${HIVE_DATABASE_NAME_GOLD}" \
   -hivevar table_prefix="${HIVE_TABLE_PREFIX}" \
   -hivevar work_table="${work_table}" \
   -hivevar incoming_table="${incoming_table}" \
   -hivevar gold_table="${gold_table}" \
   -hivevar work_chc_table="${chc_table}" \
   -hivevar gold_metadata_table="${metadata_table}" \
   -hivevar file_name="${file_name}" \
   -hivevar param_numeric="${param_numeric}" \
   -f "${hql_file}" 1>> "${log_file_path}" 2>> "${log_file_path}" 
   exit_code=$?
   
   return $exit_code
}