#!/bin/bash
######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: ddp_functions.sh  
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

################################################################################
#                                                                              #
# Function to build query for sqoop import                                     #
#                                                                              #
################################################################################

function fn_build_query_for_import(){
	source_table=$1
	database=$2
	seq_id=$3
	table_type=$4
	start_date=$5
	end_date=$6

	sequence_column=$(echo $source_table | cut -d'_' -f2 )

	if [ "${table_type}" = "${MEDIUM_TBL}" ]
	then
	   query="SELECT \
                 /*+ NO_PARALLEL(a) */ \
                 a.*, \
                 SUBSTR(to_char(LOAD_DATE,'yyyy/mm/dd'),0,4) as LOAD_YEAR, \
                 SUBSTR(to_char(LOAD_DATE,'yyyy/mm/dd'),6,2) as LOAD_MONTH \
              FROM  \
              ${database}.${source_table} a \
              WHERE \
                 a.LOAD_DATE >= TO_DATE('${start_date}','yyyy-mm-dd') \
                 AND a.LOAD_DATE < TO_DATE('${end_date}','yyyy-mm-dd') \
                 AND a.${sequence_column}_SEQ > ${seq_id} \
                 AND \$CONDITIONS"
	echo "$query"
	elif [ "${table_type}" = "${LARGE_TBL}" ]
	then
	   query="SELECT \
                 /*+ NO_PARALLEL(a) */ \
                 a.*, \
                 SUBSTR(to_char(LOAD_DATE,'yyyy/mm/dd'),0,4) as LOAD_YEAR, \
                 SUBSTR(to_char(LOAD_DATE,'yyyy/mm/dd'),6,2) as LOAD_MONTH, \
                 SUBSTR(to_char(LOAD_DATE,'yyyy/mm/dd'),9,2) as LOAD_DAY \
              FROM  \
              ${database}.${source_table} a \
              WHERE \
                 a.LOAD_DATE >= TO_DATE('${start_date}','yyyy-mm-dd') \
                 AND a.LOAD_DATE < TO_DATE('${end_date}','yyyy-mm-dd') \
                 AND a.${sequence_column}_SEQ > ${seq_id} \
                 AND \$CONDITIONS"
	echo "$query"
	elif [ -z "${table_type}" ]
        then
           query="SELECT \
                a.* \
              FROM ${database}.${source_table} a \
              WHERE \
                 \$CONDITIONS"
       echo "$query"
	fi
}

################################################################################
#                                                                              #
# Function to get max sequence id for respective table from ddp_audit table    #
#                                                                              #
################################################################################

function fn_get_max_seq_id(){
   source_table=$1
 
   export PGPASSWORD=${POSTGRES_PASSWORD};
   get_max_seq_id=`psql -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USERNAME} \
    -P t -P format=unaligned ${POSTGRES_DATABASE_NAME} -c "SELECT COALESCE(Max_sequence,0) FROM ${DDP_AUDIT} \
     WHERE table_name='$source_table';"`
   exit_code=$?
      
   if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   then
	  return $exit_code
   else
      echo "${get_max_seq_id}"
   fi
}

################################################################################
#                                                                              #
# Function to get max sequence id for respective table from ddp_audit table    #
#                                                                              #
################################################################################

function fn_get_audit_params(){
   source_table=$1
   table_name=$(echo "$source_table" | tr '[:upper:]' '[:lower:]')
   
   export PGPASSWORD=${POSTGRES_PASSWORD};
   get_params=`psql -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USERNAME} \
    -P t -P format=unaligned ${POSTGRES_DATABASE_NAME} -c "SELECT COALESCE(Max_sequence,0) FROM ${DDP_AUDIT} \
     WHERE table_name='$table_name';"`
   exit_code=$?
      
   if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   then
	  return $exit_code
   else
      echo "${get_params}"
   fi

}

################################################################################
#                                                                              #
# Function to update ddp_audit table for respective table data                 #
#                                                                              #
################################################################################

function fn_update_ddp_audit(){

        table_name=$(echo "$1" | tr '[:upper:]' '[:lower:]')
        min_sequence_id=$2
        hive_database=$3
        hive_table=$4
        HIVESERVER2_URL=$5
        HIVE_USENAME=$6
        HIVE_PASSWORD=$7
        log_file_path=$8
        last_modified=`date +"%Y-%m-%d %H:%M:%S"`
        load_start_date=$9
        load_end_date="${10}"

        sequence_column=$(echo $table_name | cut -d'_' -f2 )

        max_sequence_id=`beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 \
          -e "SELECT COALESCE(MAX("$sequence_column"_seq),0) FROM ${hive_database}.${hive_table};" 2>> ${log_file_path}`
        exit_code=$?
        
        if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   		then
	  		return $exit_code
   		else
        
        export PGPASSWORD=$POSTGRES_PASSWORD;
        export PGHOST=$POSTGRES_HOST;
        export PGPORT=$POSTGRES_PORT;
        psql $POSTGRES_DATABASE_NAME $POSTGRES_USERNAME 1>> ${log_file_path} 2>> ${log_file_path} << EOF
                update ${DDP_AUDIT} set last_modified='$last_modified', min_sequence=$min_sequence_id,
                max_sequence=$max_sequence_id,
                load_start_date='${load_start_date}',
                load_end_date='${load_end_date}'
                where (table_name='$table_name');
EOF
exit_code=$?

fi

return $exit_code

}

################################################################################
#                                                                              #
# Function to get key params for ddp 										   #
#                                                                              #
################################################################################

function fn_get_ddp_params(){
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
# Function to update key params for ddp										   #
#                                                                              #
################################################################################

function fn_update_ddp_params(){
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
# Function to get oracle count   											   #
#                                                                              #
################################################################################

function fn_validate_counts(){

   source_table=`echo $1 | tr '[:lower:]' '[:upper:]'`
   hive_count=$2
   seq_id=$3
   database=$4
   load_start_date=$5
   load_end_date=$6

   sequence_column=`echo ${source_table} | cut -d'_' -f2`_SEQ

   query="SELECT COUNT(*) AS REC_COUNT FROM ${database}.${source_table} \
             WHERE LOAD_DATE >= TO_DATE('${load_start_date}','yyyy-mm-dd') \
             AND LOAD_DATE < TO_DATE('${load_end_date}','yyyy-mm-dd') \
             AND ${sequence_column} > ${seq_id}"
   
   oracle_count=`sqoop eval --connect "${DRIVER}${HOST}:${PORT}:${SCHEMA}" \
                    --username ${USERNAME} \
                    --password ${PASSWORD} \
                    --query "${query}"`
   exit_code=$?
   
   if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   then
	   return $exit_code
   else
   	   oracle_count=`echo ${oracle_count} | awk -F "REC_COUNT" '{print$2}' | cut -d' ' -f5`
	   echo "${oracle_count}"
   fi
}

################################################################################
#                                     End                                      #
################################################################################
