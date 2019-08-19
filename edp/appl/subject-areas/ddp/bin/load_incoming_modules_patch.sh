#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: executor.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : Execute all steps one by one for DDP Ingestion Process
#   Author(s)   : DataMetica Team
#   Usage       : sh load_incoming.sh 
#   Date        : 04/08/2016
#   Log File    : .../log/ddp/EXECUTOR.log
#   SQL File    : 
#   Error File  : .../log/ddp/EXECUTOR.log
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
HOME=`dirname ${SUBJECT_AREAS_HOME}`

# Load all environment properties files
source $HOME/etc/namespace.properties
source $HOME/etc/beeline.properties
source $HOME/etc/default.env.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $SUBJECT_AREA_HOME/etc/ddp_patch.properties
source $SUBJECT_AREA_HOME/etc/oracle.properties

source $HOME/bin/functions.sh

##############################################################################
#                                                                             #
# Checking number of parameter passed                                         #
#                                                                             #
##############################################################################

if [ "$#" -ne 2 ] 
then
   echo "Illegal number of parameters. Parameters expected are [ "table_name" "table_type" ]"
   exit -1
fi

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

table_name=$1
table_type=$2
job_name="DDP_600004_LD_`echo ${table_name} | tr '[:lower:]' '[:upper:]'`"
log_file_path="${LOG_DIR}/${SUBJECT_AREA_NAME}/${job_name}_PATCH.log"
source_table="edp_"${table_name}
hive_ddl_file=$SUBJECT_AREA_HOME/hive/${table_name}.hql
connection_url="${DRIVER}${HOST}:${PORT}:${SCHEMA}"

###############################################################################
#                          Hive Create Table                                  #
###############################################################################

beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} -f "${hive_ddl_file}" $@ 1>> "${log_file_path}" 2>> "${log_file_path}" 
exit_code=$?
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Error occured while creating table ${table_name}" "${log_file_path}"
   exit ${exit_code}
fi
fn_log_info "Successfully created table ${table_name}" "${log_file_path}"

###############################################################################
#                          Build Table Query                                  #
###############################################################################

eval date_range=\${"${table_name}"_date}
fn_log_info "For table ${table_name} got date range as ${date_range}" "${log_file_path}"

if [ "${table_type}" = "medium" ]
	then
	   query="SELECT \
	          /*+ NO_PARALLEL(a) */ \
			  a.*, \
			  SUBSTR(to_char(LOAD_DATE,'yyyy/mm/dd'),0,4) as LOAD_YEAR, \
              SUBSTR(to_char(LOAD_DATE,'yyyy/mm/dd'),6,2) as LOAD_MONTH \
              FROM  ${ORACLE_DATABASE}.${source_table} a \
              WHERE load_date in (${date_range}) AND \$CONDITIONS"
		hive_file_path=$SUBJECT_AREA_HOME/hive/insert_into_medium_table.hql	  
	elif [ "${table_type}" = "large" ]
	then
	   query="SELECT \
	          /*+ NO_PARALLEL(a) */ \
			  a.*, \
			  SUBSTR(to_char(LOAD_DATE,'yyyy/mm/dd'),0,4) as LOAD_YEAR, \
              SUBSTR(to_char(LOAD_DATE,'yyyy/mm/dd'),6,2) as LOAD_MONTH, \
              SUBSTR(to_char(LOAD_DATE,'yyyy/mm/dd'),9,2) as LOAD_DAY \
              FROM ${ORACLE_DATABASE}.${source_table} a \
              WHERE load_date in (${date_range}) AND \$CONDITIONS"
		hive_file_path=$SUBJECT_AREA_HOME/hive/insert_into_large_table.hql		 
fi

###############################################################################
#                          Import data in Hive Tmp Table                     #
###############################################################################
	
seq_column=`echo "${source_table}" | cut -d'_' -f2 | tr '[:lower:]' '[:upper:]'`_SEQ
incoming_table_name="incoming_"${table_name}

     sqoop import --connect "${connection_url}" \
      --username "${USERNAME}" \
      --password "${PASSWORD}" \
      --hcatalog-database "default" \
      --hcatalog-table "${table_name}_tmp" \
      --query "${query}" \
      --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec \
      --split-by "${seq_column}" \
      -m 1 >> ${log_file_path} 2>&1
   exit_code="$?"      

if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed to import data for table ${target_table}" "${log_file_path}"
   exit ${exit_code}
fi 
fn_log_info "Successfully imported data for table ${target_table} " "${log_file_path}"

###############################################################################
#                          Import data in Incoming Hive Table                 #
###############################################################################

beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} \
	  -hivevar source_database="default" \
	  -hivevar source_table="${table_name}_tmp" \
	  -hivevar target_database="${database}" \
	  -hivevar target_table="${incoming_table_name}" \
      -hivevar sequence_column="${table_name}_seq" \
      -f "${hive_file_path}" 1>> "${log_file_path}" 2>> "${log_file_path}"
exit_code=$?
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed to importing data into table ${incoming_table_name}" "${log_file_path}"
   exit ${exit_code}
fi 
fn_log_info "Successfully imported data into table ${incoming_table_name} " "${log_file_path}"
   
###############################################################################
#                          Drop Hive Table                                    #
###############################################################################

beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} -e "Drop table ${table_name}_tmp;" >> "${log_file_path}" 2>> "${log_file_path}" 
exit_code=$?
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Error occured while droping table ${table_name}_tmp ${table_name}" "${log_file_path}"
   exit ${exit_code}
fi
fn_log_info "Successfully drop table ${table_name}_tmp" "${log_file_path}"

###############################################################################
#                          End                                                #
###############################################################################
