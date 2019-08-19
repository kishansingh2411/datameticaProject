#!/bin/bash

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: module.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : Module script to update gold unified hit data table
#                 1. Executes setup function to prepare directory structure for the table											                                						  
#                    and create gold_unified_hit_data_tbl in gold layer.
#               	 Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)                 										  
#                 2. Executes transform function.
#               	 Input Arguments for this script are: Phase name (transform), Suite name (cablevis-uow-com), suite id (7), 
#                                                         source date, frequency (daily),Log file path(/local/file/path)
#   Author(s)   : DataMetica Team
#   Usage       : setup - 
#						sh module.sh "setup" "/log/file/path" 
#				: Transform - 
#						sh module.sh "transform" "${suite_name}" "${suite_id}" "2015-12-28" "/log/file/path"
#
#   Date        : 12/28/2015
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
#    1.0     DataMetica Team          12/28/2015       Initial version
#
#
#####################################################################################################################

###############################################################################
#                          Module Environment Setup                           #
###############################################################################

###                                                                           
# Find absolute path to this script which is used to define module, proje     
# ct, subject area home directory paths.                                      
pushd . > /dev/null
SCRIPT_HOME="${BASH_SOURCE[0]}";
while([ -h "${SCRIPT_HOME}" ]); do
    cd "`dirname "${SCRIPT_HOME}"`"
    SCRIPT_HOME="$(readlink "`basename "${SCRIPT_HOME}"`")";
done
cd "`dirname "${SCRIPT_HOME}"`" > /dev/null
SCRIPT_HOME="`pwd`";
popd  > /dev/null

###                                                                           
# Set module, project, subject area home paths.                               
MODULE_HOME=`dirname ${SCRIPT_HOME}`
SUBJECT_AREA_HOME=`dirname ${MODULE_HOME}`
SUBJECT_AREAS_HOME=`dirname ${SUBJECT_AREA_HOME}`
HOME=`dirname ${SUBJECT_AREAS_HOME}`

###                                                                           
# Load all environment properties files                                       
. ${HOME}/etc/namespace.properties                                                             
. ${HOME}/etc/beeline.properties
. ${HOME}/etc/default.env.properties                                                                    
. ${SUBJECT_AREA_HOME}/etc/subject-area.env.properties
. ${SUBJECT_AREA_HOME}/etc/omniture.properties

###                                                                           
# Load all utility functions. All the script arguments are also passed to      
# functions.sh so that addition processing can be done inside function.sh     
# if required. E.g if the batch id passed from command line then functio      
# ns.sh can store it in a global environment variable so that module can      
# access it.                                                                  
. ${HOME}/bin/functions.sh "$@"

###############################################################################
#                                   Declare                                   #
###############################################################################

# Create initial setup for the tables to process the batch load 
function fn_module_setup(){
  
  log_file_path=$1
  
  # Create module data directory if not exists.
  fn_module_create_hive_table_data_layer_directory_structure \
    "${DATA_LAYER_DIR_WORK_SUBJECT_AREA}" \
    "gold_unified_hit_data_temp" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create temp hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_WORK}" \
     "${DATA_LAYER_DIR_WORK_SUBJECT_AREA}/gold_unified_hit_data_temp" \
     "gold_unified_hit_data_temp" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"
     
  fn_module_create_hive_table_data_layer_directory_structure \
    "${DATA_LAYER_DIR_WORK_SUBJECT_AREA}/temp" \
    "gold_unified_hit_data_backup" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create backup hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_WORK}" \
     "${DATA_LAYER_DIR_WORK_SUBJECT_AREA}/temp/gold_unified_hit_data_backup" \
     "gold_unified_hit_data_backup" \
     "${MODULE_HOME}/schema/module_old.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"
}

function fn_module_transform(){

	suite_id=$1
    suite_name=$2
    update_date=$3
    log_file_path=$4
    username=`echo $USER`
    cutoff_date=$CUTOFF_DATE
 
   hdfs dfs -test -e "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA}/${GOLD_UNIFIED_HIT_DATA_TBL}/suite_name=${suite_name}/source_date=${update_date}"
    exit_code=$?
    
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    then
    	 fn_log_error "Partition ${update_date} does not exist for table ${GOLD_UNIFIED_HIT_DATA_TBL}" "${log_file_path}"
  	     return 0
    fi
    
    pig_script_path="${MODULE_HOME}/pig/backfill_corp_house_for_gold_table.pig"
   
    pig -useHCatalog -p suite_name=$suite_name \
		-p suite_id=$suite_id \
		-p source_date=$update_date \
    	-p version=${version} \
    	-p udf_jar_path="${PIG_UDF_JAR_PATH}" \
		-p hive_database_name_work=${HIVE_DATABASE_NAME_WORK} \
		-p hive_database_name_gold=${HIVE_DATABASE_NAME_GOLD} \
		-p hive_database_name_incoming=${HIVE_DATABASE_NAME_INCOMING} \
		-p hive_database_name_incoming_ods=${HIVE_DATABASE_NAME_INCOMING_ODS} \
		-p incoming_ecr9_shopping_cart_tbl="${HIVE_TABLE_PREFIX}${INCOMING_ECR9_SHOPPING_CART_TBL}" \
		-p gold_hit_data_tbl="${HIVE_TABLE_PREFIX}${GOLD_UNIFIED_HIT_DATA_TBL}" \
		-p gold_unified_hit_data_temp_tbl="gold_unified_hit_data_temp" \
		-f $pig_script_path 2>> ${log_file_path};
	exit_code=$?    
    
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    then
    	 fn_log_error "Pig script failed for loading data into table gold_unified_hit_data_temp with partition ${update_date}" "${log_file_path}"
  	     return $exit_code
    fi
    
    fn_log_info "Successfully loaded gold_unified_hit_data_temp table for date ${update_date}" "${log_file_path}"
    
    fn_hadoop_move_file_or_directory \
	    "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA}/${GOLD_UNIFIED_HIT_DATA_TBL}/suite_name=${suite_name}/source_date=${update_date}/*" \
	    "${DATA_LAYER_DIR_WORK_SUBJECT_AREA}/temp/gold_unified_hit_data_backup/suite_name=${suite_name}/source_date=${update_date}" \
	    "${BOOLEAN_FALSE}" \
	    "${BOOLEAN_FALSE}" \
	    "${log_file_path}"
    exit_code=$?
    
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    then
    	fn_log_error "Falied to move data from ${GOLD_UNIFIED_HIT_DATA_TBL} table to temp location for partition ${update_date}" "${log_file_path}"
  	     return $exit_code
    fi
    
    beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
             "ALTER TABLE ${HIVE_DATABASE_NAME_GOLD}.${GOLD_UNIFIED_HIT_DATA_TBL}  \
             DROP PARTITION(suite_name='${suite_name}',source_date='${update_date}');"
    exit_code=$?
    
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    then
    	fn_log_error "Falied to alter table  ${GOLD_UNIFIED_HIT_DATA_TBL} " "${log_file_path}"
  	     return $exit_code
    fi
    
    fn_log_info "Successfully alter table  ${GOLD_UNIFIED_HIT_DATA_TBL}" "${log_file_path}"
    
    
    beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} --showHeader=false --silent=true --outputformat=tsv2 -e \
			"set hive.exec.dynamic.partition.mode=nonstrict; \
			INSERT INTO TABLE ${HIVE_DATABASE_NAME_GOLD}.${GOLD_UNIFIED_HIT_DATA_TBL} PARTITION (SUITE_NAME,SOURCE_DATE) \
			select * from ${HIVE_DATABASE_NAME_WORK}.gold_unified_hit_data_temp where suite_name='${suite_name}' and source_date='${update_date}';"
	exit_code=$?
    
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    then
    	fn_log_error "Falied to insert data from gold_unified_hit_data_temp table to ${GOLD_UNIFIED_HIT_DATA_TBL} table for partition ${update_date}" "${log_file_path}"
  	     return $exit_code
    fi
    
    fn_log_info "Successfully inserted data from gold_unified_hit_data_temp table to ${GOLD_UNIFIED_HIT_DATA_TBL} table for partition ${update_date}" "${log_file_path}"
     
     
   fn_alter_table_with_two_partitions \
     "${HIVE_DATABASE_NAME_WORK}" \
     "gold_unified_hit_data_backup" \
     "suite_name='${suite_name}'" \
     "source_date='${update_date}'" \
     "${BOOLEAN_FALSE}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}" \
     "${log_file_path}"
   exit_code=$?    
    
   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
   then
   		fn_log_error "Falied to alter gold_unified_hit_data_backup table with partition ${suite_name} and ${update_date}" "${log_file_path}"
  	   return $exit_code
   fi
   
   fn_log_info "Successfully alter gold_unified_hit_data_backup table with partition ${suite_name} and ${update_date}" "${log_file_path}"
}
###############################################################################
#                                  Implement                                  #
###############################################################################

#Calling setup function
fn_main "${@:1}"

###############################################################################
#                                     End                                     #
###############################################################################