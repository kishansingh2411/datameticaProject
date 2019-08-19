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
#   Purpose:    : Module script perform following activities
#                 1. Executes setup function to prepare directory structure for the table											                                						  
#                    and create smith_onet_prod_esp_optimum_usage_tbl in smith layer.
#               		Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)                 										  
#                 2. Executes transform function.
#               		Input Arguments for this script are: Phase name (transform), Suite Id (7),
#                                                            source date (2015-12-28), Log file path(/local/file/path)
#                 3. Executes Prepare function.
#						Input Arguments for this script are: Phase name (prepare), source date (2015-12-28),
#                                                            Log file path(/local/file/path)
#   Author(s)   : DataMetica Team
#   Usage       : setup - 
#						sh module.sh "setup" "/log/file/path" 
#				: Transform - 
#						sh module.sh "transform" "7" "2015-12-28" "/log/file/path"
#				Prepare - 
#						sh module.sh "prepare" "2015-12-28" "/log/file/path"
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
    "${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_FACT}" \
    "${SMITH_ONET_PROD_ESP_OPTIMUM_USAGE_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_SMITH}" \
     "${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_FACT}/${SMITH_ONET_PROD_ESP_OPTIMUM_USAGE_TBL}" \
     "${SMITH_KOM_ONET_PROD_ESP_OPTIMUM_USAGE_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}" \
     "source_date"
       
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_SMITH}" \
     "${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_FACT}/${SMITH_ONET_PROD_ESP_OPTIMUM_USAGE_TBL}" \
     "${SMITH_MKTG_ONET_PROD_ESP_OPTIMUM_USAGE_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}" \
     "dtm_created"
 
}

# Deriving onet prod esp optimum table at Smith layer
fn_module_transform(){

   source_date=$2
   log_file_path=$3
   dtm_created=`date +"%Y-%m-%d"`

   hive_script_path="${MODULE_HOME}/hive/module-transform.hql"

   beeline -u $HIVESERVER2_URL -n ${HIVE_USENAME} -p ${HIVE_PASSWORD}  -hivevar  source_date=$source_date \
      -hivevar  suite_id=$suite_id \
      -hivevar  hive_database_name_gold=${HIVE_DATABASE_NAME_GOLD} \
      -hivevar gold_opt_hit_data_esp_tbl="${HIVE_TABLE_PREFIX}${GOLD_OPT_HIT_DATA_ESP_TBL}" \
      -hivevar location="${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_FACT}/${SMITH_ONET_PROD_ESP_OPTIMUM_USAGE_TBL}/dtm_created=${dtm_created}/source_date=${source_date}" \
      -f $hive_script_path >> ${log_file_path} 2>&1;
}

# Adding Partition to onet prod esp optimum table at Smith layer
function fn_module_prepare(){

  source_date=$1
  log_file_path=$2
  dtm_created=`date +"%Y-%m-%d"`
  location_kom="${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_FACT}/${SMITH_ONET_PROD_ESP_OPTIMUM_USAGE_TBL}/dtm_created=${dtm_created}/source_date=${source_date}"
  location_mktg="${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_FACT}/${SMITH_ONET_PROD_ESP_OPTIMUM_USAGE_TBL}/dtm_created=${dtm_created}/"

 # Alter table to add partition
  fn_alter_table_with_one_partitions_with_location \
     "${HIVE_DATABASE_NAME_SMITH}" \
     "${SMITH_KOM_ONET_PROD_ESP_OPTIMUM_USAGE_TBL}" \
     "source_date='${source_date}'" \
     "${location_kom}" \
     "${BOOLEAN_TRUE}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}" \
     "${log_file_path}"

  fn_alter_table_with_one_partitions_with_location \
     "${HIVE_DATABASE_NAME_SMITH}" \
     "${SMITH_MKTG_ONET_PROD_ESP_OPTIMUM_USAGE_TBL}" \
     "dtm_created='${dtm_created}'" \
     "${location_mktg}" \
     "${BOOLEAN_TRUE}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}" \
     "${log_file_path}"
}


###############################################################################
#                                  Implement                                  #
###############################################################################

#Calling setup function
fn_main "${@:1}"

###############################################################################
#                                     End                                     #
###############################################################################