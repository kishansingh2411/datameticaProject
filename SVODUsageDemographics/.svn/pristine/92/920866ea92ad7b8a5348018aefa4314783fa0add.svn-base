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
#   Purpose:    : Functions to prepare incoming_encrypted_d_ethnic_chc_tbl table in incoming layer
#                 1. Executes setup function to create incoming_encrypted_d_ethnic_chc_tbl table in incoming Layer.
#                    Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)  
#                 2. Executes transform function to build incoming table.
#                    Input Arguments for this script are: Phase name (transform), "${source_date}" "/log/file/path"
#
#   Author(s)   : DataMetica Team
#   Usage       : setup -
#                    sh module.sh "setup" "/log/file/path"
#               : transform -
#                    sh module.sh "transform" "${source_date}" "/log/file/path"
#   Date        : 01/18/2017
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
#    1.0     DataMetica Team          03/18/2017       Initial version
#
#
#####################################################################################################################

###############################################################################
#                          Module Environment Setup                           #
###############################################################################

###                                                                           
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

###                                                                           
# Set module, project, subject area home paths.                               
MODULE_HOME=`dirname ${SCRIPT_HOME}`
SUBJECT_AREA_HOME=`dirname ${MODULE_HOME}`
SUBJECT_AREAS_HOME=`dirname ${SUBJECT_AREA_HOME}`
HOME=`dirname ${SUBJECT_AREAS_HOME}`

###                                                                           
# Load all environment properties files                                       
source $HOME/common/etc/namespace.properties
source $HOME/common/etc/beeline.properties
source $HOME/common/etc/default.env.properties
source $HOME/common/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $HOME/common/bin/functions.sh
source $SUBJECT_AREA_HOME/etc/svodusagedemographics.properties

###############################################################################
#                                   Declare                                   #
###############################################################################

# Create initial setup for the tables to process the batch load 
function fn_module_setup(){

  log_file_path=$1
  
  # Create module data directory if not exists.
  fn_module_create_hive_table_data_layer_directory_structure \
    "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA}" \
    "${INCOMING_ENCRYPTED_D_ETHNIC_CHC_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_INCOMING}" \
     "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA}/${INCOMING_ENCRYPTED_D_ETHNIC_CHC_TBL}" \
     "${INCOMING_ENCRYPTED_D_ETHNIC_CHC_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"
  
}

#Transforming gold_acct_hier_fa_dim table at Gold layer
function fn_module_transform(){
 
  log_file_path="$1"
  hql_file="${MODULE_HOME}/hive/module-transform.hql"
  jar_path=hdfs://${NAMENODE_SERVICE}${HDFS_APP_PREFIX_SUBJECT_AREA_OOZIE_LIB}
   
  beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} \
   -hivevar source_date=${source_date} \
   -hivevar jar_path=${jar_path} \
   -hivevar gold_database="${CAM_DATABASE_GOLD}" \
   -hivevar incoming_database="${HIVE_DATABASE_NAME_INCOMING}" \
   -hivevar table_prefix="${HIVE_TABLE_PREFIX}" \
   -hivevar gold_table="${GOLD_D_ETHNIC_CHC_TBL}" \
   -hivevar incoming_table="${INCOMING_ENCRYPTED_D_ETHNIC_CHC_TBL}" \
   -f "${hql_file}" 1>> "${log_file_path}" 2>> "${log_file_path}" 
   exit_code=$?
   if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   then
	  return $exit_code
   fi  
}
###############################################################################
#                                  Implement                                  #
###############################################################################

#Calling setup function
fn_main "${@:1}"

###############################################################################
#                                     End                                     #
###############################################################################