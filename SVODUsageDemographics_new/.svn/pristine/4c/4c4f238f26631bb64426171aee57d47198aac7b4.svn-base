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
#   Purpose:    : Functions to prepare incoming_stg1_vod_orders_mth_corp table in incoming layer
#                 1. Executes setup function to create incoming_stg1_vod_orders_mth_corp table incoming Layer.
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
    "${INCOMING_STG1_VOD_ORDERS_MTH_CORP_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_INCOMING}" \
     "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA}/${INCOMING_STG1_VOD_ORDERS_MTH_CORP_TBL}" \
     "${INCOMING_STG1_VOD_ORDERS_MTH_CORP_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"
  
}

#Transforming gold_acct_hier_fa_dim table at Gold layer
function fn_module_transform(){
 
  log_file_path="$1"
  year_month_id="$2"
  year=`echo "$year_month_id" | cut -c1-4`
  month=`echo "$year_month_id" | cut -c5-6`
  
  hql_file="${MODULE_HOME}/hive/module-transform.hql"
   
  beeline -u ${HIVESERVER2_URL} -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} \
   -hivevar source_date=${source_date} \
   -hivevar year_month_id=${year_month_id} \
   -hivevar month_id=${month} \
   -hivevar year=${year} \
   -hivevar gold_database="${HIVE_DATABASE_NAME_GOLD}" \
   -hivevar incoming_database="${HIVE_DATABASE_NAME_INCOMING}" \
   -hivevar table_prefix="${HIVE_TABLE_PREFIX}" \
   -hivevar incoming_stg1_vod_orders_mth_corp_table="${INCOMING_STG1_VOD_ORDERS_MTH_CORP_TBL}" \
   -hivevar incoming_kom_f_vod_order_table="${INCOMING_KOM_F_VOD_ORDER_TBL}" \
   -hivevar incoming_kom_d_period_table="${INCOMING_KOM_D_PERIOD_TBL}" \
   -hivevar incoming_kom_vod_order_table="${INCOMING_KOM_VOD_ORDER_TBL}" \
   -hivevar incoming_kom_customer_account_table="${INCOMING_KOM_CUSTOMER_ACCOUNT_TBL}" \
   -hivevar incoming_kom_d_product_master_table="${INCOMING_KOM_D_PRODUCT_MASTER_TBL}" \
   -hivevar incoming_kom_d_genre_table="${INCOMING_KOM_D_GENRE_TBL}" \
   -hivevar incoming_kom_d_subscription_group_table="${INCOMING_KOM_D_SUBSCRIPTION_GROUP_TBL}" \
   -hivevar gold_table="${GOLD_D_GEOG_CORP_TBL}" \
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