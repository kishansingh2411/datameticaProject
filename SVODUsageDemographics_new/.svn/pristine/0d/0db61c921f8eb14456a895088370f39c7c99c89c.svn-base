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
#   Purpose:    : Functions to prepare INCOMING_KOM_CUSTOMER_ACCOUNT_TBL table in incoming layer
#                 1. Executes setup function to create INCOMING_KOM_CUSTOMER_ACCOUNT_TBL table incoming Layer.
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
    "${INCOMING_KOM_CUSTOMER_ACCOUNT_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_INCOMING}" \
     "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA}/${INCOMING_KOM_CUSTOMER_ACCOUNT_TBL}" \
     "${INCOMING_KOM_CUSTOMER_ACCOUNT_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"
  
}

###############################################################################
#                                  Implement                                  #
###############################################################################

#Calling setup function
fn_main "${@:1}"

###############################################################################
#                                     End                                     #
###############################################################################