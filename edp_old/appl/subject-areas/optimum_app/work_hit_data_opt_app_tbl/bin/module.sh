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
#   Purpose:    : Functions to prepare Hit_data table in Incoming layer
#                 1. Executes setup function to create Hit_data table in Incoming Layer.
#                    Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)  
#                 2. Executes transform function to build Incoming table.
#                    Input Arguments for this script are: Phase name (transform),Suite name (STOCK_97), 
#                                                         Suite Id (9), Source date (2015-12-28),
#                                                         Log file path(/local/file/path)
#   Author(s)   : DataMetica Team
#   Usage       : setup -
#                    sh module.sh "setup" "/log/file/path"
#               : transform -
#                    sh module.sh "transform" "${suite_name}" "${suite_id}" "2015-12-28" "/log/file/path"
#   Date        : 09/12/2016
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
#    1.0     DataMetica Team          09/12/2016       Initial version
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
. ${HOME}/etc/namespace.properties
. ${HOME}/etc/beeline.properties
. ${HOME}/etc/default.env.properties
. ${SUBJECT_AREA_HOME}/etc/subject-area.env.properties
. ${SUBJECT_AREA_HOME}/etc/optimum_app.properties

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
    "${DATA_LAYER_DIR_WORK_SUBJECT_AREA_DATA}" \
    "${WORK_HIT_DATA_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"

  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_WORK}" \
     "${DATA_LAYER_DIR_WORK_SUBJECT_AREA_DATA}/${WORK_HIT_DATA_TBL}" \
     "${WORK_HIT_DATA_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"
}

#Transforming the Hit_data table at Incoming layer                                                                                                                                          
function fn_module_prepare(){

   
  suite_id=$1
  source_date=$2
  suite_name=$3
  log_file_path=$4
  
   fn_alter_table_with_two_partitions \
     "${HIVE_DATABASE_NAME_WORK}" \
     "${WORK_HIT_DATA_TBL}" \
     "suite_name='${suite_name}'" \
     "source_date='${source_date}'" \
     "${BOOLEAN_FALSE}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}" \
     "${log_file_path}"
   exit_code=$?    
    
   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
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