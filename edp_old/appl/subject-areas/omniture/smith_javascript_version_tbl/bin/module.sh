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
#                    and create smith_javascript_version_tbl in smith layer.
#               		Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)                 										  
#                 2. Executes transform function.
#               		Input Arguments for this script are: Phase name (transform), Suite Id (7), 
#                                                             Log file path(/local/file/path)
#   Author(s)   : DataMetica Team
#   Usage       : setup - 
#						sh module.sh "setup" "/log/file/path" 
#				: Transform - 
#						sh module.sh "transform" "${suite_id}" "/log/file/path"
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
. ${SUBJECT_AREA_HOME}/bin/omniture_functions.sh

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
    "${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_DIMENSION}" \
    "${SMITH_JAVASCRIPT_VERSION_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_SMITH}" \
     "${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_DIMENSION}/${SMITH_JAVASCRIPT_VERSION_TBL}" \
     "${SMITH_JAVASCRIPT_VERSION_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"
}

# Deriving javascript version table at Smith layer
function fn_module_transform(){
    suite_id=$1
    log_file_path=$2
    dtm_created=`date +"%Y-%m-%d"`
    hive_script_path="${MODULE_HOME}/hive/module-transform.hql"

    fn_load_smith_lookup_tables \
         "$suite_id" \
         "$dtm_created" \
         "${HIVE_DATABASE_NAME_SMITH}" \
         "${SMITH_JAVASCRIPT_VERSION_TBL}" \
         "${HIVE_DATABASE_NAME_INCOMING}" \
         "${INCOMING_JAVASCRIPT_VERSION_TBL}" \
         "${hive_script_path}" \
         "${log_file_path}" \
         "${HIVESERVER2_URL}" \
         "${HIVE_USENAME}" \
         "${HIVE_PASSWORD}"
         exit_code=$?
         
         if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
      	 then
	  	    return $exit_code
         fi
         
#    beeline -u $HIVESERVER2_URL -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} -hivevar suite_id="$suite_id" \
 #       -hivevar dtm_created="$dtm_created" \
  #      -hivevar hive_database_name_smith="${HIVE_DATABASE_NAME_SMITH}" \
   #     -hivevar smith_javascript_version_tbl="${SMITH_JAVASCRIPT_VERSION_TBL}" \
    #    -hivevar hive_database_name_incoming="${HIVE_DATABASE_NAME_INCOMING}" \
	#	-hivevar incoming_javascript_version_tbl="${INCOMING_JAVASCRIPT_VERSION_TBL}" \
     #   -f $hive_script_path >> ${log_file_path} 2>&1;

}

###############################################################################
#                                  Implement                                  #
###############################################################################

#Calling setup function
fn_main "${@:1}"

###############################################################################
#                                     End                                     #
###############################################################################