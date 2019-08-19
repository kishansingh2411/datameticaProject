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
#                    and create smith_derived_tbl in smith layer.
#               		Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)                 										  
#                 2. Executes transform function.
#               		Input Arguments for this script are: Phase name (transform), Suite name(cablevis-uow-com),
#															 Suite Id (7), Source date(2016-02-15), 
#															 Log file path(/local/file/path)
#   Author(s)   : DataMetica Team
#   Usage       : setup - 
#						sh module.sh "setup" "/log/file/path" 
#				: Transform - 
#						sh module.sh "transform" "${suite_name}" "${suite_id}" "2015-12-28" "/log/file/path"
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
    "${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_DIMENSION}" \
    "${SMITH_OPTIMUM_ISP_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
           
  fn_module_create_hive_table_data_layer_directory_structure \
    "${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_DIMENSION}" \
    "${SMITH_OPTIMUM_SITE_SECTIONS_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
   fn_module_create_hive_table_data_layer_directory_structure \
    "${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_DIMENSION}" \
    "${SMITH_OPTIMUM_PAGENAMES_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
    fn_module_create_hive_table_data_layer_directory_structure \
    "${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_DIMENSION}" \
    "${SMITH_OPTIMUM_ERROR_MESSAGE_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
# Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_SMITH}" \
     "${DATA_LAYER_DIR_SMITH_SUBJECT_AREA_DIMENSION}" \
     "" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"
}

# Deriving derived tables at Smith layer

function fn_module_transform(){

    suite_name=$1
    suite_id=$2
    source_date=$3
    log_file_path=$4

    module_name=`basename ${MODULE_HOME}`
    
        if [[ $suite_name == "vow" ]]
        then
            echo "Executing for vow"
            table_name=${GOLD_VOW_HIT_DATA_TBL}
        elif [[ $suite_name == "vow-esp" ]]
        then
            echo "Executing for vow-esp"
            table_name=${GOLD_VOW_HIT_DATA_ESP_TBL}
        elif [[ $suite_name == "onet_prod" ]]
        then
            echo "Executing for onet_prod"
            table_name=${GOLD_OPT_HIT_DATA_TBL}
        elif [[ $suite_name == "onet_prod_esp" ]]
        then
            echo "Executing for onet_prod_esp"
            table_name=${GOLD_OPT_HIT_DATA_ESP_TBL}
        elif [[ $suite_name == "cablevis-uow-com" ]]
        then
            echo "Executing for cablevis-uow-com"
            table_name=${GOLD_UNIFIED_HIT_DATA_TBL}
        elif [[ $suite_name == "channel900" ]]
        then
            echo "Executing for channel900"
            table_name=${GOLD_STOCK36_HIT_DATA_TBL}
        else
                fn_log_error "Got invalid suite_name $suite_name while transform phase of $frequency pageview counts aggregate"  ${log_file_path}
                return -1 
        fi
     
     if [[ $suite_name == "channel900" ]]
        then
          hive_script_path="${MODULE_HOME}/hive/module-channel900_transform.hql" 
         else  
           hive_script_path="${MODULE_HOME}/hive/module-transform.hql"
    fi
    
    beeline -u $HIVESERVER2_URL -n ${HIVE_USENAME} -p ${HIVE_PASSWORD} -hivevar suite_id="$suite_id" \
        -hivevar suite_name="$suite_name" \
        -hivevar source_date="$source_date" \
        -hivevar table_prefix="${HIVE_TABLE_PREFIX}" \
        -hivevar hive_database_name_smith="${HIVE_DATABASE_NAME_SMITH}" \
        -hivevar hive_database_name_gold="${HIVE_DATABASE_NAME_GOLD}" \
        -hivevar gold_hit_data_tbl="${HIVE_TABLE_PREFIX}${table_name}" \
        -f $hive_script_path >> ${log_file_path} 2>&1;
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
