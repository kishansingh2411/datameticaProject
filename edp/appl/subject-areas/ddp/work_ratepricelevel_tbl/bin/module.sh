
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
#                    and create work_ratepricelevel_tbl in work layer.
#               		Input Arguments for this script are: Phase name (setup), Log file path(/log/file/path)                 										  
#                 2. Executes prepare function.
#               		Input Arguments for this script are: "prepare" "${max_seq_id}" "${source_table}" "${log_file_path}"
#   Author(s)   : DataMetica Team
#   Usage       : setup - 
#						sh module.sh "setup" "/local/file/path" 
#				: Transform - 
#						sh module.sh "prepare" "${max_seq_id}" "${source_table}" "${log_file_path}" ${start_date} ${end_date}
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
. ${HOME}/etc/postgres.properties
. ${SUBJECT_AREA_HOME}/etc/subject-area.env.properties
. ${SUBJECT_AREA_HOME}/etc/oracle.properties
. ${SUBJECT_AREA_HOME}/etc/ddp.properties
. ${SUBJECT_AREA_HOME}/bin/ddp_functions.sh

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
    "${WORK_RATEPRICELEVEL_TBL}" \
    "${BOOLEAN_FALSE}" \
    "${log_file_path}"
    
  # Create hive table
  fn_create_hive_table \
     "${HIVE_DATABASE_NAME_WORK}" \
     "${DATA_LAYER_DIR_WORK_SUBJECT_AREA_DATA}/${WORK_RATEPRICELEVEL_TBL}" \
     "${WORK_RATEPRICELEVEL_TBL}" \
     "${MODULE_HOME}/schema/module.hql" \
     "${log_file_path}" \
     "${HIVESERVER2_URL}" \
     "${HIVE_USENAME}" \
     "${HIVE_PASSWORD}"     
}

#Preparing incoming EDP_RATEPRICELEVEL_TBL table.
function fn_module_prepare(){   
  
  max_seq_id="$1"
  source_table="$2"
  log_file_path="$3"
  connection_url="${DRIVER}${HOST}:${PORT}:${SCHEMA}"
  start_date="$4"
  end_date="$5"
  
  query="$(fn_build_query_for_import ${source_table} ${ORACLE_DATABASE} ${max_seq_id} ${MEDIUM_TBL} ${start_date} ${end_date})"  

  if [ -z "${query}" ]
  then
      fn_log_error "Failed to build query for ${source_table}" "${log_file_path}"
      return -1
  fi
   
 #Sqoop EDP_RATEPRICELEVEL_TBL table data
  
   sh ${MODULE_HOME}/bin/module.sh setup ${log_file_path}
   exit_code=$?
   
   if [ ${exit_code} -ne ${EXIT_CODE_SUCCESS} ]
   then
      fn_log_error "Failed while recreating the table ${WORK_RATEPRICELEVEL_TBL}" "${log_file_path}"
      return $exit_code
   fi
 
   fn_import_table_to_hive_from_oracle \
     "${connection_url}" \
     "${HIVE_DATABASE_NAME_WORK}" \
     "${WORK_RATEPRICELEVEL_TBL}" \
     "${query}" \
     "${log_file_path}" \
     "${MEDIUM_TBL_MAPPERS}"
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