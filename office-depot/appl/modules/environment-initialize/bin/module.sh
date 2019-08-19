#!/usr/bin/env bash
################################################################################
#                               General Details                                #
################################################################################
#                                                                              #
# Name                                                                         #
#     : Script to initialize entire environment                                #
# File                                                                         #
#     : module.sh                                                              #
#                                                                              #
# Description                                                                  #
#     :                                                                        #
#                                                                              #
#                                                                              #
#                                                                              #
# Author                                                                       #
#     : Abhijit                                                 #
#                                                                              #
################################################################################
#                           Script Environment Setup                           #
################################################################################

#Find the script file home
pushd . > /dev/null
SCRIPT_HOME="${BASH_SOURCE[0]}";
while([ -h "${SCRIPT_HOME}" ]); do
    cd "`dirname "${SCRIPT_HOME}"`"
    SCRIPT_HOME="$(readlink "`basename "${SCRIPT_HOME}"`")";
done
cd "`dirname "${SCRIPT_HOME}"`" > /dev/null
SCRIPT_HOME="`pwd`";
popd  > /dev/null

#set all parent home environment variables
MODULE_HOME=`dirname ${SCRIPT_HOME}`
MODULES_HOME=`dirname ${MODULE_HOME}`
HOME=`dirname ${MODULES_HOME}`

#Loads user namespace properties file. User may override namespace properties
#in his home folder in that case that file should be loaded after default name
#space properties file is loaded so that user is given chance to create his own 
#namespace.
#NOTE: This parameter is also used in the functions.sh file 
USER_NAMESPACE_PROPERTIES_FILE='~/.nsrc'

#Load all configurations files in order
. ${HOME}/etc/namespace.properties                                                             
if [ -f "$USER_NAMESPACE_PROPERTIES_FILE" ];                                                             
then
. ${USER_NAMESPACE_PROPERTIES_FILE}
fi
. ${HOME}/etc/default.env.properties                                                                     
. ${MODULE_HOME}/etc/module.env.properties


#load all utility functions
. ${HOME}/bin/functions.sh

################################################################################
#                                 Declaration                                  #
################################################################################


##
# Generate data layer folders
# All data layer path environment vatiables must start with DATA_LAYER_ 
# as prefix
#
function fn_create_data_layer_directories_on_hadoop(){
  fn_log_info "Creating all data layer directories on Hadoop"
  for env_variable in `set`; do
    if [[ ${env_variable} == DATA_LAYER_* ]]
    then
      data_layer_env_variable_name=`echo ${env_variable} | awk -F'=' '{print $1}'`
      data_layer_name="${!data_layer_env_variable_name}"
      if [ ! -z "${data_layer_name}" ];
      then
        fail_on_error=$BOOLEAN_TRUE
        fn_hadoop_create_directory_if_not_exists "${data_layer_name}" "${fail_on_error}"
      fi
    fi
  done
}



#Generate source system partitions in the incoming layer
# All source system id environment vatiables must start with SOURCE_SYSTEM_ 
# as 
#
function fn_create_source_system_partitions_in_incoming_data_layer(){
  fn_log_info "Creating all source system partitions in incoming layers on Hadoop"
  for env_variable in `set`; do
    if [[ ${env_variable} == SOURCE_SYSTEM_* ]]
    then
      source_system_env_variable_name=`echo ${env_variable} | awk -F'=' '{print $1}'`
      source_system_id="${!source_system_env_variable_name}"
      if [ ! -z "${source_system_id}" ];
      then
        fail_on_error=$BOOLEAN_TRUE
        fn_hadoop_create_directory_if_not_exists "${DATA_LAYER_INCOMING}/source_system_id=${source_system_id}/batch_id=${INCOMING_RAW_PARTITION_ID}" "${fail_on_error}"
      fi
    fi
  done
}



################################################################################
#                                  Initialise                                  #
################################################################################






################################################################################
#                                Implementation                                #
################################################################################


fn_create_data_layer_directories_on_hadoop


fn_create_source_system_partitions_in_incoming_data_layer


################################################################################
#                                     End                                      #
################################################################################
