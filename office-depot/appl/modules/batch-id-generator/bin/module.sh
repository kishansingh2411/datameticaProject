############################################################
####################
#                               General Details                                #
################################################################################
#                                                                              #
# Name                                                                         #
#     : Batch ID generator                                                     #
# File                                                                         #
#     : batch_id_generator.sh                                                  #
#                                                                              #
# Description                                                                  #
#     :                                                                        #
#                                                                              #
#                                                                              #
#                                                                              #
# Author                                                                       #
#     : abhijit                                                 #
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
# Generate new batch id
#
# 
function fn_generate_batch_id(){
  fn_log_info "Generating new batch id"
  fail_on_error=$BOOLEAN_TRUE
  fn_local_create_directory_if_not_exists "${BATCH_ID_DIR}" "${fail_on_error}"
  new_batch_id=`date +"%Y%d%m%H%M%S"`
  current_batch_id_file="${BATCH_ID_DIR}/current"
  if [ ! -f "${current_batch_id_file}" ]
  then
    fn_log_info "Current batch id file ${current_batch_id_file} does not exists."
  else
    current_batch_id=`cat ${current_batch_id_file}`
    mv "${current_batch_id_file}" "${BATCH_ID_DIR}/${current_batch_id}"
    exit_code=$?
    dont_fail_on_error=0
    success_message="Moved current batch id file to ${BATCH_ID_DIR}/${current_batch_id}"
    failure_messages="Failed to move current file to ${BATCH_ID_DIR}/${current_batch_id} file"
    fn_log_error "${exit_code}" "${success_message}" "${failure_messages}" "${dont_fail_on_error}"        
  fi 
  touch "${current_batch_id_file}"
  echo "${new_batch_id}" > "${current_batch_id_file}"
  exit_code=$?
  fail_on_error=1
  success_message="Generated new batch id is ${new_batch_id} and wrote it to ${current_batch_id_file} file "
  failure_messages="Failed to write new batch id ${new_batch_id} to ${current_batch_id_file} file"
  fn_log_error "${exit_code}" "${success_message}" "${failure_messages}" "${fail_on_error}"
}



################################################################################
#                                  Initialise                                  #
################################################################################






################################################################################
#                                Implementation                                #
################################################################################



#Generate new batch id
fn_generate_batch_id



################################################################################
#                                     End                                      #
################################################################################
