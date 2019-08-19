################################################################################
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
