#!/bin/sh

#######################################################################################
#                              General Details                               		  #
#######################################################################################
#                                                                            		  #
# Name                                                                                #
#     : cookie_unzip      								   	 		                  #
# File                                                                                #
#     : cookie_unzip.sh                                   	                          #
# Description                                                                         #
#     : Read FTP Cookie data and extract it on local filesystem                       #
#  		                                                                              #
#                                                                                     #
#                                                                                     #
#                                                                                     #
# Author                                                                              #
#     : Shweta                               			 					          #
#                                                                                     #
#######################################################################################

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


# Set module, project, subject area home paths.
MODULE_HOME=`dirname ${SCRIPT_HOME}`
MODULES_HOME=`dirname ${MODULE_HOME}`
SUBJECT_AREA_HOME=`dirname ${MODULES_HOME}`
PROJECT_HOME=`dirname ${SUBJECT_AREA_HOME}`
PROJECTS_HOME=`dirname ${PROJECT_HOME}`

source $MODULE_HOME/etc/deployment.properties
source $MODULE_HOME/etc/coremetrics-modules.properties

cd $PROJECTS_HOME/$PROJ_COOKIE

##############################################################################
#                                                                                                                                                        #
# Connecting to SFTP and downloading latest file                             #
#                                                                                                                                                        #
##############################################################################
echo  "Connecting to SFTP Server : $HOST:$SERVER_DIR";
   lftp -u $USER,$PASSWD sftp://$HOST:$SERVER_DIR << end
   echo "Downloading Files to Directory [ $PROJECTS_HOME/$PROJ_COOKIE ]"
   mget "*"$COOKIE_FILE
end

##############################################################################
#                                                                                                                                                        #
# Unzipping files in tmp directory                                                                                   #
#                                                                                                                                                        #
##############################################################################

unzip cookies.zip

##############################################################################
#                                                                                                                                                        #
#                               End                                                                                                                                                      #
##############################################################################
