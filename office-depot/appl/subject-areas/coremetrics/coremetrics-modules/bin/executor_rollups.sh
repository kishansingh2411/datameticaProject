#!/bin/sh

#####################################################################################
#                              General Details                                      #
#####################################################################################
#                                                                                   #
# Name: executor_rollups                                                            #
#           								   	  							        #
# File: executor_rollups.sh                                                         #
#                                        	   								        #
# Description: Load data from incoming to HBase table for specified frequency.		#
#                                                                   		        #
# Author: Sonali Rawool					                                            #
#                                                                                   #
#####################################################################################

echo "!!!!!!!!!!!!!!!!!!!  Executor script for $1 rollup !!!!!!!!!!!!!!!!!!!!!!!!!!"

##############################################################################
#																			 #
# Importing File												 			 #
#                                    										 #
##############################################################################         

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

source $PROJECT_HOME/bin/functions.sh
source $MODULE_HOME/bin/coremetrics_functions.sh
source $COREMETRICS_PROPERTIES_MODULE_HOME/etc/coremetrics-modules.properties

if [ -z "$1" ]
  then
    echo "Please provide any of the Rollup Frequency [ weekly or monthly or quarterly or yearly ]!!!"
    exit;
fi

##############################################################################
#																			 #
# Calling setup script													     #
#                                    										 #
##############################################################################

sh $MODULE_HOME/bin/setup.sh $1

echo "Successfully executed step setup !!!!"

##############################################################################
#																			 #
# Calling rollup_scripts for specified frequency							 #
#                                    										 #
##############################################################################         

sh $MODULE_HOME/$ROLLUPS"_tbl"/bin/module.sh $1

echo "Successfully executed step rollup !!!!"

##############################################################################
#																			 #
# Copy data to HBASE													     #
#                                    										 #
##############################################################################

sh $MODULE_HOME/bin/hbase_copy.sh $1

echo "Successfully loaded data to HBase Table !!!!"

##############################################################################
#																			 #
#  Calling clean_up script												 	 #
#                                    										 #
##############################################################################

sh $MODULE_HOME/bin/clean_up.sh $1

echo "Successfully executed step cleanup !!!!"

echo "!!!!!!!!!!  Successfully executed rollup for $1 HBase Table !!!!!!!!!!!!"

##############################################################################
#                                    End                                     #
##############################################################################