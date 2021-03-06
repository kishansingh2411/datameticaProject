#!/bin/sh

##############################################################################
#                              General Details                               #
##############################################################################
#                                                                            #
# Name: module                                                               #
#           								   	  							 #
# File: module.sh                                                            #
#                                        	   								 #
# Description: Loading data in work layer.						         #
#                                                                   		 #
# Author: Sonali Rawool 					                                     #
#                                                                            #
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
TABLE_HOME=`dirname ${SCRIPT_HOME}`
MODULE_HOME=`dirname ${TABLE_HOME}`
MODULES_HOME=`dirname ${MODULE_HOME}`
SUBJECT_AREA_HOME=`dirname ${MODULES_HOME}`
PROJECT_HOME=`dirname ${SUBJECT_AREA_HOME}`
PROJECTS_HOME=`dirname ${PROJECT_HOME}`

source $MODULE_HOME/bin/coverageAnalytics_functions.sh

##############################################################################
#																			 #
# ### Calling Pig Script to move data from incoming to Work layer            #
#		Taking two parameters												 #
#		1. Table name to load 												 #
#		2. Record Type								 					     #
#																			 #
##############################################################################

fn_pig_wrapper "$WORK_CDMPRDDTA_DM_ASSOCIATE" "$1"

##############################################################################
#                                    End                                     #
##############################################################################
