#!/bin/sh

##############################################################################
#                              General Details                               #
##############################################################################
#                                                                            #
# Name: module                                                               #
#           								   	  							 #
# File: module.sh                                                            #
#                                        	   								 #
# Description: Loading data in gold layer.						             #
#                                                                   		 #
# Author: Deepanshu 					                                     #
#                                                                             #
##############################################################################

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
TABLE_HOME=`dirname ${SCRIPT_HOME}`
MODULE_HOME=`dirname ${TABLE_HOME}`
MODULES_HOME=`dirname ${MODULE_HOME}`
SUBJECT_AREA_HOME=`dirname ${MODULES_HOME}`
PROJECT_HOME=`dirname ${SUBJECT_AREA_HOME}`
PROJECTS_HOME=`dirname ${PROJECT_HOME}`


source $MODULE_HOME/bin/coremetrics_functions.sh



##############################################################################
#																			 #
# ### Calling Pig Script to move data from incoming to Gold layer            #
#		Taking two parameters												 #
#		1. Table name to load 2. Root directory								 #
#																			 #
##############################################################################

fn_pig_wrapper $GOLD_TECHNICAL_PROPERTIES $1


##############################################################################
#                                    End                                     #
##############################################################################
