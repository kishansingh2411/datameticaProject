#!/bin/sh

##############################################################################
#                              General Details                               #
##############################################################################
#                                                                            #
# Name: executor                                                             #
#           								   	  							 #
# File: executor.sh                                                          #
#                                        	   								 #
# Description: Load data from incoming to HBase table.						 #
#                                                                   		 #
# Author: Shweta Karwa      					                             #
#                                                                            #
##############################################################################

echo "!!!!!!!!!!!!!!!!!!!  Executor script started   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

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
source $MODULE_HOME/bin/coverageAnalytics_functions.sh
source $MODULE_HOME/etc/coverage-analytics-modules.properties

##############################################################################
#																			 #
# Calling setup script													     #
#                                    										 #
##############################################################################

sh $MODULE_HOME/bin/setup.sh $MONTHLY

echo "Successfully executed step SetUp !!!!"

##############################################################################
#																			 #
# Calling script that FTP file and Unzip it							         #
#                                    										 #
##############################################################################         

sh $MODULE_HOME/bin/unzip.sh $MONTHLY

echo "Successfully executed step Unzip !!!!"

#############################################################################
#																			#
# ALERT MESSAGE FOR DATABASE AND TABLE CREATION         			        #	
#                                                                           # 
#############################################################################

sh $MODULE_HOME/bin/validate_hive_schema.sh $MONTHLY

echo "Successfully executed step Validate Hive Schema !!!!"

##############################################################################
#																			 #
# Creating hive tables													     #
#                                    										 #
##############################################################################     

sh $MODULE_HOME/bin/create_hive_tables.sh 'ON' $MONTHLY

echo "Successfully executed step Create Hive Schema !!!!"

#################################################################################t#
#																			      #
# Putting incoming files in respective folders and adding partition by batch_id   #
#                                    										 	  #
###################################################################################

for i in "${ARRAY_INCOMING_RAW_TBLS[@]}"
do		
    table_name=$i	    
    dirname=$table_name"_tbl"	    	
    sh $MODULE_HOME/$dirname/bin/module.sh $MONTHLY
done

echo "Successfully loaded data in Incoming layer !!!!"

##############################################################################
#																			 #
# Loading work layer													     #
#                                    										 #
##############################################################################

for i in "${ARRAY_WORK_LAYER_TBLS[@]}"
do
	table_name=$i
	echo "Populating table $table_name"        
	dirname=$table_name"_tbl"       
	sh $MODULE_HOME/$dirname/bin/module.sh $MONTHLY
done

echo "Successfully loaded data in work layer !!!!"



##############################################################################
#																			 #
# Managing Work layer hive tables											 #
#                                    										 #
############################################################################## 

sh $MODULE_HOME/bin/manage_work_hive_tables.sh 'ON' $MONTHLY

echo "Successfully executed step to Add partition to Work layer !!!!"

##############################################################################
#																			 #
# Loading work layer													     #
#                                    										 #
##############################################################################

for i in "${ARRAY_WORK_TBLS[@]}"
do
	table_name=$i        
	dirname=$table_name"_tbl"       
	sh $MODULE_HOME/$dirname/bin/module.sh $MONTHLY
done

echo "Successfully loaded data in work layer !!!!"

echo "Successfully executed step to Add partition to work layer !!!!"

##############################################################################
#																			 #
# Loading Gold layer													     #
#                                    										 #
##############################################################################

for i in "${ARRAY_GOLD_TBLS[@]}"
do
	table_name=$i        
	dirname=$table_name"_tbl"       
	sh $MODULE_HOME/$dirname/bin/module.sh $MONTHLY
done

echo "Successfully loaded data in Gold layer !!!!"

##############################################################################
#																			 #
# Managing Gold layer hive tables											 #
#                                    										 #
##############################################################################     

sh $MODULE_HOME/bin/manage_gold_hive_tables.sh 'ON' $MONTHLY

echo "Successfully executed step to Add partition to Gold layer !!!!"

##############################################################################
#																			 #
#  Loading Outgoing layer												     #
#                                    										 #
##############################################################################

for i in "${ARRAY_OUTGOING_TBLS[@]}"
do
	table_name=$i        
	dirname=$table_name"_tbl"       
	sh $MODULE_HOME/$dirname/bin/module.sh $MONTHLY
done

echo "Successfully loaded data in Outgoing layer !!!!"

##############################################################################
#																			 #
# Managing outgoing layer       											 #
#                                    										 #
############################################################################## 

table_name=$OUTGOING_MERGED      
dirname=$table_name"_tbl" 
sh $MODULE_HOME/$dirname/bin/module.sh $MONTHLY

echo "Successfully executed outgoing manage layer !!!!"

##############################################################################
#																			 #
# Copy data to HBASE													     #
#                                    										 #
##############################################################################

sh $MODULE_HOME/bin/hbase_copy.sh $MONTHLY

echo "Successfully loaded data HBase Table!!!!"

##############################################################################
#																			 #
#  Calling clean_up script												 	 #
#                                    										 #
##############################################################################

sh $MODULE_HOME/bin/clean_up.sh $MONTHLY

echo "Successfully executed step cleanup !!!!"

echo "!!!!!!!!!!!!!!!!!!!  Executor script completed   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

##############################################################################
#                                    End                                     #
##############################################################################