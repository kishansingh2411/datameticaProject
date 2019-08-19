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
# Author: Vikram / Deepanshu					                             #
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
source $MODULE_HOME/bin/coremetrics_functions.sh
source $COREMETRICS_PROPERTIES_MODULE_HOME/etc/coremetrics-modules.properties

##############################################################################
#																			 #
# Calling setup script													     #
#                                    										 #
##############################################################################

sh $MODULE_HOME/bin/setup.sh $DAILY

echo "Successfully executed step SetUp !!!!"

##############################################################################
#																			 #
# Calling script that FTP file and Unzip it							         #
#                                    										 #
##############################################################################         

sh $MODULE_HOME/bin/unzip.sh $DAILY

echo "Successfully executed step Unzip !!!!"

#############################################################################
#																			#
# ALERT MESSAGE FOR DATABASE AND TABLE CREATION         			    #	
#                                                                           # 
#############################################################################

sh $MODULE_HOME/bin/validate_hive_schema.sh $DAILY

echo "Successfully executed step Validate Hive Schema !!!!"

##############################################################################
#																			 #
# Creating hive tables													     #
#                                    										 #
##############################################################################     

sh $MODULE_HOME/bin/create_hive_tables.sh 'ON' $DAILY

echo "Successfully executed step Create Hive Schema !!!!"

#################################################################################t#
#																			      #
# Putting incoming files in respective folders and adding partition by batch_id    #
#                                    										 	  #
###################################################################################

for i in "${ARRAY_INCOMING_RAW_TBLS[@]}"
do		
    table_name=$i	    
    dirname=$table_name"_tbl"	    	
    sh $MODULE_HOME/$dirname/bin/module.sh $DAILY
done

echo "Successfully loaded data in Incoming layer !!!!"

#################################################################################t#
#																			      #
# Join Geography data with Country and Cookie data										      #
#                                    										 	  #
###################################################################################

sh $MODULE_HOME/$WORK_GEOGRAPHY_COUNTRY_COOKIE"_tbl"/bin/module.sh $DAILY

echo "Successfully Join Geography Data with Cookie data at Work layer !!!!"

##############################################################################
#																			 #
# Loading Gold layer													     #
#                                    										 #
##############################################################################

for i in "${ARRAY_GOLD_TBLS[@]}"
do
	table_name=$i        
	dirname=$table_name"_tbl"       
	sh $MODULE_HOME/$dirname/bin/module.sh $DAILY
done

echo "Successfully loaded data in Gold layer !!!!"

##############################################################################
#																			 #
# Managing Gold layer hive tables											 #
#                                    										 #
##############################################################################     

sh $MODULE_HOME/bin/manage_hive_tables.sh 'ON' $DAILY

echo "Successfully executed step to Add partition to Gold layer !!!!"

##############################################################################
#																			 #
# Loading Work layer													     #
#                                    										 #
##############################################################################

for i in "${ARRAY_WORK_TBLS[@]}"
do
	table_name=$i			
	dirname=$table_name"_tbl"       
	sh $MODULE_HOME/$dirname/bin/module.sh $DAILY
	
done

echo "Successfully loaded data in Work layer !!!!"

##############################################################################
#																			 #
#  Loading Outgoing layer												     #
#                                    										 #
##############################################################################

sh $MODULE_HOME/$HBASE_TBL"_tbl"/bin/module.sh $DAILY
#sh $MODULE_HOME/$OUTGOING_ORDER_TBL"_tbl"/bin/module.sh $DAILY

echo "Successfully loaded data in Outgoing layer !!!!"

##############################################################################
#																			 #
# Copy data to HBASE													     #
#                                    										 #
##############################################################################

sh $MODULE_HOME/bin/hbase_copy.sh $DAILY

echo "Successfully loaded data HBase Table!!!!"

##############################################################################
#																			 #
# Loading data to Validation table										     #
#                                    										 #
##############################################################################
sh $MODULE_HOME/bin/data_validation.sh

echo "Successfully loaded data validation Table!!!!"

##############################################################################
#																			 #
#  Calling clean_up script												 	 #
#                                    										 #
##############################################################################

sh $MODULE_HOME/bin/clean_up.sh $DAILY

echo "Successfully executed step cleanup !!!!"

echo "!!!!!!!!!!!!!!!!!!!  Executor script completed   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

##############################################################################
#                                    End                                     #
##############################################################################