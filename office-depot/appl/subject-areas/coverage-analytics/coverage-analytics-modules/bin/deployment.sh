#!/bin/sh

#######################################################################################
#                              General Details                               		  #
#######################################################################################
#                                                                            		  #
# Name                                                                                #
#     : deployment      								   	 		                  #
# File                                                                                #
#     : deployment.sh                                   	                          #
# Description                                                                         #
#     : Deployment script to execute all the component in sequence for clickstream    #
#       project                                                                       #
#  		                                                                              #
#                                                                                     #
# Author                                                                              #
#     : Shweta Karwa                           			 					          #
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
source $MODULE_HOME/etc/coverage-analytics-modules.properties
source $MODULE_HOME/bin/coverageAnalytics_functions.sh

##############################################################################
#																			 #
#   copying oozie workflows from local dir to HDFS directory		         #
#                                    										 #
##############################################################################         

hadoop fs -mkdir -p $oozie_wf_destination

hadoop fs -put -f $oozie_wf_source/$oozie_wf_name $oozie_wf_destination

OUT=$?
if [ $OUT -eq 1 ];then
   echo "Failed to copy Oozie workflow xml files"
else
   echo "Successfully copied Oozie workflow xml"
fi

################################################################################
#																			   #
#   copying Dasboard and DataService war file from target to tomcat directory  #
#                                    										   #
################################################################################         

cp $dashboard_war_file_path $war_destination
OUT=$?
if [ $OUT -eq 1 ];then
   echo "Failed to copy Dashboard war at path [$war_destination]!"
else
   echo "Successfully copied Dashboard war file"
fi


cp $data_service_war_file_path/$data_service_war $war_destination
OUT=$?
if [ $OUT -eq 1 ];then
   echo "Failed to copy Data Service war at path [$war_destination]!"
else
   echo "Successfully copied DataService war!"
fi

##############################################################################
#																			 #
#       starting tomcat                                                      #
#                                    										 #
##############################################################################         

cd $tomcat_bin_path
./shutdown.sh
./startup.sh
OUT=$?
if [ $OUT -eq 1 ];then
   echo " Failed to Start Tomcat !"
else
   echo "Successfully Started !"
fi

##############################################################################
#                                                                            #
# put Standard_Industrial_Classification.csv file on hdfs path               #
#                                                                            #
##############################################################################

table_name=`echo $INCOMING_DIM_NACE_MAPPING | cut -d',' -f1`
incoming_filename=`echo $INCOMING_DIM_NACE_MAPPING | cut -d',' -f2`
hadoop fs -mkdir -p $INCOMING_HDFS/$table_name
hadoop fs -put $MODULE_HOME/etc/$incoming_filename $INCOMING_HDFS/$table_name

echo "Standard_Industrial_Classification file put successfully"

##############################################################################
#																			 #
#      creating Hbase tables                                                 #
#                                    										 #
##############################################################################         

cd $phoenix
./sqlline.py $hbase_ip:$hbase_port $hbase_tbl_script_path/$hbase_tbl_script_name
    
    echo "Successfully Created HBase Table !!"

##############################################################################
#                                    End                                     #
##############################################################################    