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
#                                                                                     #
#                                                                                     #
# Author                                                                              #
#     : SarfarazKhan                         			 					          #
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
source $MODULE_HOME/bin/coremetrics_functions.sh


##############################################################################
#																			 #
#   Importing War file for clickstream project 				 			     #
#                                    										 #
##############################################################################         

#example ---
# scp sarfarazkhanp@159.253.128.86:/home/sarfarazk/Deployement.sh ./

#scp $scp_ser_name@$scp_file_ip:$scp_file_path/$scp_file_name $scp_destination_path

    chmod 777 $scp_destination_path
    
OUT=$?
if [ $OUT -eq 1 ];then
   echo "Failed to Copy Source Code"
else
   echo "Successfully copied source code"
fi

##############################################################################
#																			 #
#   copying oozie workflows from local dir to HDFS directory		         #
#                                    										 #
##############################################################################         

 fn_hadoop_create_directory_if_not_exists $oozie_wf_destination 1 ${logFilePath}
hadoop fs -put -f $oozie_wf_source/$oozie_wf_name $oozie_wf_destination

OUT=$?
if [ $OUT -eq 1 ];then
   echo "Failed to copy Oozie workflow xml files"
else
   echo "Successfully copied Oozie workflow xml"
fi

##############################################################################
#																			 #
#   copying war file from maven to tomcat directory			                 #
#                                    										 #
##############################################################################         


cp $dashboard_war_file_path/$dashboard_war $war_destination
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


#cp $clicklogger_war_file_path/$clicklogger_war $war_destination
#OUT=$?
#if [ $OUT -eq 1 ];then
 #  echo "Failed to copy Click Logger War!"
  # else
   #echo "Successfully copied ClickLogger war"
#fi


##############################################################################
#																			 #
#    copying flume.conf agent from local to flume conf directory		     #
#                                    										 #
##############################################################################         

cp $flume_conf/$flume_agent_name $flume_conf_dest

OUT=$?
if [ $OUT -eq 1 ];then
   echo "Failed to copied flume agent!"
else
   echo "Successfully copied Flume Agent!"
fi

##############################################################################
#																			 #
#      starting flume agent		                                             #
#                                    										 #
##############################################################################         

cd $flume_agent

# Run in Background 
#nohup flume-ng agent --conf $flume_conf_dir --conf-file $flume_conf_dir/$flume_agent_conf_file --name $flume_agent_name -Dflume.root.logger=info,console

#Run on Current Console 
./flume –ng agent –n $flume_agent_name –c conf –f $flume_conf_dir/$flume_agent_name Dflume.root.logger=DEBUG,console -n $flume_agent_name


echo "Successfully Started flume"
OUT=$?
if [ $OUT -eq 1 ];then
   echo " Failed to Start Flume Agent"
else
   echo "Successfully Started Flumn Agent"
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
#																			 #
#      creating Hbase tables                                                 #
#                                    										 #
##############################################################################         

cd $phoenix
./sqlline.py $zk_ip:$zk_port $hbase_tbl_script_path/$hbase_tbl_script_name
    
    echo "Successfully Created HBase Tables !!"
 
##############################################################################
#                                                                            #                                                                            #
# Making directory for cookies                                               #
#                                                                            #                                                                            #
##############################################################################

mkdir $PROJECTS_HOME/$PROJ_COOKIE

##############################################################################
#                                                                            #   
# Unzipping files in cookies directory                                       #
#                                                                            #    
##############################################################################

sh $MODULE_HOME/bin/cookie_unzip.sh

##############################################################################
#                                                                            #
# put cookies.csv file on hdfs path                                          #
#                                                                            #
##############################################################################

table_name=`echo $INCOMING_DIM_COOKIES_MAPPING | cut -d',' -f1`
incoming_filename=`echo $INCOMING_DIM_COOKIES_MAPPING | cut -d',' -f2`
hadoop fs -mkdir -p $INCOMING_HDFS/$table_name
hdfs dfs -put $PROJECTS_HOME/$PROJ_COOKIE/$incoming_filename $INCOMING_HDFS/$table_name

    
###############################################################################
#																			  #
#		loading country table												  #
#												                              #
############################################################################### 

table_name=`echo $INCOMING_DIM_COUNTRY_MAPPING | cut -d',' -f1`
incoming_filename=`echo $INCOMING_DIM_COUNTRY_MAPPING | cut -d',' -f2` 
hadoop fs -mkdir $INCOMING_HDFS/$table_name
hadoop fs -put $MODULE_HOME/etc/$incoming_filename $INCOMING_HDFS/$table_name

echo "country file put successfully"


##############################################################################
#                                    End                                     #
##############################################################################    