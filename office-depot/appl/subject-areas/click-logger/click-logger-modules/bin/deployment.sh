#!/bin/sh

#######################################################################################
#                              General Details                                        #
#######################################################################################
#                                                                                     #
# Name                                                                                #
#     : click logger                                                                  #
# File                                                                                #
#     : Deployement.sh                                                                #
# Description                                                                         #
#     : First time to deploye the project          		                      #
#       project                                                                       #
#                                                                                     #
#                                                                                     #
#                                                                                     #
#                                                                                     #
# Author                                                                              #
#     : SarfarazKhan                                                                  #
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
source $MODULE_HOME/etc/click-logger-modules.properties
 

##############################################################################
#								                                             #
#     Function to convert windows files to linux environment		         #
#									                                         #
# Taking one parameter							                             #
# 	1. Path of File							                                 #
# 	2. Giving Permission to path      				                         #
#									                                         #
##############################################################################

echo "$MODULE_HOME"
echo "$MODULES_HOME"

echo "Providing permission $MODULE_HOME"
#chmod 777 -R $MODULE_HOME
echo "Given Permission to $MODULE_HOME"
echo "Converting File to Unix format"
#find $MODULE_HOME -type f -exec dos2unix {} \;

echo "Converted Files of $MODULE_HOME to Linux Executable"



##############################################################################
#																			 #
#   copying oozie workflows from local dir to HDFS directory		         #
#                                    										 #
##############################################################################         



#hadoop fs -mkdir -p $oozie_wf_destination

#hadoop fs -put -f $MODULE_HOME/$oozie_wf_names $oozie_wf_destination

OUT=$?
if [ $OUT -eq 1 ];then
   echo "Failed to copy Oozie workflow xml files"
else
   echo "Successfully copied Oozie workflow xml"
fi



##############################################################################
#	         																 #
#   Copying jar & war file from maven to flume lib and tomcat webapps        #
#   directory                                   			                 #
#                                    										 #
##############################################################################         

#cp $MODULE_HOME/$Avro_jar_file_path/$Avro_jar_file_name $Avro_jar_destination
OUT=$?
if [ $OUT -eq 1 ];then
   echo "Failed to copy Avro jar at path [$Avro_jar_destination]!"
else
   echo "Successfully copied Avro jar file"
fi

#cp $PROJECT_HOME/$clicklogger_war_file_path/$clicklogger_war_file_name $clicklogger_war_file_destination
OUT=$?
if [ $OUT -eq 1 ];then
   echo "Failed to copy Click Logger War!"
   else
   echo "Successfully copied ClickLogger war"
fi
##############################################################################
#                                                                            #
#   Creating Hive Table on Avro Data                                         #
#                                                                            #
##############################################################################

echo "MODLE home $MODULE_HOME"
echo "MODLES home $MODULES_HOME"
echo "proj home $PROJECT_HOME"
echo "projs home $PROJECTS_HOME"
hive -hiveconf DB_INCOMING=$DB_INCOMING -hiveconf table_name=$table_name -hiveconf avro_scehma_path=$avro_scehma_path -hiveconf avro_schema_file=$avro_schema_file -hiveconf avro_incoming_dir=$avro_incoming_dir -f $MODULE_HOME/$hive_script_name".hql"

OUT=$?
if [ $OUT -eq 1 ];then
   echo "Failed to Create Table!"
   else
   echo "Successfully Created Table"
fi

exit
##############################################################################
#									                                         #
#      starting flume agent		                                             #
#                                                                            #   
##############################################################################         

#cd $flume_agent

# Run in Background 
#nohup flume-ng agent --conf $flume_conf_dir --conf-file $flume_conf_dir/$flume_agent_conf_file --name $flume_agent_name -Dflume.root.logger=info,console

#Run on Current Console 
#./flume–ng agent –n $flume_agent_name -c conf –f $flume_conf_dir/$flume_conf_name Dflume.root.logger=DEBUG,console -n $flume_agent_name


echo "Successfully Started flume"
OUT=$?
if [ $OUT -eq 0 ];then
   echo " Failed to Start Flume Agent"
else
   echo "Successfully Started Flumn Agent"
fi



##############################################################################
#                                                                            #
#      END OF SCRIPTS                                                        #
#                                                                            #
##############################################################################


