#!/bin/bash
########################################################################################################################
#
#   Program name: compute_max_date.sh
#   Program type: Unix Shell script
#   Author      : Kriti Singh
#   Date        : 05/29/2016
#
#   Description : This Shell script
#
#   Usage       : run.sh
#
#
########################################################################################################################

echo "create the log directory"
mkdir /UTIL/app/util/SvodUsageDemographics/logs

echo "delete if the directory already exists on hdfs"
hdfs dfs -rm -r /appbis/app/SvodUsageDemographics

echo "create all the dir structure on hdfs"
hdfs dfs -mkdir /appbis/app/SvodUsageDemographics/
hdfs dfs -mkdir /appbis/app/SvodUsageDemographics/oozie
hdfs dfs -mkdir /appbis/app/SvodUsageDemographics/oozie/lib
hdfs dfs -mkdir /appbis/app/SvodUsageDemographics/db
hdfs dfs -mkdir /appbis/app/SvodUsageDemographics/conf

echo "copy hive-site.xml to hdfs"
hdfs dfs -copyFromLocal /usr/hdp/current/hive-server2/conf/hive-site.xml /appbis/app/SvodUsageDemographics/conf

echo "delete the existing jar file on hdfs"
hdfs dfs -rmr /UTIL/app/util/SvodUsageDemographics/oozie/lib/*

echo "copy the jar files"
#hdfs dfs -copyFromLocal /UTIL/app/data_encryption/lib/DataEncryption-0.0.1-SNAPSHOT-jar-with-dependencies.jar  /appbis/app/SvodUsageDemographics/oozie/lib/
hdfs dfs -copyFromLocal /UTIL/app/util/SvodUsageDemographics/lib/SVODUsageDemographics-1.0-SNAPSHOT.jar /appbis/app/SvodUsageDemographics/oozie/lib/
hdfs dfs -copyFromLocal /UTIL/app/util/SvodUsageDemographics/lib/cvc-hive-udf-0.0.2.jar /appbis/app/SvodUsageDemographics/oozie/lib/
hdfs dfs -copyFromLocal /UTIL/app/util/SvodUsageDemographics/lib/CVSecurityApplication.jar /appbis/app/SvodUsageDemographics/oozie/lib/
hdfs dfs -copyFromLocal /UTIL/app/util/SvodUsageDemographics/lib/ojdbc6.jar /appbis/app/SvodUsageDemographics/oozie/lib/

echo "copy the hql script to hdfs"
hdfs dfs -rm -r /appbis/app/SvodUsageDemographics/db/svodusagedemo_process_customer_usage_data.hql
hdfs dfs -copyFromLocal /UTIL/app/util/SvodUsageDemographics/db/svodusagedemo_process_customer_usage_data.hql /appbis/app/SvodUsageDemographics/db/

echo "copy the oozie files"
hdfs dfs -rmr  /appbis/app/SvodUsageDemographics/oozie/dev_workflow.xml
hdfs dfs -rmr  /appbis/app/SvodUsageDemographics/oozie/dev_coordinator.xml

hdfs dfs -copyFromLocal /UTIL/app/util/SvodUsageDemographics/oozie/dev_coordinator.xml /appbis/app/SvodUsageDemographics/oozie/
hdfs dfs -copyFromLocal /UTIL/app/util/SvodUsageDemographics/oozie/dev_workflow.xml /appbis/app/SvodUsageDemographics/oozie/

#echo "run oozie coordinator"
#oozie job -oozie http://ip-10-177-228-223.ec2.internal:11000/oozie -config /UTIL/app/util/SvodUsageDemographics/oozie/dev_job.properties -run