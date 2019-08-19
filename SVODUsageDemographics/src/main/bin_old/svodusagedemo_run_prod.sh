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

echo "delete if the folder already exists on hdfs"
#hdfs dfs -rm -r /app/util/SvodUsageDemographics

echo "create the dir structure for the svod on hdfs"
hdfs dfs -mkdir /app/util/SvodUsageDemographics/
hdfs dfs -mkdir /app/util/SvodUsageDemographics/oozie
hdfs dfs -mkdir /app/util/SvodUsageDemographics/oozie/lib
hdfs dfs -mkdir /app/util/SvodUsageDemographics/db
hdfs dfs -mkdir /app/util/SvodUsageDemographics/conf
#hdfs dfs -mkdir /incoming/vod/subscription_type_dir

echo "copy hive-site.xml to hdfs"
hdfs dfs -copyFromLocal /usr/hdp/current/hive-server2/conf/hive-site.xml /app/util/SvodUsageDemographics/conf

echo "delete the existing jar file on hdfs"
hdfs dfs -rmr /app/util/SvodUsageDemographics/oozie/lib/*

echo "copy the jar files"
#hdfs dfs -copyFromLocal /UTIL/app/data_encryption/lib/DataEncryption-0.0.1-SNAPSHOT-jar-with-dependencies.jar  /app/util/SvodUsageDemographics/oozie/lib/
hdfs dfs -copyFromLocal /UTIL/app/util/SvodUsageDemographics/lib/SVODUsageDemographics-1.0-SNAPSHOT.jar /app/util/SvodUsageDemographics/oozie/lib/
hdfs dfs -copyFromLocal /UTIL/app/util/SvodUsageDemographics/lib/cvc-hive-udf-0.0.2.jar /app/util/SvodUsageDemographics/oozie/lib/
hdfs dfs -copyFromLocal /UTIL/app/util/SvodUsageDemographics/lib/CVSecurityApplication.jar /app/util/SvodUsageDemographics/oozie/lib/
hdfs dfs -copyFromLocal /UTIL/app/util/SvodUsageDemographics/lib/ojdbc6.jar /app/util/SvodUsageDemographics/oozie/lib/

echo "copy the hql script to hdfs"
hdfs dfs -rm -r /app/util/SvodUsageDemographics/db/svodusagedemo_process_customer_usage_data.hql
hdfs dfs -copyFromLocal /UTIL/app/util/SvodUsageDemographics/db/svodusagedemo_process_customer_usage_data.hql /app/util/SvodUsageDemographics/db/

echo "copy the oozie files"
hdfs dfs -rmr  /app/util/SvodUsageDemographics/oozie/prod_workflow.xml
hdfs dfs -rmr  /app/util/SvodUsageDemographics/oozie/prod_coordinator.xml

hdfs dfs -copyFromLocal /UTIL/app/util/SvodUsageDemographics/oozie/prod_coordinator.xml /app/util/SvodUsageDemographics/oozie/
hdfs dfs -copyFromLocal /UTIL/app/util/SvodUsageDemographics/oozie/prod_workflow.xml /app/util/SvodUsageDemographics/oozie/

#echo "run oozie coordinator"
#oozie job -oozie http://cvlphdpen1.cablevision.com:11000/oozie -config /UTIL/app/util/SvodUsageDemographics/oozie/prod_job.properties -run
