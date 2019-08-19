#!/bin/bash
########################################################################################################################
#
#   Program name: svodusagedemo_load_historical_data_from_uat_to_prod.sh
#   Program type: Unix Shell script
#   Author      : Kriti Singh
#   Date        : 05/29/2016
#
#   Description :
#
#   Usage       :
#
#
########################################################################################################################

hdfs dfs -rm -r hdfs://cvlphdpd1/prcssd/vod/f_vod_orders_mth_corp

#copy from uat hdfs cluster to prod hdfs cluster
hadoop distcp hdfs://cvhdpuat/prcssd/vod/f_vod_orders_mth_corp hdfs://cvlphdpd1/prcssd/vod/f_vod_orders_mth_corp



