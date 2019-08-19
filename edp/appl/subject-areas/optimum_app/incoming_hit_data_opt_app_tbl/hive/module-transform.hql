--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Gold table from Incoming table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 09/12/2016
--#   Log File    : .../log/channel_tuning/
--#   SQL File    : 
--#   Error File  : .../log/channel_tuning/
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          09/12/2016       Initial version
--#
--#
--#####################################################################################################################

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.output.compression.type=BLOCK;
set avro.output.codec=snappy;

INSERT OVERWRITE TABLE ${hivevar:target_database}.${hivevar:target_table}
     PARTITION(SUITE_NAME,SOURCE_DATE)
SELECT 
*
FROM 
	 ${hivevar:source_database}.${hivevar:source_table}
	 where SUITE_NAME='${hivevar:suite_name}' AND SOURCE_DATE='${hivevar:source_date}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################