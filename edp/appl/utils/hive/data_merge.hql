--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_auxcust_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-prepare.hql
--#   Date        : 05/02/2016
--#   Log File    : 
--#   SQL File    : 
--#   Error File  : 
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          05/02/2016       Initial version
--#
--#
--#####################################################################################################################

CREATE TEMPORARY TABLE 
   ${hivevar:target_table}_temp AS 
   SELECT 
      * 
   FROM ${hivevar:target_database}.${hivevar:table_prefix}${hivevar:target_table} 
   WHERE ${hivevar:where_condition}
;

set hive.exec.compress.output=true
;

set hive.exec.dynamic.partition.mode=nonstrict
;

set hive.merge.tezfiles=true
;

set hive.merge.mapfiles=true
;

set hive.merge.mapredfiles=true
;

set hive.merge.smallfiles.avgsize=1000000000
;

INSERT OVERWRITE TABLE 
   ${hivevar:target_database}.${hivevar:table_prefix}${hivevar:target_table} 
   PARTITION (${hivevar:partition_columns}) 
   SELECT 
      * 
   FROM ${hivevar:target_table}_temp
;

--##############################################################################
--#                                    End                                     #
--##############################################################################