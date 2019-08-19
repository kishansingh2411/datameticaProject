--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_t_f_household_device_tbl)
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

set hive.exec.compress.output=true;
set avro.output.codec=snappy;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE 
   ${hivevar:target_database}.${hivevar:table_prefix}${hivevar:target_table} 
   PARTITION (LOAD_MONTH)
SELECT 
   a.*, from_unixtime(unix_timestamp(a.load_date,'yyyy-MM-dd hh:mm:ss'),'yyyyMM') as LOAD_MONTH 
FROM ${hivevar:source_database}.${hivevar:table_prefix}${hivevar:source_table} a
;

--##############################################################################
--#                                    End                                     #
--##############################################################################