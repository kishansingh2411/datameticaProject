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
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-prepare.hql
--#   Date        : 01/18/2017
--#   Log File    : .../log/ovcdr/EDP_*.log
--#   SQL File    : 
--#   Error File  : .../log/ovcdr/EDP_*.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          01/18/2017       Initial version
--#
--#
--#####################################################################################################################

set hive.vectorized.execution.enabled=false;

INSERT OVERWRITE TABLE
   ${hivevar:work_database}.${hivevar:table_prefix}${hivevar:work_opt_sdl_table}
SELECT
      PHONE_NBR
      ,1 AS  SOURCE_SYSTEM_ID
      ,DDP_ACCOUNT AS CUST_ACCT_NUMBER
   FROM ${hivevar:hive_database_name_ods_gold}.${hivevar:table_prefix}${hivevar:gold_opt_table}
   WHERE TO_DATE('${hivevar:source_date}') >= TO_DATE(DTM_EFFTV)
      AND TO_DATE('${hivevar:source_date}') < TO_DATE(DTM_EXPIRED)
   UNION
   SELECT
      PHONE_NBR
      ,4 AS  SOURCE_SYSTEM_ID
      ,CONCAT(SITE_ID,ACCT_NBR) AS CUST_ACCT_NUMBER
   FROM ${hivevar:hive_database_name_ods_gold}.${hivevar:table_prefix}${hivevar:gold_sdl_table}
   WHERE TO_DATE('${hivevar:source_date}') >= TO_DATE(DTM_EFFTV)
      AND TO_DATE('${hivevar:source_date}') < TO_DATE(DTM_EXPIRED)
;

--##############################################################################
--#                                    End                                     #
--##############################################################################