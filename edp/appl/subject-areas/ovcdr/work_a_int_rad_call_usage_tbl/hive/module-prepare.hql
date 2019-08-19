--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Work table from Incoming table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-prepare.hql
--#   Date        : 12/28/2016
--#   Log File    : .../log/ovcdr/{job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/ovcdr/{job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          12/28/2016       Initial version
--#
--#
--#####################################################################################################################

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled=false;

INSERT OVERWRITE TABLE 
   ${hivevar:work_database}.${hivevar:table_prefix}${hivevar:work_table}  
SELECT
   TELEPHONE_NUMBER
   ,PRODUCT_NAME
   ,PLAN_NUMBER
   ,SECONDS_OF_USE
   ,NUMBER_OF_CALLS
   ,BATCH_NBR
   ,ATTEMPT_COUNT
   ,(CASE WHEN (AGG.CORP = -1 ) AND (BHV.PHONE_NBR IS NOT NULL) THEN COALESCE(BHV.CORP,-1) ELSE COALESCE(AGG.CORP,-1) END) AS CORP
   ,(CASE WHEN (TRIM(AGG.HOUSE) = '-1') AND (BHV.PHONE_NBR IS NOT NULL) THEN COALESCE(BHV.HOUSE,'-1') ELSE COALESCE(AGG.HOUSE,'-1') END) AS HOUSE
   ,(CASE WHEN (TRIM(AGG.CUST) = '-1') AND (BHV.PHONE_NBR IS NOT NULL) THEN COALESCE(BHV.CUST,'-1') ELSE COALESCE(AGG.CUST,'-1') END) AS CUST
   ,AGG.DTM_CREATED
   ,DTM_LAST_MODIFIED
   ,FILE_NAME
   ,TERM_COUNTRY
   ,USAGE_DATE
   ,(CASE WHEN BHV.PHONE_NBR IS NOT NULL THEN 'H' ELSE NULL END) AS BHV_INDICATOR
   ,BHV.CUST_ACCT_NUMBER
   ,BHV.SOURCE_SYSTEM_ID
FROM(
   SELECT 
      INCOMING.TELEPHONE_NUMBER
      ,CASE WHEN TRIM(INCOMING.PRODUCT_NAME) IS NULL THEN 'NULL' ELSE UPPER(INCOMING.PRODUCT_NAME) END AS PRODUCT_NAME
      ,CASE WHEN TRIM(INCOMING.PLAN_NUMBER) IS NULL THEN '-1' ELSE UPPER(INCOMING.PLAN_NUMBER) END AS PLAN_NUMBER
      ,CASE WHEN INCOMING.SECONDS_OF_USE IS NULL THEN 0 ELSE INCOMING.SECONDS_OF_USE END AS SECONDS_OF_USE
      ,CASE WHEN INCOMING.NUMBER_OF_CALLS IS NULL THEN 0 ELSE INCOMING.NUMBER_OF_CALLS END AS NUMBER_OF_CALLS
      ,INCOMING.BATCH_ID AS BATCH_NBR
      ,CASE WHEN INCOMING.ATTEMPT_CNT IS NULL THEN 0 ELSE INCOMING.ATTEMPT_CNT END AS ATTEMPT_COUNT
      ,CASE WHEN WORK.CORP IS NULL THEN '-1' ELSE WORK.CORP  END AS CORP
      ,CASE WHEN WORK.HOUSE IS NULL THEN '-1' ELSE WORK.HOUSE  END AS HOUSE
      ,CASE WHEN WORK.CUST IS NULL THEN '-1' ELSE WORK.CUST END AS CUST
      ,CURRENT_TIMESTAMP AS DTM_CREATED
      ,CURRENT_TIMESTAMP AS DTM_LAST_MODIFIED
      ,'${hivevar:file_name}' AS FILE_NAME
      ,CASE WHEN TRIM(INCOMING.TERM_COUNTRY) IS NULL THEN 'NULL' ELSE INCOMING.TERM_COUNTRY END AS TERM_COUNTRY
      ,INCOMING.USAGE_DATE      
   FROM ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_table} INCOMING 
   LEFT OUTER JOIN ${hivevar:work_database}.${hivevar:table_prefix}${hivevar:work_chc_table} WORK
      ON(INCOMING.USAGE_DATE = WORK.USAGE_DATE 
      AND INCOMING.TELEPHONE_NUMBER = WORK.TELEPHONE_NUMBER)
   WHERE INCOMING.SOURCE_DATE = '${hivevar:source_date}' 
    ) AGG
LEFT OUTER JOIN ${hivevar:hive_database_name_ods_gold}.${hivevar:gold_bhv_opt_sdl} BHV
   ON AGG.TELEPHONE_NUMBER = BHV.PHONE_NBR
;
 
--##############################################################################
--#                                    End                                     #
--##############################################################################