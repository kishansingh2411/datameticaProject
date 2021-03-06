--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Work table from GOLD table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
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

set hive.exec.dynamic.partition.mode=nonstrict
;

set hive.mapred.supports.subdirectories=true
;

set hive.vectorized.execution.enabled=false
;

WITH TEMP AS (
SELECT
   SRC_GOLD.USAGE_DATE,
   SRC_GOLD.TELEPHONE_NUMBER,
   SRC_GOLD.CALL_TYPE,
   SRC_GOLD.SECONDS_OF_USE,
   SRC_GOLD.NUMBER_OF_CALLS,
   SRC_GOLD.BATCH_NBR,
   SRC_GOLD.ATTEMPT_COUNT,
   SRC_WORK.CORP,
   SRC_WORK.HOUSE,
   SRC_WORK.CUST,
   SRC_GOLD.DTM_CREATED,
   SRC_GOLD.DTM_LAST_MODIFIED,
   SRC_GOLD.FILE_NAME,
   SRC_GOLD.BHV_INDICATOR,
   SRC_GOLD.CUST_ACCT_NUMBER,
   SRC_GOLD.SOURCE_SYSTEM_ID,
   CASE 
      WHEN SRC_WORK.CORP = SRC_GOLD.CORP THEN 0
      ELSE 1
   END AS CORP_IND,
   CASE 
      WHEN TRIM(SRC_WORK.HOUSE) = TRIM(SRC_GOLD.HOUSE) THEN 0
      ELSE 1
   END AS HOUSE_IND,
   CASE 
      WHEN TRIM(SRC_WORK.CUST) = TRIM(SRC_GOLD.CUST) THEN 0
      ELSE 1
   END AS CUST_IND
FROM
   ${hivevar:work_database}.${hivevar:work_chc_table} SRC_WORK
LEFT OUTER JOIN ${hivevar:gold_database}.${hivevar:gold_table} SRC_GOLD
      ON ( SRC_GOLD.USAGE_DATE = SRC_WORK.USAGE_DATE
           AND SRC_GOLD.TELEPHONE_NUMBER = SRC_WORK.TELEPHONE_NUMBER)
WHERE
   TO_DATE(SRC_GOLD.DTM_CREATED) >= TO_DATE(DATE_SUB('${hivevar:source_date}',${param_numeric})) 
   AND SRC_GOLD.CORP = '-1'
),
TEMP1 AS (
   SELECT
      TELEPHONE_NUMBER,
      CALL_TYPE,
      SECONDS_OF_USE,
      NUMBER_OF_CALLS,
      BATCH_NBR,
      ATTEMPT_COUNT,
      CAST(CORP AS INT) AS CORP,
      HOUSE,
      CUST,
      DTM_CREATED,
      CURRENT_TIMESTAMP AS DTM_LAST_MODIFIED,
      FILE_NAME,
	  BHV_INDICATOR,
      CUST_ACCT_NUMBER,
      SOURCE_SYSTEM_ID,
      USAGE_DATE
   FROM TEMP
   WHERE 
      CORP_IND = 1
      OR HOUSE_IND = 1
      OR CUST_IND = 1
),
GOLD_UPDATE AS (
   SELECT 
      SRC_GOLD_1.TELEPHONE_NUMBER,
      SRC_GOLD_1.CALL_TYPE,
      SRC_GOLD_1.SECONDS_OF_USE,
      SRC_GOLD_1.NUMBER_OF_CALLS,
      SRC_GOLD_1.BATCH_NBR,
      SRC_GOLD_1.ATTEMPT_COUNT,
      CASE 
         WHEN SRC_WORK_1.CORP IS NOT NULL THEN SRC_WORK_1.CORP
         ELSE SRC_GOLD_1.CORP
      END AS CORP,
      CASE 
         WHEN SRC_WORK_1.HOUSE IS NOT NULL THEN SRC_WORK_1.HOUSE
         ELSE SRC_GOLD_1.HOUSE
      END AS HOUSE,
      CASE 
         WHEN SRC_WORK_1.CUST IS NOT NULL THEN SRC_WORK_1.CUST
         ELSE SRC_GOLD_1.CUST
      END AS CUST,
      SRC_GOLD_1.DTM_CREATED,
      CASE 
         WHEN SRC_WORK_1.DTM_LAST_MODIFIED IS NOT NULL THEN SRC_WORK_1.DTM_LAST_MODIFIED
         ELSE SRC_GOLD_1.DTM_LAST_MODIFIED
      END AS DTM_LAST_MODIFIED,
      SRC_GOLD_1.FILE_NAME,
	  SRC_GOLD_1.BHV_INDICATOR,
      SRC_GOLD_1.CUST_ACCT_NUMBER,
      SRC_GOLD_1.SOURCE_SYSTEM_ID,
      SRC_GOLD_1.USAGE_DATE
   FROM ${hivevar:gold_database}.${hivevar:gold_table} SRC_GOLD_1
   LEFT OUTER JOIN TEMP1 SRC_WORK_1
   ON (
      SRC_GOLD_1.TELEPHONE_NUMBER = SRC_WORK_1.TELEPHONE_NUMBER 
      AND SRC_GOLD_1.USAGE_DATE = SRC_WORK_1.USAGE_DATE
      AND SRC_GOLD_1.CALL_TYPE = SRC_WORK_1.CALL_TYPE
      AND SRC_GOLD_1.BATCH_NBR = SRC_WORK_1.BATCH_NBR
      AND SRC_GOLD_1.ATTEMPT_COUNT = SRC_WORK_1.ATTEMPT_COUNT
      )
   WHERE
      TO_DATE(SRC_GOLD_1.DTM_CREATED) >= TO_DATE(DATE_SUB('${hivevar:source_date}',${param_numeric}))
),
GOLD_UPDATE_1 AS (
   SELECT
      TELEPHONE_NUMBER,
      CALL_TYPE,
      SECONDS_OF_USE,
      NUMBER_OF_CALLS,
      BATCH_NBR,
      ATTEMPT_COUNT,
      (CASE WHEN (AGG.CORP IS NULL OR AGG.CORP = -1 ) AND (BHV.PHONE_NBR IS NOT NULL) AND FROM_UNIXTIME(UNIX_TIMESTAMP(USAGE_DATE,'yyyyMMdd'),'yyyy-MM-dd') >= To_DATE(BHV.DTM_EFFTV) AND FROM_UNIXTIME(UNIX_TIMESTAMP(USAGE_DATE,'yyyyMMdd'),'yyyy-MM-dd') < TO_DATE(BHV.DTM_EXPIRED) THEN COALESCE(BHV.CORP,-1) ELSE COALESCE(AGG.CORP,-1) END) AS CORP,
      (CASE WHEN (AGG.HOUSE IS NULL OR TRIM(AGG.HOUSE) = '-1') AND (BHV.PHONE_NBR IS NOT NULL) AND FROM_UNIXTIME(UNIX_TIMESTAMP(USAGE_DATE,'yyyyMMdd'),'yyyy-MM-dd') >= To_DATE(BHV.DTM_EFFTV) AND FROM_UNIXTIME(UNIX_TIMESTAMP(USAGE_DATE,'yyyyMMdd'),'yyyy-MM-dd') < TO_DATE(BHV.DTM_EXPIRED) THEN COALESCE(BHV.HOUSE,'-1') ELSE COALESCE(AGG.HOUSE,'-1') END) AS HOUSE,
      (CASE WHEN (AGG.CUST IS NULL OR TRIM(AGG.CUST) = '-1') AND (BHV.PHONE_NBR IS NOT NULL) AND FROM_UNIXTIME(UNIX_TIMESTAMP(USAGE_DATE,'yyyyMMdd'),'yyyy-MM-dd') >= To_DATE(BHV.DTM_EFFTV) AND FROM_UNIXTIME(UNIX_TIMESTAMP(USAGE_DATE,'yyyyMMdd'),'yyyy-MM-dd') < TO_DATE(BHV.DTM_EXPIRED) THEN COALESCE(BHV.CUST,'-1') ELSE COALESCE(AGG.CUST,'-1') END) AS CUST,
      AGG.DTM_CREATED,
      DTM_LAST_MODIFIED,
      FILE_NAME,
      COALESCE(AGG.BHV_INDICATOR,(CASE WHEN BHV.PHONE_NBR IS NOT NULL AND FROM_UNIXTIME(UNIX_TIMESTAMP(USAGE_DATE,'yyyyMMdd'),'yyyy-MM-dd') >= To_DATE(BHV.DTM_EFFTV) AND FROM_UNIXTIME(UNIX_TIMESTAMP(USAGE_DATE,'yyyyMMdd'),'yyyy-MM-dd') < TO_DATE(BHV.DTM_EXPIRED) THEN 'H' ELSE NULL END)) AS BHV_INDICATOR,
      COALESCE(AGG.CUST_ACCT_NUMBER,(CASE WHEN BHV.PHONE_NBR IS NOT NULL AND FROM_UNIXTIME(UNIX_TIMESTAMP(USAGE_DATE,'yyyyMMdd'),'yyyy-MM-dd') >= To_DATE(BHV.DTM_EFFTV) AND FROM_UNIXTIME(UNIX_TIMESTAMP(USAGE_DATE,'yyyyMMdd'),'yyyy-MM-dd') < TO_DATE(BHV.DTM_EXPIRED) THEN BHV.CUST_ACCT_NUMBER ELSE NULL END)) AS CUST_ACCT_NUMBER,
      COALESCE(AGG.SOURCE_SYSTEM_ID,(CASE WHEN BHV.PHONE_NBR IS NOT NULL AND FROM_UNIXTIME(UNIX_TIMESTAMP(USAGE_DATE,'yyyyMMdd'),'yyyy-MM-dd') >= To_DATE(BHV.DTM_EFFTV) AND FROM_UNIXTIME(UNIX_TIMESTAMP(USAGE_DATE,'yyyyMMdd'),'yyyy-MM-dd') < TO_DATE(BHV.DTM_EXPIRED) THEN BHV.SOURCE_SYSTEM_ID ELSE NULL END)) AS SOURCE_SYSTEM_ID,
      USAGE_DATE
   FROM GOLD_UPDATE AGG
   LEFT OUTER JOIN ${hivevar:hive_database_name_ods_gold}.${hivevar:gold_bhv_opt_sdl} BHV
   ON AGG.TELEPHONE_NUMBER = BHV.PHONE_NBR
)
FROM GOLD_UPDATE_1
INSERT OVERWRITE TABLE
   ${hivevar:work_database}.${hivevar:work_table}
SELECT 
   TELEPHONE_NUMBER
   ,CALL_TYPE
   ,SECONDS_OF_USE
   ,NUMBER_OF_CALLS
   ,BATCH_NBR
   ,ATTEMPT_COUNT
   ,CORP
   ,HOUSE
   ,CUST
   ,DTM_CREATED
   ,DTM_LAST_MODIFIED
   ,FILE_NAME
   ,USAGE_DATE
   ,BHV_INDICATOR
   ,CUST_ACCT_NUMBER
   ,SOURCE_SYSTEM_ID
;

--##############################################################################
--#                                    End                                     #
--##############################################################################