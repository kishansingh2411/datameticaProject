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
   ,CORP
   ,HOUSE
   ,CUST
   ,DTM_CREATED
   ,DTM_LAST_MODIFIED
   ,FILE_NAME
   ,TERM_COUNTRY
   ,USAGE_DATE
   ,BHV_INDICATOR
   ,CUST_ACCT_NUMBER
   ,SOURCE_SYSTEM_ID
FROM ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_table}
WHERE 
   USAGE_DATE IN(
      SELECT 
         DISTINCT GOLD_METADATA.USAGE_DATE 
      FROM ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_table} INCOMING
      LEFT OUTER JOIN ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_metadata_table} GOLD_METADATA   
         ON (INCOMING.BATCH_ID = GOLD_METADATA.BATCH_NBR)
      WHERE 
         GOLD_METADATA.BATCH_NBR IS NOT NULL 
         AND INCOMING.SOURCE_DATE = '${hivevar:source_date}'
    )
;

--##############################################################################
--#                                    End                                     #
--##############################################################################