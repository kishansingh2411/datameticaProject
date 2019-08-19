--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build work bhv_acct_phone_nbr_sdl_tmp table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 03/01/2017
--#   Log File    : .../log/customer/
--#   SQL File    : 
--#   Error File  : .../log/customer/
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          03/01/2017       Initial version
--#
--#
--#####################################################################################################################

set hive.vectorized.execution.enabled=false;

INSERT OVERWRITE 
   TABLE ${hivevar:work_database}.${hivevar:work_bhv_acct_phone_nbr_sdl_tmp_tbl} 
SELECT
   *
   ,CASE WHEN GOLD_ACCT_NBR IS NULL AND GOLD_PHONE_NBR IS NULL AND GOLD_DTM_EFFTV IS NULL THEN 'T' ELSE 'F' END AS NEW_FLAG
   ,CASE WHEN ( GOLD_ACCT_NBR IS NOT NULL AND GOLD_PHONE_NBR IS NOT NULL AND GOLD_DTM_EFFTV IS NOT NULL ) 
      AND (SITE_ID_FLAG = 'T' OR AREA_CODE_FLAG = 'T' OR PHONE_EXCHANGE_FLAG = 'T' 
         OR EXCHANGE_EXTENTION_FLAG = 'T' OR CUST_PHONE_STS_FLAG = 'T' OR CUSTOMER_TN_TYPE_FLAG = 'T' OR SRVC_CLASS_FLAG = 'T' 
   OR DTM_EXPIRED_FLAG = 'T' OR CUST_TEL_INSTALL_DT_FLAG = 'T' OR CUST_TEL_DISC_DT_FLAG = 'T'
         OR CUST_TEL_INSTALL_TM_FLAG = 'T' OR CUST_TEL_DISC_TM_FLAG = 'T' OR DTM_TEL_INSTALL_FLAG = 'T' 
   OR DTM_TEL_DISCONNECT_FLAG = 'T') THEN 'T' ELSE 'F' END AS CHANGED_FLAG 
FROM
   (
   SELECT
      GOLD.BHV_ACCT_PHONE_NBR_SDL_ID AS GOLD_BHV_ACCT_PHONE_NBR_SDL_ID
      ,WORK.BHV_ACCT_PHONE_NBR_SDL_ID AS WORK_BHV_ACCT_PHONE_NBR_SDL_ID
      ,GOLD.ACCT_NBR AS GOLD_ACCT_NBR
      ,WORK.ACCT_NBR AS WORK_ACCT_NBR
      ,GOLD.PHONE_NBR AS GOLD_PHONE_NBR
      ,WORK.PHONE_NBR AS WORK_PHONE_NBR
      ,GOLD.DTM_EFFTV AS GOLD_DTM_EFFTV
      ,WORK.DTM_EFFTV AS WORK_DTM_EFFTV
      ,GOLD.SITE_ID AS GOLD_SITE_ID
      ,WORK.SITE_ID AS WORK_SITE_ID
      ,CASE WHEN COALESCE(GOLD.SITE_ID,-1) = COALESCE(WORK.SITE_ID,-1) THEN 'F' ELSE 'T' END AS SITE_ID_FLAG
      ,GOLD.AREA_CODE AS GOLD_AREA_CODE
      ,WORK.AREA_CODE AS WORK_AREA_CODE
      ,CASE WHEN COALESCE(TRIM(GOLD.AREA_CODE),'') = COALESCE(TRIM(WORK.AREA_CODE),'') THEN 'F' ELSE 'T' END AS AREA_CODE_FLAG
      ,GOLD.PHONE_EXCHANGE AS GOLD_PHONE_EXCHANGE
      ,WORK.PHONE_EXCHANGE AS WORK_PHONE_EXCHANGE
      ,CASE WHEN COALESCE(TRIM(GOLD.PHONE_EXCHANGE),'') = COALESCE(TRIM(WORK.PHONE_EXCHANGE),'') THEN 'F' ELSE 'T' END AS PHONE_EXCHANGE_FLAG
      ,GOLD.EXCHANGE_EXTENTION AS GOLD_EXCHANGE_EXTENTION
      ,WORK.EXCHANGE_EXTENTION AS WORK_EXCHANGE_EXTENTION
      ,CASE WHEN COALESCE(TRIM(GOLD.EXCHANGE_EXTENTION),'') = COALESCE(TRIM(WORK.EXCHANGE_EXTENTION),'') THEN 'F' ELSE 'T' END AS EXCHANGE_EXTENTION_FLAG
      ,GOLD.CUST_PHONE_STS AS GOLD_CUST_PHONE_STS
      ,WORK.CUST_PHONE_STS AS WORK_CUST_PHONE_STS
      ,CASE WHEN COALESCE(TRIM(GOLD.CUST_PHONE_STS),'') = COALESCE(TRIM(WORK.CUST_PHONE_STS),'') THEN 'F' ELSE 'T' END AS CUST_PHONE_STS_FLAG
      ,GOLD.CUSTOMER_TN_TYPE AS GOLD_CUSTOMER_TN_TYPE
      ,WORK.CUSTOMER_TN_TYPE AS WORK_CUSTOMER_TN_TYPE
      ,CASE WHEN COALESCE(TRIM(GOLD.CUSTOMER_TN_TYPE),'') = COALESCE(TRIM(WORK.CUSTOMER_TN_TYPE),'') THEN 'F' ELSE 'T' END AS CUSTOMER_TN_TYPE_FLAG
      ,GOLD.SRVC_CLASS AS GOLD_SRVC_CLASS
      ,WORK.SRVC_CLASS AS WORK_SRVC_CLASS
      ,CASE WHEN COALESCE(TRIM(GOLD.SRVC_CLASS),'') = COALESCE(TRIM(WORK.SRVC_CLASS),'') THEN 'F' ELSE 'T' END AS SRVC_CLASS_FLAG
      ,GOLD.DTM_EXPIRED AS GOLD_DTM_EXPIRED
      ,WORK.DTM_EXPIRED AS WORK_DTM_EXPIRED
      ,CASE WHEN COALESCE(GOLD.DTM_EXPIRED,CAST('${hive_default_coalesce_value}' AS TIMESTAMP)) = COALESCE(WORK.DTM_EXPIRED,CAST('${hive_default_coalesce_value}' AS TIMESTAMP)) THEN 'F' ELSE 'T' END AS DTM_EXPIRED_FLAG
      ,GOLD.CUST_TEL_INSTALL_DT AS GOLD_CUST_TEL_INSTALL_DT
      ,WORK.CUST_TEL_INSTALL_DT AS WORK_CUST_TEL_INSTALL_DT
      ,CASE WHEN COALESCE(GOLD.CUST_TEL_INSTALL_DT,-1) = COALESCE(WORK.CUST_TEL_INSTALL_DT,-1) THEN 'F' ELSE 'T' END AS CUST_TEL_INSTALL_DT_FLAG
      ,GOLD.CUST_TEL_DISC_DT AS GOLD_CUST_TEL_DISC_DT
      ,WORK.CUST_TEL_DISC_DT AS WORK_CUST_TEL_DISC_DT
      ,CASE WHEN COALESCE(GOLD.CUST_TEL_DISC_DT,-1) = COALESCE(WORK.CUST_TEL_DISC_DT,-1) THEN 'F' ELSE 'T' END AS CUST_TEL_DISC_DT_FLAG
      ,GOLD.CUST_TEL_INSTALL_TM AS GOLD_CUST_TEL_INSTALL_TM
      ,WORK.CUST_TEL_INSTALL_TM AS WORK_CUST_TEL_INSTALL_TM
      ,CASE WHEN COALESCE(GOLD.CUST_TEL_INSTALL_TM,-1) = COALESCE(WORK.CUST_TEL_INSTALL_TM,-1) THEN 'F' ELSE 'T' END AS CUST_TEL_INSTALL_TM_FLAG
      ,GOLD.CUST_TEL_DISC_TM AS GOLD_CUST_TEL_DISC_TM
      ,WORK.CUST_TEL_DISC_TM AS WORK_CUST_TEL_DISC_TM
      ,CASE WHEN COALESCE(GOLD.CUST_TEL_DISC_TM,-1) = COALESCE(WORK.CUST_TEL_DISC_TM,-1) THEN 'F' ELSE 'T' END AS CUST_TEL_DISC_TM_FLAG
      ,GOLD.DTM_TEL_INSTALL AS GOLD_DTM_TEL_INSTALL
      ,WORK.DTM_TEL_INSTALL AS WORK_DTM_TEL_INSTALL
      ,CASE WHEN COALESCE(GOLD.DTM_TEL_INSTALL,CAST('${hive_default_coalesce_value}' AS TIMESTAMP)) = COALESCE(WORK.DTM_TEL_INSTALL,CAST('${hive_default_coalesce_value}' AS TIMESTAMP)) THEN 'F' ELSE 'T' END AS DTM_TEL_INSTALL_FLAG
      ,GOLD.DTM_TEL_DISCONNECT AS GOLD_DTM_TEL_DISCONNECT
      ,WORK.DTM_TEL_DISCONNECT AS WORK_DTM_TEL_DISCONNECT
      ,CASE WHEN COALESCE(GOLD.DTM_TEL_DISCONNECT,CAST('${hive_default_coalesce_value}' AS TIMESTAMP)) = COALESCE(WORK.DTM_TEL_DISCONNECT,CAST('${hive_default_coalesce_value}' AS TIMESTAMP)) THEN 'F' ELSE 'T' END AS DTM_TEL_DISCONNECT_FLAG
      ,COALESCE(GOLD.DTM_CREATED,CURRENT_TIMESTAMP) AS DTM_CREATED
      ,COALESCE(GOLD.DTM_LAST_MODIFIED,CURRENT_TIMESTAMP) AS DTM_LAST_MODIFIED
   FROM ${hivevar:work_database}.${hivevar:work_bhv_acct_phone_nbr_sdl_tbl} WORK
   LEFT OUTER JOIN ${hivevar:gold_database}.${hivevar:gold_bhv_acct_phone_nbr_sdl_tbl} GOLD
      ON WORK.ACCT_NBR = GOLD.ACCT_NBR
      AND WORK.PHONE_NBR = GOLD.PHONE_NBR
      AND WORK.DTM_EFFTV = GOLD.DTM_EFFTV
) TEMP
;

--##############################################################################
--#                                    End                                     #
--##############################################################################