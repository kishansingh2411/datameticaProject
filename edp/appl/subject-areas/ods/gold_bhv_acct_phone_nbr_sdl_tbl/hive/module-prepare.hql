--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build incoming_bhv_acct_phone_nbr_sdl table from source table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-prepare.hql
--#   Date        : 02/01/2017
--#   Log File    : .../log/ods/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/ods/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          02/01/2017       Initial version
--#
--#
--#####################################################################################################################

set hive.vectorized.execution.enabled=false;

DELETE FROM 
   ${hivevar:gold_database}.${hivevar:gold_bhv_acct_phone_nbr_sdl}
WHERE 
   BHV_ACCT_PHONE_NBR_SDL_ID IN 
   	(
      SELECT 
         DISTINCT GOLD_BHV_ACCT_PHONE_NBR_SDL_ID 
      FROM 
      	${hivevar:work_database}.${hivevar:work_bhv_acct_phone_nbr_sdl_tmp}
      WHERE CHANGED_FLAG = 'T'
      	AND TO_DATE(GOLD_DTM_EXPIRED) = '${hivevar:dtm_expired_default}'
    )
;

INSERT INTO
   TABLE ${hivevar:gold_database}.${hivevar:gold_bhv_acct_phone_nbr_sdl}
SELECT
   COALESCE(GOLD_BHV_ACCT_PHONE_NBR_SDL_ID,WORK_BHV_ACCT_PHONE_NBR_SDL_ID) AS BHV_ACCT_PHONE_NBR_SDL_ID
   ,WORK_SITE_ID AS SITE_ID
   ,COALESCE(GOLD_ACCT_NBR,WORK_ACCT_NBR) AS ACCT_NBR
   ,COALESCE(GOLD_PHONE_NBR,WORK_PHONE_NBR) AS PHONE_NBR
   ,WORK_AREA_CODE AS AREA_CODE
   ,WORK_PHONE_EXCHANGE AS PHONE_EXCHANGE
   ,WORK_EXCHANGE_EXTENTION AS EXCHANGE_EXTENTION
   ,WORK_CUST_PHONE_STS AS CUST_PHONE_STS
   ,WORK_CUSTOMER_TN_TYPE AS CUSTOMER_TN_TYPE
   ,WORK_SRVC_CLASS AS SRVC_CLASS
   ,COALESCE(GOLD_DTM_EFFTV,WORK_DTM_EFFTV) AS DTM_EFFTV
   ,WORK_DTM_EXPIRED AS DTM_EXPIRED
   ,WORK_CUST_TEL_INSTALL_DT AS CUST_TEL_INSTALL_DT
   ,WORK_CUST_TEL_DISC_DT AS CUST_TEL_DISC_DT
   ,WORK_CUST_TEL_INSTALL_TM AS CUST_TEL_INSTALL_TM
   ,WORK_CUST_TEL_DISC_TM AS CUST_TEL_DISC_TM
   ,WORK_DTM_TEL_INSTALL AS DTM_TEL_INSTALL
   ,WORK_DTM_TEL_DISCONNECT AS DTM_TEL_DISCONNECT
   ,DTM_CREATED
   ,CASE WHEN CHANGED_FLAG = 'T' THEN CURRENT_TIMESTAMP ELSE DTM_LAST_MODIFIED END AS DTM_LAST_MODIFIED
FROM
	${hivevar:work_database}.${hivevar:work_bhv_acct_phone_nbr_sdl_tmp}
WHERE 
   NEW_FLAG = 'T'
   OR (CHANGED_FLAG = 'T' AND TO_DATE(GOLD_DTM_EXPIRED) = '${hivevar:dtm_expired_default}')
;

--##############################################################################
--#                                    End                                     #
--##############################################################################