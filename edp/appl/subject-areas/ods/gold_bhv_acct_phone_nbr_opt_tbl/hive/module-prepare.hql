--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build incoming_bhv_acct_phone_nbr_opt table from source table
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

--Delete rows which are updated from target table 
DELETE FROM 
   ${hivevar:gold_database}.${hivevar:gold_bhv_acct_phone_nbr_opt}
WHERE 
   BHV_ACCT_PHONE_NBR_OPT_ID IN 
   (
      SELECT 
         DISTINCT GOLD_BHV_ACCT_PHONE_NBR_OPT_ID 
      FROM 
      	${hivevar:work_database}.${hivevar:work_bhv_acct_phone_nbr_opt_tmp}
      WHERE 
      	CHANGED_FLAG = 'T'
      	AND TO_DATE(GOLD_DTM_EXPIRED) = '${hivevar:dtm_expired_default}'
    )
;

--Insert updated and newly inserted records in target table.
INSERT INTO
   TABLE ${hivevar:gold_database}.${hivevar:gold_bhv_acct_phone_nbr_opt}
SELECT 
   COALESCE(GOLD_BHV_ACCT_PHONE_NBR_OPT_ID,WORK_BHV_ACCT_PHONE_NBR_OPT_ID) AS BHV_ACCT_PHONE_NBR_OPT_ID
   ,COALESCE(GOLD_ID_ACCOUNT_TN,WORK_ID_ACCOUNT_TN) AS ID_ACCOUNT_TN
   ,COALESCE(GOLD_ID_BHV_ACCOUNT,WORK_ID_BHV_ACCOUNT) AS ID_BHV_ACCOUNT
   ,COALESCE(GOLD_PHONE_NBR,WORK_PHONE_NBR) AS PHONE_NBR
   ,WORK_DDP_ACCOUNT AS DDP_ACCOUNT
   ,WORK_AREA_CODE AS AREA_CODE
   ,WORK_PHONE_EXCHANGE AS PHONE_EXCHANGE
   ,WORK_EXCHANGE_EXTENTION AS EXCHANGE_EXTENTION
   ,WORK_CORP AS CORP
   ,WORK_HOUSE AS HOUSE
   ,WORK_CUST AS CUST
   ,COALESCE(GOLD_DTM_EFFTV,WORK_DTM_EFFTV) AS DTM_EFFTV
   ,WORK_DTM_EXPIRED AS DTM_EXPIRED
   ,WORK_ID_SERVICE_REC AS ID_SERVICE_REC
   ,WORK_ID_CUST AS ID_CUST
   ,WORK_PPI_VENDOR_ACCOUNT_ID AS PPI_VENDOR_ACCOUNT_ID
   ,WORK_ID_IPTN AS ID_IPTN
   ,WORK_ID_TN_TYPE AS ID_TN_TYPE
   ,WORK_ID_IPTEL_ORDER AS ID_IPTEL_ORDER
   ,WORK_ID_ORDER_STATUS AS ID_ORDER_STATUS
   ,WORK_DTM_CREATED_SRC AS DTM_CREATED_SRC
   ,WORK_STATUS_DATE AS STATUS_DATE
   ,WORK_END_DATE AS END_DATE
   ,WORK_PORTED AS PORTED
   ,WORK_SWAP_FROM_ID_ACCOUNT_TN AS SWAP_FROM_ID_ACCOUNT_TN
   ,WORK_SWAP_FROM_TN AS SWAP_FROM_TN
   ,WORK_PRIOR_ID_ORDER_STATUS AS PRIOR_ID_ORDER_STATUS
   ,DTM_CREATED
   ,CASE WHEN CHANGED_FLAG = 'T' THEN CURRENT_TIMESTAMP ELSE DTM_LAST_MODIFIED END AS DTM_LAST_MODIFIED
FROM
	${hivevar:work_database}.${hivevar:work_bhv_acct_phone_nbr_opt_tmp}
WHERE 
   NEW_FLAG = 'T'
   OR (CHANGED_FLAG = 'T' AND TO_DATE(GOLD_DTM_EXPIRED) = '${hivevar:dtm_expired_default}')
;

--##############################################################################
--#                                    End                                     #
--##############################################################################