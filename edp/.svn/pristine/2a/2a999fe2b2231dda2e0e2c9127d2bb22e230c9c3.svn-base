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

WITH 
INCOMING_AGG AS(
   SELECT
      DISTINCT
      USAGE_DATE
     ,TELEPHONE_NUMBER
   FROM ${hive_database_name_incoming}.${incoming_agg_data}
   WHERE
      SOURCE_DATE = '${source_date}'
),
GOLD_AGG AS(
   SELECT
      DISTINCT
      USAGE_DATE
     ,TELEPHONE_NUMBER
   FROM ${hive_database_name_gold}.${gold_agg_data}
   WHERE 
      TO_DATE(DTM_CREATED) >= TO_DATE(DATE_SUB('${source_date}',${param_numeric}))
      AND CORP = -1
),
AGG_DATA AS(
   SELECT
      USAGE_DATE
     ,TELEPHONE_NUMBER
   FROM INCOMING_AGG
   UNION
   SELECT
      USAGE_DATE
      ,TELEPHONE_NUMBER
   FROM GOLD_AGG
),
OV_TELEPHONE AS(
   SELECT
      TMP.CORP
      ,TMP.CUSTOMER_ACCOUNT_ID
      ,TMP.TELEPHONE_NUMBER
   FROM
   (
      SELECT
         CORP AS CORP
         ,CUSTOMER_ACCOUNT_ID
         ,DTM_LAST_MODIFIED
         ,TELEPHONE_NUMBER
         ,TO_DATE(DTM_EFFTV) AS DTM_EFFTV
         ,TO_DATE(DTM_EXPIRED) AS DTM_EXPIRED
         ,ROW_NUMBER() OVER (PARTITION BY TELEPHONE_NUMBER ORDER BY DTM_EFFTV DESC, DTM_EXPIRED DESC, DTM_LAST_MODIFIED DESC) AS RNK
      FROM ${hive_database_name_ods_incoming}.${ov_telephone_number}
      WHERE
         TO_DATE(DTM_EFFTV) <= TO_DATE('${source_date}')
         AND TO_DATE(DTM_EXPIRED) > TO_DATE('${source_date}')
   )TMP
   WHERE 
      TMP.RNK = 1
),
CUSTOMER_ACCOUNT AS(
   SELECT
      DWELLING_NBR AS COCT_DWELLING_NBR
      ,CUSTOMER_ACCOUNT_ID
      ,CUST AS COCT_CUST
   FROM ${hive_database_name_ods_incoming}.${customer_account}
),
IP_DID AS(
   SELECT
      IPDD_CORP
      ,IPDD_DWELLING_NBR
      ,IPDD_CUST
      ,TELEPHONE_NUMBER
   FROM(
      SELECT
         IPDD_CORP
        ,IPDD_DWELLING_NBR
        ,IPDD_CUST
        ,DTM_LAST_UPDATED
        ,TELEPHONE_NUMBER
        ,DTM_EFFTV
        ,DTM_EXPIRED
        ,ROW_NUMBER() OVER (PARTITION BY TELEPHONE_NUMBER ORDER BY DTM_EFFTV DESC, DTM_EXPIRED DESC, DTM_LAST_UPDATED DESC) AS RNK
      FROM(
         SELECT
            CORP AS IPDD_CORP
            ,DWELLING_NBR AS IPDD_DWELLING_NBR
            ,CUST AS IPDD_CUST
            ,DTM_LAST_UPDATED
            ,CONCAT(AREA_CODE,PHONE_EXCHANGE,EXCHANGE_EXTENTION) AS TELEPHONE_NUMBER
            ,TO_DATE(DTM_EFFTV) AS DTM_EFFTV
            ,TO_DATE(DTM_EXPIRED) AS DTM_EXPIRED
         FROM ${hive_database_name_ods_incoming}.${ip_sip_did_phone_nbr}
         WHERE		 
		    TO_DATE(DTM_EFFTV) <= TO_DATE('${source_date}')
            AND TO_DATE(DTM_EXPIRED) > TO_DATE('${source_date}')
        )TMP
    )TMP1
   WHERE 
      TMP1.RNK = 1
),
IP_PILOT AS(
   SELECT
      IPPT_CORP
      ,IPPT_DWELLING_NBR
      ,IPPT_CUST
      ,TELEPHONE_NUMBER
   FROM(
      SELECT
         IPPT_CORP
         ,IPPT_DWELLING_NBR
         ,IPPT_CUST
         ,DTM_LAST_UPDATED
         ,TELEPHONE_NUMBER
         ,DTM_EFFTV
         ,DTM_EXPIRED
         ,ROW_NUMBER() OVER (PARTITION BY TELEPHONE_NUMBER ORDER BY DTM_EFFTV DESC, DTM_EXPIRED DESC, DTM_LAST_UPDATED DESC) AS RNK
      FROM(
         SELECT
            CORP AS IPPT_CORP
            ,DWELLING_NBR AS IPPT_DWELLING_NBR
            ,CUST AS IPPT_CUST
            ,DTM_LAST_UPDATED
            ,CONCAT(AREA_CODE,PHONE_EXCHANGE,EXCHANGE_EXTENTION) AS TELEPHONE_NUMBER
            ,TO_DATE(DTM_EFFTV) AS DTM_EFFTV
            ,TO_DATE(DTM_EXPIRED) AS DTM_EXPIRED
         FROM ${hive_database_name_ods_incoming}.${ip_sip_pilot_phone_nbr}
         WHERE		 
		    TO_DATE(DTM_EFFTV) <= TO_DATE('${source_date}')
            AND TO_DATE(DTM_EXPIRED) > TO_DATE('${source_date}')
        )TMP
    )TMP1
    WHERE 
       TMP1.RNK = 1
),
AGG_OV_JOIN AS(
   SELECT
      AGG.USAGE_DATE
      ,AGG.TELEPHONE_NUMBER
      ,OV.CORP
      ,OV.CUSTOMER_ACCOUNT_ID
      ,CASE WHEN OV.CORP IS NULL THEN 1 ELSE 0 END AS OVTN_CORP_ISNULL
   FROM AGG_DATA AGG
   LEFT OUTER JOIN OV_TELEPHONE OV
      ON AGG.TELEPHONE_NUMBER = OV.TELEPHONE_NUMBER
),
AGG_OV_CUST AS(
   SELECT
      AGG.USAGE_DATE
      ,AGG.TELEPHONE_NUMBER
      ,IF(AGG.OVTN_CORP_ISNULL = 1,-1,AGG.CORP) AS COCT_CORP
      ,CUST.COCT_DWELLING_NBR AS COCT_DWELLING_NBR
      ,CUST.COCT_CUST AS COCT_CUST
      ,AGG.OVTN_CORP_ISNULL
   FROM AGG_OV_JOIN AGG
   LEFT OUTER JOIN CUSTOMER_ACCOUNT CUST
      ON AGG.CUSTOMER_ACCOUNT_ID = CUST.CUSTOMER_ACCOUNT_ID
   WHERE 
      AGG.OVTN_CORP_ISNULL = 0
),
AGG_OV_DID AS(
   SELECT
      AGG.USAGE_DATE
      ,AGG.TELEPHONE_NUMBER
      ,DID.IPDD_CORP AS IPDD_CORP
      ,CASE WHEN DID.IPDD_CORP IS NULL THEN 1 ELSE 0 END AS IPDD_CORP_ISNULL
      ,DID.IPDD_DWELLING_NBR AS IPDD_DWELLING_NBR
      ,DID.IPDD_CUST AS IPDD_CUST   
   FROM AGG_OV_JOIN AGG
   LEFT OUTER JOIN IP_DID DID
      ON AGG.TELEPHONE_NUMBER = DID.TELEPHONE_NUMBER
   WHERE 
      AGG.OVTN_CORP_ISNULL = 1
),
AGG_OV_PILOT AS(
   SELECT
      AGG.USAGE_DATE
      ,AGG.TELEPHONE_NUMBER
      ,PILOT.IPPT_CORP AS IPPT_CORP
      ,CASE WHEN PILOT.IPPT_CORP IS NULL THEN 1 ELSE 0 END AS IPPT_CORP_ISNULL
      ,PILOT.IPPT_DWELLING_NBR AS IPPT_DWELLING_NBR
      ,PILOT.IPPT_CUST AS IPPT_CUST
   FROM AGG_OV_DID AGG
   LEFT OUTER JOIN IP_PILOT PILOT
      ON AGG.TELEPHONE_NUMBER = PILOT.TELEPHONE_NUMBER
   WHERE
      IPDD_CORP_ISNULL = 1
),
AGG_OV_DID_PILOT AS(
   SELECT 
      USAGE_DATE
      ,TELEPHONE_NUMBER
      ,IPDD_CORP AS IP_CORP
      ,IPDD_CORP_ISNULL AS IP_CORP_ISNULL
      ,IPDD_DWELLING_NBR AS IP_DWELLING_NBR
      ,IPDD_CUST AS IP_CUST
   FROM AGG_OV_DID
   WHERE 
      IPDD_CORP_ISNULL = 0
   UNION
   SELECT
      USAGE_DATE 
      ,TELEPHONE_NUMBER
      ,IPPT_CORP AS IP_CORP
      ,IPPT_CORP_ISNULL AS IP_CORP_ISNULL
      ,IPPT_DWELLING_NBR AS IP_DWELLING_NBR
      ,IPPT_CUST AS IP_CUST
   FROM AGG_OV_PILOT
),
AGG_CUST_IP_UNION AS(
   SELECT
      COALESCE(CUST.USAGE_DATE,IP.USAGE_DATE) AS USAGE_DATE
      ,COALESCE(CUST.TELEPHONE_NUMBER,IP.TELEPHONE_NUMBER) AS TELEPHONE_NUMBER
      ,IF(COALESCE(CUST.OVTN_CORP_ISNULL,1) = 1 AND COALESCE(IP_CORP_ISNULL,1) = 0,COALESCE(IP_CORP,-1),COALESCE(COCT_CORP,-1)) AS CORP
      ,IF(COALESCE(CUST.OVTN_CORP_ISNULL,1) = 1 AND COALESCE(IP_CORP_ISNULL,1) = 0,COALESCE(IP_DWELLING_NBR,'-1'),COALESCE(COCT_DWELLING_NBR,'-1')) AS HOUSE
      ,IF(COALESCE(CUST.OVTN_CORP_ISNULL,1) = 1 AND COALESCE(IP_CORP_ISNULL,1) = 0,COALESCE(IP_CUST,'-1'),COALESCE(COCT_CUST,'-1')) AS CUST
   FROM AGG_OV_CUST CUST
   FULL OUTER JOIN AGG_OV_DID_PILOT IP
      ON CUST.TELEPHONE_NUMBER = IP.TELEPHONE_NUMBER
      AND CUST.USAGE_DATE = IP.USAGE_DATE
)
FROM AGG_CUST_IP_UNION
INSERT OVERWRITE TABLE 
   ${hive_database_name_work}.${work_agg_chc}
SELECT 
   USAGE_DATE
   ,TELEPHONE_NUMBER
   ,CORP
   ,HOUSE
   ,CUST
;

--##############################################################################
--#                                    End                                     #
--##############################################################################