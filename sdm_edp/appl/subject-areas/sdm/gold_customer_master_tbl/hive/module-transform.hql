--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the gold gold_customer_master table from incoming.
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 01/22/2017
--#   SQL File    : 
--#   Error File  : .../log/sdm/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          01/22/2017       Initial version
--#
--#
--#####################################################################################################################

--##############################################################################
--#                           Hive Table Definition                            #
--##############################################################################



INSERT OVERWRITE TABLE ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_table}
 PARTITION(P_YYYYMMDD)
SELECT 
     SITE_ID
     , ACCT_NBR
     , CUSTOMER_KEY
     , NVL(TITLE,'null')
     , NVL(FIRST_NM,'null')
     , NVL(MIDDLE_INITIAL,'null')
     , NVL(LAST_NM,'null')
     , NVL(BILL_TO_ADDR,'null')
     , HOME_AREA_CD
     , HOME_PHONE_NBR
     , BUS_AREA_CD
     , BUS_PHONE_NBR
     , OTHER_AREA_CD
     , OTHER_PHONE
     , NVL(SSN,'null')
     , NVL(CUST_TYPE_CD,'null')
     , NVL(CUST_CATG,'null')
     , NVL(CUST_CMT,'null')
     , NVL(CUST_STS_CD,'null')
     , STS_DT
     , PRIVACY_CD
     , MTHLY_AMT
     , NVL(MERGE_PRORATE,'null')
     , SALESMAN_NBR
     , NVL(CR_RATING,'null')
     , NO_OF_MTHS_AT_CR
     , NVL(SELECT_CD_AR_RPTS,'null')
     , INSTALL_DT
     , NVL(REASON_CD,'null')
     , COMPANY_NBR
     , DIVISION_NBR
     , FRANCHISE_NBR
     , OLD_TRAN_ACCT_NBR
     , NVL(POOL,'null')
     , NVL(BANK_DRAFT,'null')
     , NVL(BILL_CD,'null')
     , RANGE_NBR
     , BILLING_RANGE_NBR
     , NVL(CUST_NM,'null')
     , NVL(OFFICE_ONLY_DISC,'null')
     , MTHLY_RATE_BILLED
     , CONNECT_DT
     , NVL(PIN_NBR,'null')
     , HOUSE_NBR
     , HOUSE_RESIDENT_NBR
     , NVL(VIP_CD,'null')
     , HOME_EXCHG_NBR
     , BUS_EXCHG_NBR
     , OTHER_EXCHG_NBR
     , NVL(CUST_NAME_FORMAT,'null')
     , NVL(TITLE_OF_LINEAGE,'null')
     , EXTERNAL_CR_SCORE_DTE
     , OLD_TRAN_SITE_ID
     , PIN_ENTRY_DT
     , NVL(VIDEO_RATING_CD,'null')
     , NVL(CUST_BUS_NAME,'null')
     , NVL(EXTERNAL_CR_SCORE,'null')
     , NVL(TAX_EXEMPT_CD,'null')
     , NVL(LANGUAGE_CD,'null')
     , NVL(E_CARE_CUST,'null')
     , CREATED
     , UPDATED
     , HOUSE_MASTER_KEY
     , P_YYYYMMDD
FROM ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_table}
WHERE P_YYYYMMDD='${hivevar:source_date}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################