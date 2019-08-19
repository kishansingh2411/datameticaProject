--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Deriving the gold gold_customer_master table from incoming.
--#   Author(s)   : DataMetica Team
--#   Usage       : pig -p var1=var1 -useHCatalog module-transform.pig
--#   Date        : 12/28/2015
--#   Log File    : .../log/${suite_name}/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/${suite_name}/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          12/28/2015       Initial version
--#
--#
--#####################################################################################################################

--##############################################################################
--#                           Hive Table Definition                            #
--##############################################################################

INSERT OVERWRITE TABLE processed.sdm_customer_master_hist  PARTITION(SOURCE_DATE)
SELECT LAST_CHANGE
     , SITE_ID
     , ACCT_NBR
     , CUSTOMER_KEY
     , TITLE
     , FIRST_NM
     , MIDDLE_INITIAL
     , LAST_NM
     , BILL_TO_ADDR
     , HOME_AREA_CD
     , HOME_PHONE_NBR
     , BUS_AREA_CD
     , BUS_PHONE_NBR
     , OTHER_AREA_CD
     , OTHER_PHONE
     , SSN
     , CUST_TYPE_CD
     , CUST_CATG
     , CUST_CMT
     , CUST_STS_CD
     , STS_DT
     , PRIVACY_CD
     , MTHLY_AMT
     , MERGE_PRORATE
     , SALESMAN_NBR
     , CR_RATING
     , NO_OF_MTHS_AT_CR
     , SELECT_CD_AR_RPTS
     , INSTALL_DT
     , REASON_CD
     , COMPANY_NBR
     , DIVISION_NBR
     , FRANCHISE_NBR
     , OLD_TRAN_ACCT_NBR
     , POOL
     , BANK_DRAFT
     , BILL_CD
     , RANGE_NBR
     , BILLING_RANGE_NBR
     , CUST_NM
     , OFFICE_ONLY_DISC
     , MTHLY_RATE_BILLED
     , CONNECT_DT
     , PIN_NBR
     , HOUSE_NBR
     , HOUSE_RESIDENT_NBR
     , VIP_CD
     , HOME_EXCHG_NBR
     , BUS_EXCHG_NBR
     , OTHER_EXCHG_NBR
     , CUST_NAME_FORMAT
     , TITLE_OF_LINEAGE
     , EXTERNAL_CR_SCORE_DTE
     , OLD_TRAN_SITE_ID
     , PIN_ENTRY_DT
     , VIDEO_RATING_CD
     , CUST_BUS_NAME
     , EXTERNAL_CR_SCORE
     , TAX_EXEMPT_CD
     , LANGUAGE_CD
     , E_CARE_CUST
     , CREATED
     , UPDATED
     , HOUSE_MASTER_KEY
     , SOURCE_DATE
FROM incoming.sdm_customer_master
;

--##############################################################################
--#                                    End                                     #
--##############################################################################