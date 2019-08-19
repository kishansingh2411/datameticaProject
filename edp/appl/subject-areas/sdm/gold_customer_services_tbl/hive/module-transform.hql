--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Deriving the gold gold_customer_services table from incoming.
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

INSERT OVERWRITE TABLE processed.sdm_customer_services_hist  PARTITION(SOURCE_DATE)
SELECT LAST_CHANGE
     , SITE_ID
     , ACCT_NBR
     , SVC_CTGY_CD
     , SERVICE_OCCURRENCE
     , SERVICE_CD
     , SERVICE_QTY
     , SERVICE_QTY_FREE
     , SERVICE_RATE
     , INSTALL_DT
     , LAST_CHANGE_DT
     , START_BILLING_DT
     , CTGY_CD
     , WEIGHT
     , DISCOUNT_ACTV
     , SPREAD_CHARGED_TO_DT
     , SPREAD_TOTAL_DOLLARS
     , SPREAD_TOTAL_MONTHS
     , SPREAD_MONTHS_CHARGED
     , SPREAD_CHG
     , SPREAD_TAX_CMP
     , BYPASS_MATRIX
     , SERVICE_STATUS
     , SERVICE_TYPE
     , PRI_MINOR_ACCT_NBR
     , BILL_THRU_DT
     , CHECKED_IN
     , SOFT_DISCONNECT
     , BILLABLE_OVERRIDE
     , PRIMARY_OR_ADDL
     , ALA_CARTE_RATE
     , STOP_BILLING
     , PREV_BILLING_RATE
     , COMPANY_NBR
     , DIVISION_NBR
     , FRANCHISE_NBR
     , BILL_TYPE_CD
     , BILL_WITH_ACCT_NBR
     , BILL_WITH_STMT_CD
     , AR_CD
     , BILLING_FREQ_CD
     , CUSTOMER_KEY
     , PRICE_PROTECTION_END_DT
     , SOURCE_DATE
FROM incoming.sdm_customer_services
;

--##############################################################################
--#                                    End                                     #
--##############################################################################