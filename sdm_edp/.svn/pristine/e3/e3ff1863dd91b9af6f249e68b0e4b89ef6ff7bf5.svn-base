--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the gold gold_customer_services table from incoming.
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 01/22/2017
--#   Log File    : .../log/sdm/${job_name}.log
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
     , NVL(SVC_CTGY_CD,'null')
     , SERVICE_OCCURRENCE
     , NVL(SERVICE_CD,'null')
     , SERVICE_QTY
     , SERVICE_QTY_FREE
     , SERVICE_RATE
     , INSTALL_DT
     , LAST_CHANGE_DT
     , START_BILLING_DT
     , NVL(CTGY_CD,'null')
     , WEIGHT
     , NVL(DISCOUNT_ACTV,'null')
     , SPREAD_CHARGED_TO_DT
     , SPREAD_TOTAL_DOLLARS
     , SPREAD_TOTAL_MONTHS
     , SPREAD_MONTHS_CHARGED
     , NVL(SPREAD_CHG,'null')
     , NVL(SPREAD_TAX_CMP,'null')
     , NVL(BYPASS_MATRIX,'null')
     , NVL(SERVICE_STATUS,'null')
     , NVL(SERVICE_TYPE,'null')
     , PRI_MINOR_ACCT_NBR
     , BILL_THRU_DT
     , NVL(CHECKED_IN,'null')
     , NVL(SOFT_DISCONNECT,'null')
     , NVL(BILLABLE_OVERRIDE,'null')
     , NVL(PRIMARY_OR_ADDL,'null')
     , ALA_CARTE_RATE
     , NVL(STOP_BILLING,'null')
     , PREV_BILLING_RATE
     , COMPANY_NBR
     , DIVISION_NBR
     , FRANCHISE_NBR
     , NVL(BILL_TYPE_CD,'null')
     , BILL_WITH_ACCT_NBR
     , BILL_WITH_STMT_CD
     , AR_CD
     , NVL(BILLING_FREQ_CD,'null')
     , CUSTOMER_KEY
     , PRICE_PROTECTION_END_DT
     , P_YYYYMMDD
FROM ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_table}
WHERE P_YYYYMMDD='${hivevar:source_date}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################