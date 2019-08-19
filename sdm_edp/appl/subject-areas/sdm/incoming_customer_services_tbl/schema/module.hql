--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create customer_services table at Incoming layer
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 01/22/2017
--#   Log File    : .../log/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/${job_name}.log
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

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
  SITE_ID                 INT                     COMMENT '',
  ACCT_NBR                BIGINT                  COMMENT '',
  SVC_CTGY_CD             STRING                  COMMENT '',
  SERVICE_OCCURRENCE      INT                     COMMENT '',
  SERVICE_CD              STRING                  COMMENT '',
  SERVICE_QTY             INT                     COMMENT '',
  SERVICE_QTY_FREE        INT                     COMMENT '',
  SERVICE_RATE            DECIMAL(7,2)            COMMENT '',
  INSTALL_DT              INT                     COMMENT '',
  LAST_CHANGE_DT          INT                     COMMENT '',
  START_BILLING_DT        INT                     COMMENT '',
  CTGY_CD                 STRING                  COMMENT '',
  WEIGHT                  INT                     COMMENT '',
  DISCOUNT_ACTV           STRING                  COMMENT '',
  SPREAD_CHARGED_TO_DT    DECIMAL(7,2)            COMMENT '',
  SPREAD_TOTAL_DOLLARS    DECIMAL(7,2)            COMMENT '',
  SPREAD_TOTAL_MONTHS     INT                     COMMENT '',
  SPREAD_MONTHS_CHARGED   INT                     COMMENT '',
  SPREAD_CHG              STRING                  COMMENT '',
  SPREAD_TAX_CMP          STRING                  COMMENT '',
  BYPASS_MATRIX           STRING                  COMMENT '',
  SERVICE_STATUS          STRING                  COMMENT '',
  SERVICE_TYPE            STRING                  COMMENT '',
  PRI_MINOR_ACCT_NBR      INT                     COMMENT '',
  BILL_THRU_DT            INT                     COMMENT '',
  CHECKED_IN              STRING                  COMMENT '',
  SOFT_DISCONNECT         STRING                  COMMENT '',
  BILLABLE_OVERRIDE       STRING                  COMMENT '',
  PRIMARY_OR_ADDL         STRING                  COMMENT '',
  ALA_CARTE_RATE          DECIMAL(7,2)            COMMENT '',
  STOP_BILLING            STRING                  COMMENT '',
  PREV_BILLING_RATE       DECIMAL(7,2)            COMMENT '',
  COMPANY_NBR             INT                     COMMENT '',
  DIVISION_NBR            INT                     COMMENT '',
  FRANCHISE_NBR           INT                     COMMENT '',
  BILL_TYPE_CD            STRING                  COMMENT '',
  BILL_WITH_ACCT_NBR      BIGINT                  COMMENT '',
  BILL_WITH_STMT_CD       INT                     COMMENT '',
  AR_CD                   INT                     COMMENT '',
  BILLING_FREQ_CD         STRING                  COMMENT '',
  CUSTOMER_KEY            BIGINT                  COMMENT '', 
  PRICE_PROTECTION_END_DT INT                     COMMENT ''
)
PARTITIONED BY (P_YYYYMMDD STRING)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################