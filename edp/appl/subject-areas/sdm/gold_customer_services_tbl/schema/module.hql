--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_customer_services table at Gold layer
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 12/28/2015
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
--#    1.0     DataMetica Team          12/28/2015       Initial version
--#
--#
--#####################################################################################################################


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
      SITE_ID                 INT,
  ACCT_NBR                BIGINT,
  SVC_CTGY_CD             STRING,
  SERVICE_OCCURRENCE      INT,
  SERVICE_CD              STRING,
  SERVICE_QTY             INT,
  SERVICE_QTY_FREE        INT,
  SERVICE_RATE            DECIMAL(7,2),
  INSTALL_DT              INT,
  LAST_CHANGE_DT          INT,
  START_BILLING_DT        INT,
  CTGY_CD                 STRING,
  WEIGHT                  INT,
  DISCOUNT_ACTV           STRING,
  SPREAD_CHARGED_TO_DT    DECIMAL(7,2),
  SPREAD_TOTAL_DOLLARS    DECIMAL(7,2),
  SPREAD_TOTAL_MONTHS     INT,
  SPREAD_MONTHS_CHARGED   INT,
  SPREAD_CHG              STRING,
  SPREAD_TAX_CMP          STRING,
  BYPASS_MATRIX           STRING,
  SERVICE_STATUS          STRING,
  SERVICE_TYPE            STRING,
  PRI_MINOR_ACCT_NBR      INT,
  BILL_THRU_DT            INT,
  CHECKED_IN              STRING,
  SOFT_DISCONNECT         STRING,
  BILLABLE_OVERRIDE       STRING,
  PRIMARY_OR_ADDL         STRING,
  ALA_CARTE_RATE          DECIMAL(7,2),
  STOP_BILLING            STRING,
  PREV_BILLING_RATE       DECIMAL(7,2),
  COMPANY_NBR             INT,
  DIVISION_NBR            INT,
  FRANCHISE_NBR           INT,
  BILL_TYPE_CD            STRING,
  BILL_WITH_ACCT_NBR      BIGINT,
  BILL_WITH_STMT_CD       INT,
  AR_CD                   INT,
  BILLING_FREQ_CD         STRING,
  CUSTOMER_KEY            BIGINT,
  PRICE_PROTECTION_END_DT INT
)
PARTITIONED BY (SOURCE_DATE STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################