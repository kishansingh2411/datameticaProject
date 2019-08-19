--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Gold table(incoming_ov_telephone_number_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 12/28/2016
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
--#    1.0     DataMetica Team          12/28/2016       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
   CUSTOMER_ACCOUNT_ID         INT  COMMENT  ' ',
  DWELLING_ID                 INT  COMMENT  ' ',
  CUSTOMER_ID                 INT  COMMENT  ' ',
  CORP_ID                     INT  COMMENT  ' ',
  CORP                        INT  COMMENT  ' ',
  DWELLING_NBR                VARCHAR(6)  COMMENT  ' ',
  CUST                        VARCHAR(2)  COMMENT  ' ',
  STATUS                      CHAR(1)     COMMENT  ' ',
  TITLE                       VARCHAR(10) COMMENT  ' ',
  LAST_NAME                   VARCHAR(20) COMMENT  ' ',
  FIRST_MIDDLE_INITIAL        VARCHAR(2)  COMMENT  ' ',
  FIRST_NAME                  VARCHAR(20) COMMENT  ' ',
  ACCOUNT_TYPE                VARCHAR(1)  COMMENT  ' ',
  CONVERTER_IND               DECIMAL(1,0) COMMENT  ' ',
  MONTHLY_RATE                DECIMAL(9,2) COMMENT  ' ',
  BILLING_CYCLE               CHAR(1)      COMMENT  ' ',
  DTM_ORIGINAL_INSTALL        TIMESTAMP    COMMENT  ' ',
  WRT_OFF_AMT                 DECIMAL(9,2)  COMMENT  ' ',
  DTM_BILLTHRU                TIMESTAMP     COMMENT  ' ',
  SALES_REASON_ID             INT  COMMENT  ' ',
  ACTIVE_IND                  CHAR(1)  COMMENT  ' ',
  CUSTOMER_ACCOUNT_XREF_ID    INT  COMMENT  ' ',
  DTM_CUSTOMER_ACCOUNT_XREF   TIMESTAMP  COMMENT  ' ',
  XREF_REASON_ID              INT  COMMENT  ' ',
  ID_CUST                     INT  COMMENT  ' ',
  VOD_CUSTOMER_ACCOUNT_ID     INT  COMMENT  ' ',
  DTM_EFFTV                   TIMESTAMP  COMMENT  ' ',
  DTM_EXPIRED                 TIMESTAMP  COMMENT  ' ',
  DTM_CREATED                 TIMESTAMP  COMMENT  ' ',
  DTM_LAST_UPDATE             TIMESTAMP  COMMENT  ' ',
  DTM_EFFTV_ALT               TIMESTAMP  COMMENT  ' ',
  DTM_EXPIRED_ALT             TIMESTAMP  COMMENT  ' ',
  ACCT_INFO_CODE              VARCHAR(1)  COMMENT  ' ',
  ACCT_INFO_ID                INT  COMMENT  ' ',
  ACCT_MAIL_CODE              VARCHAR(1)  COMMENT  ' ',
  ACCT_MAIL_ID                INT  COMMENT  ' ',
  ORIGINAL_CAMPAIGN_CODE      VARCHAR(1)  COMMENT  ' ',
  ORIGINAL_CAMPAIGN_ID        INT  COMMENT  ' ',
  ACCT_CLASS_CODE             VARCHAR(1)  COMMENT  ' ',
  ACCT_CLASS_ID               INT  COMMENT  ' ',
  BILLING_ADDRESS_ID          INT  COMMENT  ' ',
  DISCOUNT_CODE               VARCHAR(1)  COMMENT  ' ',
  SALES_REASON_CODE           VARCHAR(1)  COMMENT  ' ',
  HOLD_NBR                    INT  COMMENT  ' ',
  BILLING_ADDRESS_CODE        VARCHAR(1)  COMMENT  ' ',
  ACCT_TYPE_ID                INT  COMMENT  ' ',
  DISCOUNT_CODE_ID            INT  COMMENT  ' ',
  IN_CARE_OF                  VARCHAR(26)  COMMENT  ' ',
  LAST_UOW_ID                 INT  COMMENT  ' ',
  SSN                         VARCHAR(9)  COMMENT  ' ',
  MEMO_CREDIT                 DECIMAL(9,2)  COMMENT  ' ',
  RESIDENTIAL_AREA_CODE       VARCHAR(3)  COMMENT  ' ',
  RESIDENTIAL_PHONE_NBR       VARCHAR(7)  COMMENT  ' ',
  BUSINESS_AREA_CODE          VARCHAR(3)  COMMENT  ' ',
  BUSINESS_PHONE_NBR          VARCHAR(7)  COMMENT  ' ',
  AR_BAL_CURRENT_AMT          DECIMAL(9,2)  COMMENT  ' ',
  AR_BAL_30                   DECIMAL(9,2)  COMMENT  ' ',
  AR_BAL_60                   DECIMAL(9,2)  COMMENT  ' ',
  AR_BAL_90                   DECIMAL(9,2)  COMMENT  ' ',
  AR_BAL_120                  DECIMAL(9,2)  COMMENT  ' ',
  COLLECTION_STATUS           VARCHAR(1)  COMMENT  ' ',
  EXEMPT_FROM_COLLECTION_IND  CHAR(1)  COMMENT  ' ',
  DT_CHARGED_THRU             TIMESTAMP  COMMENT  ' ',
  STATEMENT_FREQ_CODE         VARCHAR(1)  COMMENT  ' ',
  FINANCIAL_REMINDER_SCHEME   VARCHAR(1)  COMMENT  ' ',
  TWILIGHT_IND                CHAR(1)  COMMENT  ' ',
  UNIQUE_CUST_IDENTIFICATION  VARCHAR(15)  COMMENT  ' ',
  DT_OF_UNIQUE_CUSTOMER_RQST  TIMESTAMP  COMMENT  ' '
)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '~'
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################