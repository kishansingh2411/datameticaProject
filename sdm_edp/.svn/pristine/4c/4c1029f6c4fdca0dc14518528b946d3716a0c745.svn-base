--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create customer_master table at Incoming layer
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
    SITE_ID               INT                            COMMENT '',
    ACCT_NBR              BIGINT                         COMMENT '',
    CUSTOMER_KEY          BIGINT                         COMMENT '',
    TITLE                 STRING                         COMMENT '',
    FIRST_NM              STRING                         COMMENT '',
    MIDDLE_INITIAL        STRING                         COMMENT '',
    LAST_NM               STRING                         COMMENT '',
    BILL_TO_ADDR          STRING                         COMMENT '',
    HOME_AREA_CD          INT                            COMMENT '',
    HOME_PHONE_NBR        INT                            COMMENT '',
    BUS_AREA_CD           INT                            COMMENT '',
    BUS_PHONE_NBR         INT                            COMMENT '',
    OTHER_AREA_CD         INT                            COMMENT '',
    OTHER_PHONE           INT                            COMMENT '',
    SSN                   STRING                         COMMENT '',
    CUST_TYPE_CD          STRING                         COMMENT '',
    CUST_CATG             STRING                         COMMENT '',
    CUST_CMT              STRING                         COMMENT '',
    CUST_STS_CD           STRING                         COMMENT '',
    STS_DT                INT                            COMMENT '',
    PRIVACY_CD            INT                            COMMENT '',
    MTHLY_AMT             DECIMAL(7,2)                   COMMENT '',
    MERGE_PRORATE         STRING                         COMMENT '',
    SALESMAN_NBR          INT                            COMMENT '',
    CR_RATING             STRING                         COMMENT '',
    NO_OF_MTHS_AT_CR      INT                            COMMENT '',
    SELECT_CD_AR_RPTS     STRING                         COMMENT '',
    INSTALL_DT            INT                            COMMENT '',
    REASON_CD             STRING                         COMMENT '',
    COMPANY_NBR           INT                            COMMENT '',
    DIVISION_NBR          INT                            COMMENT '',
    FRANCHISE_NBR         INT                            COMMENT '',
    OLD_TRAN_ACCT_NBR     BIGINT                         COMMENT '',
    POOL                  STRING                         COMMENT '',
    BANK_DRAFT            STRING                         COMMENT '',
    BILL_CD               STRING                         COMMENT '',
    RANGE_NBR             BIGINT                         COMMENT '',
    BILLING_RANGE_NBR     BIGINT                         COMMENT '',
    CUST_NM               STRING                         COMMENT '',
    OFFICE_ONLY_DISC      STRING                         COMMENT '',
    MTHLY_RATE_BILLED     DECIMAL(7,2)                   COMMENT '',
    CONNECT_DT            INT                            COMMENT '',
    PIN_NBR               STRING                         COMMENT '',
    HOUSE_NBR             INT                            COMMENT '',
    HOUSE_RESIDENT_NBR    INT                            COMMENT '',
    VIP_CD                STRING                         COMMENT '',
    HOME_EXCHG_NBR        INT                            COMMENT '',
    BUS_EXCHG_NBR         INT                            COMMENT '',
    OTHER_EXCHG_NBR       INT                            COMMENT '',
    CUST_NAME_FORMAT      STRING                         COMMENT '',
    TITLE_OF_LINEAGE      STRING                         COMMENT '',
    EXTERNAL_CR_SCORE_DTE INT                            COMMENT '',
    OLD_TRAN_SITE_ID      INT                            COMMENT '',
    PIN_ENTRY_DT          INT                            COMMENT '',
    VIDEO_RATING_CD       STRING                         COMMENT '',
    CUST_BUS_NAME         STRING                         COMMENT '',
    EXTERNAL_CR_SCORE     STRING                         COMMENT '',
    TAX_EXEMPT_CD         STRING                         COMMENT '',
    LANGUAGE_CD           STRING                         COMMENT '',
    E_CARE_CUST           STRING                         COMMENT '',
    CREATED               TIMESTAMP                      COMMENT '',
    UPDATED               TIMESTAMP                      COMMENT '',
    HOUSE_MASTER_KEY      BIGINT                         COMMENT ''
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