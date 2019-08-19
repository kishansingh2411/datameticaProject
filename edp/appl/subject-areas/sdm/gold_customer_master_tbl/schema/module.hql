--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_customer_master table at Gold layer
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
    SITE_ID               INT,
    ACCT_NBR              BIGINT,
    CUSTOMER_KEY          BIGINT,
    TITLE                 STRING,
    FIRST_NM              STRING,
    MIDDLE_INITIAL        STRING,
    LAST_NM               STRING,
    BILL_TO_ADDR          STRING,
    HOME_AREA_CD          INT,
    HOME_PHONE_NBR        INT,
    BUS_AREA_CD           INT,
    BUS_PHONE_NBR         INT,
    OTHER_AREA_CD         INT,
    OTHER_PHONE           INT,
    SSN                   STRING,
    CUST_TYPE_CD          STRING,
    CUST_CATG             STRING,
    CUST_CMT              STRING,
    CUST_STS_CD           STRING,
    STS_DT                INT,
    PRIVACY_CD            INT,
    MTHLY_AMT             DECIMAL(7,2),
    MERGE_PRORATE         STRING,
    SALESMAN_NBR          INT,
    CR_RATING             STRING,
    NO_OF_MTHS_AT_CR      INT,
    SELECT_CD_AR_RPTS     STRING,
    INSTALL_DT            INT,
    REASON_CD             STRING,
    COMPANY_NBR           INT,
    DIVISION_NBR          INT,
    FRANCHISE_NBR         INT,
    OLD_TRAN_ACCT_NBR     BIGINT,
    POOL                  STRING,
    BANK_DRAFT            STRING,
    BILL_CD               STRING,
    RANGE_NBR             BIGINT,
    BILLING_RANGE_NBR     BIGINT,
    CUST_NM               STRING,
    OFFICE_ONLY_DISC      STRING,
    MTHLY_RATE_BILLED     DECIMAL(7,2),
    CONNECT_DT            INT,
    PIN_NBR               STRING,
    HOUSE_NBR             INT,
    HOUSE_RESIDENT_NBR    INT,
    VIP_CD                STRING,
    HOME_EXCHG_NBR        INT,
    BUS_EXCHG_NBR         INT,
    OTHER_EXCHG_NBR       INT,
    CUST_NAME_FORMAT      STRING,
    TITLE_OF_LINEAGE      STRING,
    EXTERNAL_CR_SCORE_DTE INT,
    OLD_TRAN_SITE_ID      INT,
    PIN_ENTRY_DT          INT,
    VIDEO_RATING_CD       STRING,
    CUST_BUS_NAME         STRING,
    EXTERNAL_CR_SCORE     STRING,
    TAX_EXEMPT_CD         STRING,
    LANGUAGE_CD           STRING,
    E_CARE_CUST           STRING,
    CREATED               TIMESTAMP,
    UPDATED               TIMESTAMP,
    HOUSE_MASTER_KEY      BIGINT
)
PARTITIONED BY (SOURCE_DATE STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################