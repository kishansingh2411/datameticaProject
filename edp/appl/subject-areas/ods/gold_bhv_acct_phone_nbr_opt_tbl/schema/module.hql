--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_bhv_acct_phone_nbr_opt)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 02/01/2017
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
--#    1.0     DataMetica Team          02/01/2017       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE TABLE 
   ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
   BHV_ACCT_PHONE_NBR_OPT_ID     BIGINT          COMMENT 'PK of this table (sequence nbr)'
   ,ID_ACCOUNT_TN                BIGINT          COMMENT 'FK to BHVSMGR.ACCOUNT_TNS'
   ,ID_BHV_ACCOUNT               BIGINT          COMMENT 'FK to BHVSMGR.ACCOUNT_INFO'
   ,PHONE_NBR                    VARCHAR(10)     COMMENT 'Sourced from BHVSMGR.ACCOUNT_TNS.TN'
   ,DDP_ACCOUNT                  VARCHAR(15)     COMMENT 'Sourced from BHVSMGR.ACCOUNT_INFO.DDP_ACCOUNT'
   ,AREA_CODE                    VARCHAR(3)      COMMENT 'Sourced from BHVSMGR.ACCOUNT_TNS.TN SUBSTR(TN,1,3)'
   ,PHONE_EXCHANGE               VARCHAR(3)      COMMENT 'Sourced from BHVSMGR.ACCOUNT_TNS.TN SUBSTR(TN,4,3)'
   ,EXCHANGE_EXTENTION           VARCHAR(4)      COMMENT 'Sourced from BHVSMGR.ACCOUNT_TNS.TN SUBSTR(TN,7,4)'
   ,CORP                         INT             COMMENT 'Sourced from BHVSMGR.ACCOUNT_INFO.DDP_ACCOUNT  SUBSTR(DDP_ACCOUNT,1,5)'
   ,HOUSE                        VARCHAR(6)      COMMENT 'Sourced from BHVSMGR.ACCOUNT_INFO.DDP_ACCOUNT  LPAD(SUBSTR(DDP_ACCOUNT,6,6),6, )'
   ,CUST                         VARCHAR(2)      COMMENT 'Sourced from BHVSMGR.ACCOUNT_INFO.DDP_ACCOUNT  LPAD(SUBSTR(DDP_ACCOUNT,11,2),2, )'
   ,DTM_EFFTV                    TIMESTAMP       COMMENT 'Sourced from BHVSMGR.ACCT_INFO.DTM_CREATED'
   ,DTM_EXPIRED                  TIMESTAMP       COMMENT 'populated with BHVSMGR.ACCOUNT_TNS.END_DATE when End_date is NOT NULL Populate with 12/31/2999 when END_DTAE is NULL'
   ,ID_SERVICE_REC               BIGINT          COMMENT 'Sourced from BHVSMGR.ACCT_INFO.ID_SERVICE_REC - FK to CCIP.. CUST_SERVICES - provides CHC (ddp_account)'
   ,ID_CUST                      BIGINT          COMMENT 'Sourced from BHVSMGR.ACCT_INFO.ID_CUST - reference to ID_CUST in the Cust_Services'
   ,PPI_VENDOR_ACCOUNT_ID        VARCHAR(30)     COMMENT 'Sourced from BHVSMGR.ACCT_INFO - PrePaid International account numbers form provisioning'
   ,ID_IPTN                      BIGINT          COMMENT 'Sourced from BHVSMGR.ACCT_TNS.ID_IPTN - FK to iptelmgr.iptns'
   ,ID_TN_TYPE                   INT             COMMENT 'Sourced from BHVSMGR.ACCT_TNS.ID_TN_TYPE -  TN type 1-Permanent 2- Temporary'
   ,ID_IPTEL_ORDER               BIGINT          COMMENT 'Sourced from BHVSMGR.ACCT_TNS.ID_IPTEL_ORDER -   FK to iptelmgr.iptel_orders'
   ,ID_ORDER_STATUS              INT             COMMENT 'Sourced from BHVSMGR.ACCT_TNS.ID_ORDER_STATUS - Orders status values to be found in the Order_Status table in iptelmgr'
   ,DTM_CREATED_SRC              TIMESTAMP       COMMENT 'Sourced from BHVSMGR.ACCOUNT_INFO.DTM_CREATED'
   ,STATUS_DATE                  TIMESTAMP       COMMENT 'Sourced from BHVSMGR.ACCOUNT_TNS.status_date'
   ,END_DATE                     TIMESTAMP       COMMENT 'Sourced from BHVSMGR.ACCOUNT_TNS.end_date'
   ,PORTED                       VARCHAR(1)      COMMENT 'Sourced from BHVSMGR.ACCOUNT_TNS.Ported'
   ,SWAP_FROM_ID_ACCOUNT_TN      BIGINT          COMMENT 'Sourced from BHVSMGR.ACCOUNT_TNS.SWAP_FROM_ID_ACCOUNT_TN'
   ,SWAP_FROM_TN                 VARCHAR(10)     COMMENT 'Sourced from BHVSMGR.ACCOUNT_TNS.SWAP_FROM_TN'
   ,PRIOR_ID_ORDER_STATUS        BIGINT          COMMENT 'Sourced from BHVSMGR.ACCOUNT_TNS.PRIOR_ID_ORDER_STATUS'
   ,DTM_CREATED                  TIMESTAMP       COMMENT 'Datetime record created in this table'
   ,DTM_LAST_MODIFIED            TIMESTAMP       COMMENT 'Datetime record created or updated'
)
CLUSTERED BY (BHV_ACCT_PHONE_NBR_OPT_ID) INTO 1 BUCKETS
STORED AS ORC
LOCATION '${hivevar:location}'
TBLPROPERTIES ("transactional"="true")
;

--##############################################################################
--#                                    End                                     #
--##############################################################################