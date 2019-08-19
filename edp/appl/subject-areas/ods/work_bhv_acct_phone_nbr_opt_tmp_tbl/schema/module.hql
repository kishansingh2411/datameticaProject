--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Work table(work_bhv_acct_phone_nbr_opt_tmp)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 03/01/2017
--#   Log File    : .../log/customer/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/customer/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          03/01/2017       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE TABLE 
   ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
   GOLD_BHV_ACCT_PHONE_NBR_OPT_ID   BIGINT
   ,WORK_BHV_ACCT_PHONE_NBR_OPT_ID  BIGINT
   ,GOLD_ID_BHV_ACCOUNT             BIGINT
   ,WORK_ID_BHV_ACCOUNT             BIGINT
   ,GOLD_PHONE_NBR                  VARCHAR(10)
   ,WORK_PHONE_NBR                  VARCHAR(10)
   ,GOLD_DTM_EFFTV                  TIMESTAMP
   ,WORK_DTM_EFFTV                  TIMESTAMP
   ,GOLD_ID_ACCOUNT_TN              BIGINT
   ,WORK_ID_ACCOUNT_TN              BIGINT
   ,ID_ACCOUNT_TN_FLAG              VARCHAR(1)
   ,GOLD_DDP_ACCOUNT                VARCHAR(15)
   ,WORK_DDP_ACCOUNT                VARCHAR(15)
   ,DDP_ACCOUNT_FLAG                VARCHAR(1)
   ,GOLD_AREA_CODE                  VARCHAR(3)
   ,WORK_AREA_CODE                  VARCHAR(3)
   ,AREA_CODE_FLAG                  VARCHAR(1)
   ,GOLD_PHONE_EXCHANGE             VARCHAR(3)
   ,WORK_PHONE_EXCHANGE             VARCHAR(3)
   ,PHONE_EXCHANGE_FLAG             VARCHAR(1)
   ,GOLD_EXCHANGE_EXTENTION         VARCHAR(4)
   ,WORK_EXCHANGE_EXTENTION         VARCHAR(4)
   ,EXCHANGE_EXTENTION_FLAG         VARCHAR(1)
   ,GOLD_CORP                       INT
   ,WORK_CORP                       INT
   ,CORP_FLAG                       VARCHAR(1)
   ,GOLD_HOUSE                      VARCHAR(6)
   ,WORK_HOUSE                      VARCHAR(6)
   ,HOUSE_FLAG                      VARCHAR(1)
   ,GOLD_CUST                       VARCHAR(2)
   ,WORK_CUST                       VARCHAR(2)
   ,CUST_FLAG                       VARCHAR(1)
   ,GOLD_DTM_EXPIRED                TIMESTAMP
   ,WORK_DTM_EXPIRED                TIMESTAMP
   ,DTM_EXPIRED_FLAG                VARCHAR(1)
   ,GOLD_ID_SERVICE_REC             BIGINT
   ,WORK_ID_SERVICE_REC             BIGINT
   ,ID_SERVICE_REC_FLAG             VARCHAR(1)
   ,GOLD_ID_CUST                    BIGINT
   ,WORK_ID_CUST                    BIGINT
   ,ID_CUST_FLAG                    VARCHAR(1)
   ,GOLD_PPI_VENDOR_ACCOUNT_ID      VARCHAR(30)
   ,WORK_PPI_VENDOR_ACCOUNT_ID      VARCHAR(30)
   ,PPI_VENDOR_ACCOUNT_ID_FLAG      VARCHAR(1)
   ,GOLD_ID_IPTN                    BIGINT
   ,WORK_ID_IPTN                    BIGINT
   ,ID_IPTN_FLAG                    VARCHAR(1)
   ,GOLD_ID_TN_TYPE                 INT
   ,WORK_ID_TN_TYPE                 INT
   ,ID_TN_TYPE_FLAG                 VARCHAR(1)
   ,GOLD_ID_IPTEL_ORDER             BIGINT
   ,WORK_ID_IPTEL_ORDER             BIGINT
   ,ID_IPTEL_ORDER_FLAG             VARCHAR(1)
   ,GOLD_ID_ORDER_STATUS            INT
   ,WORK_ID_ORDER_STATUS            INT
   ,ID_ORDER_STATUS_FLAG            VARCHAR(1)
   ,GOLD_DTM_CREATED_SRC            TIMESTAMP
   ,WORK_DTM_CREATED_SRC            TIMESTAMP
   ,DTM_CREATED_SRC_FLAG            VARCHAR(1)
   ,GOLD_STATUS_DATE                TIMESTAMP
   ,WORK_STATUS_DATE                TIMESTAMP
   ,STATUS_DATE_FLAG                VARCHAR(1)
   ,GOLD_END_DATE                   TIMESTAMP
   ,WORK_END_DATE                   TIMESTAMP
   ,END_DATE_FLAG                   VARCHAR(1)
   ,GOLD_PORTED                     VARCHAR(1)
   ,WORK_PORTED                     VARCHAR(1)
   ,PORTED_FLAG                     VARCHAR(1)
   ,GOLD_SWAP_FROM_ID_ACCOUNT_TN    BIGINT
   ,WORK_SWAP_FROM_ID_ACCOUNT_TN    BIGINT
   ,SWAP_FROM_ID_ACCOUNT_TN_FLAG    VARCHAR(1)
   ,GOLD_SWAP_FROM_TN               VARCHAR(10)
   ,WORK_SWAP_FROM_TN               VARCHAR(10)
   ,SWAP_FROM_TN_FLAG               VARCHAR(1)
   ,GOLD_PRIOR_ID_ORDER_STATUS      BIGINT
   ,WORK_PRIOR_ID_ORDER_STATUS      BIGINT
   ,PRIOR_ID_ORDER_STATUS_FLAG      VARCHAR(1)
   ,DTM_CREATED                     TIMESTAMP
   ,DTM_LAST_MODIFIED               TIMESTAMP
   ,NEW_FLAG                        VARCHAR(1)
   ,CHANGED_FLAG                    VARCHAR(1)  
)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################