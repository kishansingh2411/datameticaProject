--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_account_tbs_tbl)
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

CREATE EXTERNAL TABLE 
   ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
   ID_ACCOUNT_TN              BIGINT        COMMENT 'Primary key'
   ,ID_BHV_ACCOUNT            BIGINT        COMMENT 'Foreign key to Account_info table'
   ,TN                        VARCHAR(10)   COMMENT '10 digit phone'
   ,ID_IPTN                   BIGINT        COMMENT 'Foreign key to IPTNS table'
   ,ID_TN_TYPE                INT           COMMENT 'TN type 1-Permanent 2- Temporary'
   ,ID_IPTEL_ORDER            BIGINT        COMMENT 'Foreign key to Iptel_orders table'
   ,ID_ORDER_STATUS           INT           COMMENT 'Order_status Id (see table to the right)'
   ,STATUS_DATE               TIMESTAMP     COMMENT 'Status date'
   ,END_DATE                  TIMESTAMP     COMMENT ''
   ,PORTED                    VARCHAR(1)    COMMENT ''
   ,DTM_PORT_ACTIVATED        TIMESTAMP     COMMENT ''
   ,ID_FEATURE_PACK           INT           COMMENT ''
   ,AUTO_ATTENDANT            VARCHAR(1)    COMMENT ''
   ,CONSOLE                   VARCHAR(1)    COMMENT ''
   ,MIGRATION_TYPE            VARCHAR(15)   COMMENT ''
   ,WANT_DIR_LISTING          VARCHAR(1)    COMMENT ''
   ,ID_DL_TYPE                INT           COMMENT ''
   ,LIDB_STATUS               VARCHAR(1)    COMMENT ''
   ,LIDB_DATE                 TIMESTAMP     COMMENT ''
   ,CNAM_STATUS               VARCHAR(1)    COMMENT ''
   ,CNAM_DATE                 TIMESTAMP     COMMENT ''
   ,PSALI_STATUS              VARCHAR(1)    COMMENT ''
   ,PSALI_DATE                TIMESTAMP     COMMENT ''
   ,MEDIATION_STATUS          VARCHAR(1)    COMMENT ''
   ,MEDIATION_DATE            TIMESTAMP     COMMENT ''
   ,CREATED_BY_SALES_REP      VARCHAR(30)   COMMENT ''
   ,MODIFIED_BY_SALES_REP     VARCHAR(30)   COMMENT ''
   ,CREATE_SOURCE_APP         VARCHAR(100)  COMMENT ''
   ,UPDATE_SOURCE_APP         VARCHAR(100)  COMMENT ''
   ,DTM_CREATED               TIMESTAMP     COMMENT 'sysdate'
   ,CREATED_BY                VARCHAR(30)   COMMENT ''
   ,DTM_MODIFIED              TIMESTAMP     COMMENT 'sysdate'
   ,MODIFIED_BY               VARCHAR(30)   COMMENT ''
   ,SWAP_FROM_ID_ACCOUNT_TN   BIGINT        COMMENT ''
   ,SWAP_FROM_TN              VARCHAR(10)   COMMENT ''
   ,PRIOR_ID_ORDER_STATUS     BIGINT        COMMENT ''
   ,MIGRATED                  VARCHAR(1)    COMMENT ''
)
PARTITIONED BY (SOURCE_DATE STRING)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################