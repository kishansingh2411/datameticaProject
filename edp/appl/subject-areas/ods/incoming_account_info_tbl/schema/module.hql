--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_account_info_tbl)
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
   ID_BHV_ACCOUNT             BIGINT             COMMENT 'Primary Key'
   ,DDP_ACCOUNT               VARCHAR(15)        COMMENT 'CHC'
   ,ID_SERVICE_REC            BIGINT             COMMENT 'Foreign key to Cust_Services  (ccip)'
   ,ID_CUST                   BIGINT             COMMENT ''
   ,PPI_VENDOR_ACCOUNT_ID     VARCHAR(30)        COMMENT 'PrePaidInternational account number'
   ,CREATE_SOURCE_APP         VARCHAR(100)       COMMENT 'Application that created the record'
   ,UPDATE_SOURCE_APP         VARCHAR(100)       COMMENT 'Application that updated the record'
   ,DTM_CREATED               TIMESTAMP          COMMENT 'sysdate'
   ,CREATED_BY                VARCHAR(30)        COMMENT ''
   ,DTM_MODIFIED              TIMESTAMP          COMMENT 'sysdate'
   ,MODIFIED_BY               VARCHAR(30)        COMMENT ''
)
PARTITIONED BY (SOURCE_DATE STRING)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################