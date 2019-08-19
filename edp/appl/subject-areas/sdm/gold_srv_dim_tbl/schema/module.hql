--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_srv_dim table at Gold layer
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
  SRV_DIM_PK                  BIGINT,
  SRV_CODE                    STRING,
  SRV_CODE_DESCR              STRING,
  SRV_TYPE                    STRING,
  SRV_SUBGROUP_DIM_PK         BIGINT,
  SRV_SUBGROUP_DESCR          STRING,
  SRV_GROUP_DIM_PK            BIGINT,
  SRV_GROUP_DESCR             STRING,
  SRV_LOB_DIM_PK              BIGINT,
  SRV_LOB_DESCR               STRING,
  DATA_DOWN_SPEED             BIGINT,
  DATA_UP_SPEED               BIGINT,
  SUB_COUNT_FLAG              STRING,
  CREATE_TSP                  TIMESTAMP,
  CREATE_USER                 STRING,
  LAST_UPD_TSP                TIMESTAMP,
  LAST_UPD_USER               STRING,
  REC_STATUS                  STRING,
  UNITS_IND                   STRING,
  SRV_COM_SUBGROUP_DIM_PK     BIGINT,
  SRV_COM_SUBGROUP_DESCR      STRING,
  SRV_COM_GROUP_DIM_PK        BIGINT,
  SRV_COM_GROUP_DESCR         STRING,
  SRV_COM_LOB_DIM_PK          BIGINT,
  SRV_COM_LOB_DESCR           STRING,
  COM_SUB_COUNT_FLAG          STRING,
  COM_SUB_COUNT_FLAG_NON_100  STRING,
  COM_UNITS_IND               STRING,
  CB_IND                      STRING,
  SRV_EBU_SUBGROUP_DIM_PK     BIGINT,
  SRV_EBU_SUBGROUP_DESCR      STRING,
  SRV_EBU_GROUP_DIM_PK        BIGINT,
  SRV_EBU_GROUP_DESCR         STRING,
  SRV_EBU_LOB_DIM_PK          BIGINT,
  SRV_EBU_LOB_DESCR           STRING,
  EBU_SUB_COUNT_FLAG          STRING,
  EBU_UNITS_IND               STRING,
  CONSUMPTION_LIMIT           BIGINT,
  CHARGE_PER_UNIT             DECIMAL(7,2),
  TYPICAL_MONTHLY_USAGE_GB    INT,
  CAPITALIZATION_FLAG         INT,
  CAP_OTC_FLAG                INT,
  CAP_OTC_POINTS              INT,
  SRV_CAP_GROUP_DIM_PK        BIGINT,
  SRV_CAP_GROUP_DESCR         STRING,
  BILLING_UNIT                INT
)
PARTITIONED BY (SOURCE_DATE STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################