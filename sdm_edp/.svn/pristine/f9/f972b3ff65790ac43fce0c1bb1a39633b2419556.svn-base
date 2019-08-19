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
  SRV_DIM_PK                  BIGINT            COMMENT '',
  SRV_CODE                    STRING            COMMENT '',
  SRV_CODE_DESCR              STRING            COMMENT '',
  SRV_TYPE                    STRING            COMMENT '',
  SRV_SUBGROUP_DIM_PK         BIGINT            COMMENT '',
  SRV_SUBGROUP_DESCR          STRING            COMMENT '',
  SRV_GROUP_DIM_PK            BIGINT            COMMENT '',
  SRV_GROUP_DESCR             STRING            COMMENT '',
  SRV_LOB_DIM_PK              BIGINT            COMMENT '',
  SRV_LOB_DESCR               STRING            COMMENT '',
  DATA_DOWN_SPEED             BIGINT            COMMENT '',
  DATA_UP_SPEED               BIGINT            COMMENT '',
  SUB_COUNT_FLAG              STRING            COMMENT '',
  CREATE_TSP                  TIMESTAMP         COMMENT '',
  CREATE_USER                 STRING            COMMENT '',
  LAST_UPD_TSP                TIMESTAMP         COMMENT '',
  LAST_UPD_USER               STRING            COMMENT '',
  REC_STATUS                  STRING            COMMENT '',
  UNITS_IND                   STRING            COMMENT '',
  SRV_COM_SUBGROUP_DIM_PK     BIGINT            COMMENT '',
  SRV_COM_SUBGROUP_DESCR      STRING            COMMENT '',
  SRV_COM_GROUP_DIM_PK        BIGINT            COMMENT '',
  SRV_COM_GROUP_DESCR         STRING            COMMENT '',
  SRV_COM_LOB_DIM_PK          BIGINT            COMMENT '',
  SRV_COM_LOB_DESCR           STRING            COMMENT '',
  COM_SUB_COUNT_FLAG          STRING            COMMENT '',
  COM_SUB_COUNT_FLAG_NON_100  STRING            COMMENT '',
  COM_UNITS_IND               STRING            COMMENT '',
  CB_IND                      STRING            COMMENT '',
  SRV_EBU_SUBGROUP_DIM_PK     BIGINT            COMMENT '',
  SRV_EBU_SUBGROUP_DESCR      STRING            COMMENT '',
  SRV_EBU_GROUP_DIM_PK        BIGINT            COMMENT '',
  SRV_EBU_GROUP_DESCR         STRING            COMMENT '',
  SRV_EBU_LOB_DIM_PK          BIGINT            COMMENT '',
  SRV_EBU_LOB_DESCR           STRING            COMMENT '',
  EBU_SUB_COUNT_FLAG          STRING            COMMENT '',
  EBU_UNITS_IND               STRING            COMMENT '',
  CONSUMPTION_LIMIT           BIGINT            COMMENT '',
  CHARGE_PER_UNIT             DECIMAL(7,2)      COMMENT '',
  TYPICAL_MONTHLY_USAGE_GB    INT               COMMENT '',
  CAPITALIZATION_FLAG         INT               COMMENT '',
  CAP_OTC_FLAG                INT               COMMENT '',
  CAP_OTC_POINTS              INT               COMMENT '',
  SRV_CAP_GROUP_DIM_PK        BIGINT            COMMENT '',
  SRV_CAP_GROUP_DESCR         STRING            COMMENT '',
  BILLING_UNIT                INT               COMMENT ''
)
PARTITIONED BY (P_YYYYMMDD STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################