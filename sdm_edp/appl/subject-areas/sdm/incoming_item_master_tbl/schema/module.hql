--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create incoming_item_master table at Incoming layer
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
  SITE_ID                    INT               COMMENT '',
  ITEM_NBR                   STRING            COMMENT '', 
  ITEM_DESC                  STRING            COMMENT '',
  ITEM_EQUIP_TYPE_CD         STRING            COMMENT '',
  ITEM_ADDRESSABLE           STRING            COMMENT '',
  ITEM_REMOTE_CTRL_CAP       STRING            COMMENT '',
  ITEM_INV_TRACKING          STRING            COMMENT '',
  ITEM_SERIAL_NO_CD          STRING            COMMENT '',
  ITEM_WRRNTY_NO_DY_NEW      INT               COMMENT '',
  ITEM_WRRNTY_NO_DAYS_REPAIR INT               COMMENT '',
  ITEM_WRRNTY_NO_DY_FAILED   INT               COMMENT '',
  ITEM_WRRNTY_AUTO_EXTEND    STRING            COMMENT '',
  ITEM_NONRETURN_CHARGE      DECIMAL(7,2)      COMMENT '',
  ITEM_SIDECAR_ITEM_NO       STRING            COMMENT '',
  ITEM_SWAP_SER_NO_AND_ADDR  STRING            COMMENT '',
  ITEM_DEPOSIT_CD            STRING            COMMENT '',
  ITEM_TEMP_ENABLE_NO_OF_DYS INT               COMMENT '',
  ITEM_SER_NO_MIN_LNTH       INT               COMMENT '',
  ITEM_SER_NO_MAX_LNTH       INT               COMMENT '',
  ITEM_UPDATE_CVMOV          STRING            COMMENT '',
  ASSIGN_LOT_CARTON          STRING            COMMENT '',
  FORCE_RETURN_TO_INV        STRING            COMMENT '',
  HEX_BIN_CNV_CD             STRING            COMMENT '',
  SEND_ACTUAL_STS_CD         STRING            COMMENT '',
  ITEM_SET_TOP_TYPE          STRING            COMMENT '',
  ASSIGN_WAREHOUSE_ROW_BIN   STRING            COMMENT '',
  NODE_DEPENDENT             STRING            COMMENT '',
  DIGITAL_SRVC_CAPABLE       STRING            COMMENT '',
  ITEM_SPECIAL_USE_FLAG_09   STRING            COMMENT '',
  COMPONENT_INV_COUNT_ACT    STRING            COMMENT '',
  ITEM_MOVEMENT_ACTION       STRING            COMMENT '',
  ITEM_COMPONENT_TYPE        STRING            COMMENT '',
  COMPONENT_OUTLET_ASSIGN    STRING            COMMENT '',
  ALLOW_MULTIPLE_PRIMARIES   STRING            COMMENT '',
  ITEM_MODEL_NBR             STRING            COMMENT '',
  ITEM_MODEL_MAKE            STRING            COMMENT '',
  ITEM_DOCSIS_VER            STRING            COMMENT '',
  ITEM_NBR_OF_COMPONENTS     INT               COMMENT '',
  ITEM_PAIRING_RELATIONSHIP  STRING            COMMENT '',
  NIU_VERSION                STRING            COMMENT '',
  NIU_NO_OF_CHANNELS         INT               COMMENT '',
  ITEM_OWNERSHIP_TYPE        STRING            COMMENT '',
  VENDOR                     STRING            COMMENT '',
  SRVC_CATG_CD               STRING            COMMENT '',
  AR_CODE                    INT               COMMENT '',
  ADJ_CODE_CHARGE            INT               COMMENT '',
  ADJ_CD_CR                  INT               COMMENT '',
  ITEM_GRP_CD                STRING            COMMENT '',
  CREATED_USER_ID            STRING            COMMENT '',
  CREATED_DT                 INT               COMMENT '',
  LAST_CHG_USER_ID           STRING            COMMENT '',
  LAST_CHG_DT                INT               COMMENT '',
  ITEM_MASTER_KEY            BIGINT            COMMENT ''
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