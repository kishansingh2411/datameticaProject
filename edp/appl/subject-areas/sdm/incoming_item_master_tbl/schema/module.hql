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
  SITE_ID                    INT,
  ITEM_NBR                   STRING,
  ITEM_DESC                  STRING,
  ITEM_EQUIP_TYPE_CD         STRING,
  ITEM_ADDRESSABLE           STRING,
  ITEM_REMOTE_CTRL_CAP       STRING,
  ITEM_INV_TRACKING          STRING,
  ITEM_SERIAL_NO_CD          STRING,
  ITEM_WRRNTY_NO_DY_NEW      INT,
  ITEM_WRRNTY_NO_DAYS_REPAIR INT,
  ITEM_WRRNTY_NO_DY_FAILED   INT,
  ITEM_WRRNTY_AUTO_EXTEND    STRING,
  ITEM_NONRETURN_CHARGE      DECIMAL(7,2),
  ITEM_SIDECAR_ITEM_NO       STRING,
  ITEM_SWAP_SER_NO_AND_ADDR  STRING,
  ITEM_DEPOSIT_CD            STRING,
  ITEM_TEMP_ENABLE_NO_OF_DYS INT,
  ITEM_SER_NO_MIN_LNTH       INT,
  ITEM_SER_NO_MAX_LNTH       INT,
  ITEM_UPDATE_CVMOV          STRING,
  ASSIGN_LOT_CARTON          STRING,
  FORCE_RETURN_TO_INV        STRING,
  HEX_BIN_CNV_CD             STRING,
  SEND_ACTUAL_STS_CD         STRING,
  ITEM_SET_TOP_TYPE          STRING,
  ASSIGN_WAREHOUSE_ROW_BIN   STRING,
  NODE_DEPENDENT             STRING,
  DIGITAL_SRVC_CAPABLE       STRING,
  ITEM_SPECIAL_USE_FLAG_09   STRING,
  COMPONENT_INV_COUNT_ACT    STRING,
  ITEM_MOVEMENT_ACTION       STRING,
  ITEM_COMPONENT_TYPE        STRING,
  COMPONENT_OUTLET_ASSIGN    STRING,
  ALLOW_MULTIPLE_PRIMARIES   STRING,
  ITEM_MODEL_NBR             STRING,
  ITEM_MODEL_MAKE            STRING,
  ITEM_DOCSIS_VER            STRING,
  ITEM_NBR_OF_COMPONENTS     INT,
  ITEM_PAIRING_RELATIONSHIP  STRING,
  NIU_VERSION                STRING,
  NIU_NO_OF_CHANNELS         INT,
  ITEM_OWNERSHIP_TYPE        STRING,
  VENDOR                     STRING,
  SRVC_CATG_CD               STRING,
  AR_CODE                    INT,
  ADJ_CODE_CHARGE            INT,
  ADJ_CD_CR                  INT,
  ITEM_GRP_CD                STRING,
  CREATED_USER_ID            STRING,
  CREATED_DT                 INT,
  LAST_CHG_USER_ID           STRING,
  LAST_CHG_DT                INT,
  item_master_KEY            BIGINT
)
PARTITIONED BY (SOURCE_DATE STRING)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################