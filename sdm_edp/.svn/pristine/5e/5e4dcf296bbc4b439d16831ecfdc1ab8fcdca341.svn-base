--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_item_dtl table at Gold layer
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
  SITE_ID                   INT               COMMENT '',
  ITEM_NBR                  STRING            COMMENT '',
  PORT_NBR                  INT               COMMENT '',
  ITEM_DESC                 STRING            COMMENT '',
  ITEM_PROGRAMMABLE_CD      STRING            COMMENT '',
  ITEM_ADDRESSABLE          STRING            COMMENT '',
  ITEM_ADDRESS_REQUIRED     STRING            COMMENT '',
  ITEM_DISABLE_PAY_PER_VIEW STRING            COMMENT '',
  ITEM_BOX_TYPE             STRING            COMMENT '',
  ITEM_BEHAVIOR_CD          STRING            COMMENT '',
  ITEM_DEACTIVATE_PPV       STRING            COMMENT '',
  ITEM_INTERDICTION_CD      STRING            COMMENT '',
  ITEM_DEFAULT_PARENTAL_CD  STRING            COMMENT '',
  ITEM_SET_TOP_TYPE         STRING            COMMENT '',
  FEATURE_BIT_TOT           INT               COMMENT '',
  MAX_NBR_OF_COMPONENTS     INT               COMMENT '',
  PORT_CATG_CD              STRING            COMMENT '',
  PORT_TYPE                 STRING            COMMENT '',
  PERSONALITY_CD            STRING            COMMENT '',
  SRVC_REQ_REFERENCE_NBR    BIGINT            COMMENT '',
  SRVC_REQ_TYPE             STRING            COMMENT '',
  CREATED_USER_ID           STRING            COMMENT '',
  CREATED_DT                INT               COMMENT '',
  LAST_CHG_USER_ID          STRING            COMMENT '',
  LAST_CHG_DT               INT               COMMENT '',
  ITEM_MASTER_KEY           BIGINT            COMMENT '',
  ITEM_DTL_KEY              BIGINT            COMMENT ''
)
PARTITIONED BY (P_YYYYMMDD STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################