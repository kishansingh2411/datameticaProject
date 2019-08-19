--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_equipment_master table at Gold layer
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
  ITEM_ADDRESSABLE        STRING            COMMENT '',
  SITE_ID                 INT               COMMENT '',
  SERIAL_NBR              STRING            COMMENT '',
  ITEM_NBR                STRING            COMMENT '',
  EQUIPMENT_KEY           BIGINT            COMMENT '',
  ACCT_NBR                BIGINT            COMMENT '',
  CUSTOMER_KEY            BIGINT            COMMENT '',
  ROOM_CD                 STRING            COMMENT '',
  EQUIP_STAT_CD           STRING            COMMENT '',
  STAT_DT                 INT               COMMENT '',
  QUALITY_ASSURANCE_CD    STRING            COMMENT '',
  QUALITY_ASSURANCE_DT    INT               COMMENT '',
  INVENTORY_RECEIPT_DT    INT               COMMENT '',
  WARRANTY_EXPIRATION_DT  INT               COMMENT '',
  PICKUP_WO_OMIT_TAG      STRING            COMMENT '',
  CAPITALIZATION_CD       STRING            COMMENT '',
  CAPITALIZATION_DT       INT               COMMENT '',
  HEADEND                 STRING            COMMENT '',
  DT_INSTALLED            INT               COMMENT '',
  INSTALLER_NBR           INT               COMMENT '',
  PURCHASE_CD             STRING            COMMENT '',
  PURCHASE_DT             INT               COMMENT '',
  COMPANY_NBR             INT               COMMENT '',
  DIVISION_NBR            INT               COMMENT '',
  EQUIP_TYPE_CD           STRING            COMMENT '',
  LOT                     STRING            COMMENT '',
  CARTON                  STRING            COMMENT '',
  RETURN_REASON_CD        STRING            COMMENT '',
  HOUSE_NBR               INT               COMMENT '',
  CURR_COMPONENT_TYPE     STRING            COMMENT '',
  COMPONENT_QTY           INT               COMMENT '',
  DISPLAY_COMPONENT_IN_OE STRING            COMMENT '',
  CREATED_USER_ID         STRING            COMMENT '',
  CREATED_DT              INT               COMMENT '',
  LAST_CHG_USER_ID        STRING            COMMENT '',
  LAST_CHG_DT             INT               COMMENT '',
  ITEM_MASTER_KEY         BIGINT            COMMENT ''
)
PARTITIONED BY (P_YYYYMMDD STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################