--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_equipment_dtls table at Gold layer
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
  SERIAL_NBR                STRING            COMMENT '',
  PORT_NBR                  INT               COMMENT '',
  EQUIPMENT_ADDRESSABLE     STRING            COMMENT '',
  STATUS_DT                 INT               COMMENT '',
  QUALITY_ASSURANCE_CD      STRING            COMMENT '',
  QUALITY_ASSURANCE_DT      INT               COMMENT '',
  EQUIPMENT_ADDRESS         STRING            COMMENT '',
  MAC_ADDRESS               STRING            COMMENT '',
  EQUIPMENT_OVERRIDE_ACTIVE STRING            COMMENT '',
  INITIALIZE_REQUIRED       STRING            COMMENT '',
  PARENTAL_CD               STRING            COMMENT '',
  TEMP_ENABLED              STRING            COMMENT '',
  TRANSMISSION_DT           INT               COMMENT '',
  DNS_NM                    STRING            COMMENT '',
  IP_ADDRESS                STRING            COMMENT '',
  FQDN                      STRING            COMMENT '',
  LOCAL_STATUS              STRING            COMMENT '',
  LOCAL_STATUS_DT           INT               COMMENT '',
  SERVER_ID                 STRING            COMMENT '',
  SERVER_STATUS             STRING            COMMENT '',
  SERVER_STATUS_DT          INT               COMMENT '',
  EQUIP_DTL_STATUS          STRING            COMMENT '',
  PORT_CAT_CD               STRING            COMMENT '',
  CABLE_CARD_ID             STRING            COMMENT '',
  HEADEND                   STRING            COMMENT '',
  ACCT_NBR                  BIGINT            COMMENT '',
  SUB_ACCT_ID               INT               COMMENT '',
  VIDEO_RATING_CD           STRING            COMMENT '',
  PORT_TYPE                 STRING            COMMENT '',
  SERVICE_CAT_CD            STRING            COMMENT '',
  SERVICE_OCCURRENCE        INT               COMMENT '',
  CREATED_USER_ID           STRING            COMMENT '',
  DATE_CREATED              INT               COMMENT '',
  LAST_CHANGE_USER_ID       STRING            COMMENT '',
  LAST_CHANGE_DT            INT               COMMENT '',
  EQUIPMENT_KEY             BIGINT            COMMENT '',
  EQUIPMENT_DTLS_KEY        BIGINT            COMMENT '',
  CUSTOMER_KEY              BIGINT            COMMENT '',
  ITEM_DTL_KEY              BIGINT            COMMENT ''
)
PARTITIONED BY (P_YYYYMMDD STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################