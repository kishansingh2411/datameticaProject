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
  SITE_ID                   INT,
  ITEM_NBR                  STRING,
  SERIAL_NBR                STRING,
  PORT_NBR                  INT,
  EQUIPMENT_ADDRESSABLE     STRING,
  STATUS_DT                 INT,
  QUALITY_ASSURANCE_CD      STRING,
  QUALITY_ASSURANCE_DT      INT,
  EQUIPMENT_ADDRESS         STRING,
  EQUIPMENT_OVERRIDE_ACTIVE STRING,
  INITIALIZE_REQUIRED       STRING,
  PARENTAL_CD               STRING,
  TEMP_ENABLED              STRING,
  TRANSMISSION_DT           INT,
  DNS_NM                    STRING,
  IP_ADDRESS                STRING,
  FQDN                      STRING,
  LOCAL_STATUS              STRING,
  LOCAL_STATUS_DT           INT,
  SERVER_ID                 STRING,
  SERVER_STATUS             STRING,
  SERVER_STATUS_DT          INT,
  EQUIP_DTL_STATUS          STRING,
  PORT_CAT_CD               STRING,
  CABLE_CARD_ID             STRING,
  HEADEND                   STRING,
  ACCT_NBR                  BIGINT,
  SUB_ACCT_ID               INT,
  VIDEO_RATING_CD           STRING,
  PORT_TYPE                 STRING,
  SERVICE_CAT_CD            STRING,
  SERVICE_OCCURRENCE        INT,
  CREATED_USER_ID           STRING,
  DATE_CREATED              INT,
  LAST_CHANGE_USER_ID       STRING,
  LAST_CHANGE_DT            INT,
  EQUIPMENT_KEY             BIGINT,
  equipment_dtls_KEY        BIGINT,
  CUSTOMER_KEY              BIGINT,
  ITEM_DTL_KEY              BIGINT
)
PARTITIONED BY (SOURCE_DATE STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################