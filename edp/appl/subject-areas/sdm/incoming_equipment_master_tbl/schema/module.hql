--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create incoming_equipment_master table at Incoming layer
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
  ITEM_ADDRESSABLE        STRING,
  SITE_ID                 INT,
  SERIAL_NBR              STRING,
  ITEM_NBR                STRING,
  EQUIPMENT_KEY           BIGINT,
  ACCT_NBR                BIGINT,
  CUSTOMER_KEY            BIGINT,
  ROOM_CD                 STRING,
  EQUIP_STAT_CD           STRING,
  STAT_DT                 INT,
  QUALITY_ASSURANCE_CD    STRING,
  QUALITY_ASSURANCE_DT    INT,
  INVENTORY_RECEIPT_DT    INT,
  WARRANTY_EXPIRATION_DT  INT,
  PICKUP_WO_OMIT_TAG      STRING,
  CAPITALIZATION_CD       STRING,
  CAPITALIZATION_DT       INT,
  HEADEND                 STRING,
  DT_INSTALLED            INT,
  INSTALLER_NBR           INT,
  PURCHASE_CD             STRING,
  PURCHASE_DT             INT,
  COMPANY_NBR             INT,
  DIVISION_NBR            INT,
  EQUIP_TYPE_CD           STRING,
  LOT                     STRING,
  CARTON                  STRING,
  RETURN_REASON_CD        STRING,
  HOUSE_NBR               INT,
  CURR_COMPONENT_TYPE     STRING,
  COMPONENT_QTY           INT,
  DISPLAY_COMPONENT_IN_OE STRING,
  CREATED_USER_ID         STRING,
  CREATED_DT              INT,
  LAST_CHG_USER_ID        STRING,
  LAST_CHG_DT             INT,
  ITEM_MASTER_KEY         BIGINT
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