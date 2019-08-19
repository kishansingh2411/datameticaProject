--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create incoming_item_dtl table at Incoming layer
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
  PORT_NBR                  INT,
  ITEM_DESC                 STRING,
  ITEM_PROGRAMMABLE_CD      STRING,
  ITEM_ADDRESSABLE          STRING,
  ITEM_ADDRESS_REQUIRED     STRING,
  ITEM_DISABLE_PAY_PER_VIEW STRING,
  ITEM_BOX_TYPE             STRING,
  ITEM_BEHAVIOR_CD          STRING,
  ITEM_DEACTIVATE_PPV       STRING,
  ITEM_INTERDICTION_CD      STRING,
  ITEM_DEFAULT_PARENTAL_CD  STRING,
  ITEM_SET_TOP_TYPE         STRING,
  FEATURE_BIT_TOT           INT,
  MAX_NBR_OF_COMPONENTS     INT,
  PORT_CATG_CD              STRING,
  PORT_TYPE                 STRING,
  PERSONALITY_CD            STRING,
  SRVC_REQ_REFERENCE_NBR    BIGINT,
  SRVC_REQ_TYPE             STRING,
  CREATED_USER_ID           STRING,
  CREATED_DT                INT,
  LAST_CHG_USER_ID          STRING,
  LAST_CHG_DT               INT,
  ITEM_MASTER_KEY           BIGINT,
  item_dtl_KEY              BIGINT
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