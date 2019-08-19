--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_customer_service_groups_tbl table at Gold layer
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 01/26/2017
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
--#    1.0     DataMetica Team          01/26/2017       Initial version
--#
--#
--#####################################################################################################################


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
  SITE_ID                      INT                COMMENT '',
  ACCT_NBR                     BIGINT             COMMENT '',
  BASIC                        TINYINT            COMMENT '',
  DIGITAL                      TINYINT            COMMENT '',
  DIGITAL_EQUIPMENT            TINYINT            COMMENT '',
  DVR                          TINYINT            COMMENT '',
  EQUIPMENT                    TINYINT            COMMENT '',
  EXPANDED                     TINYINT            COMMENT '',
  HDTV                         TINYINT            COMMENT '',
  HOME_SECURITY                TINYINT            COMMENT '',
  HSD                          TINYINT            COMMENT '',
  HSD_COMMERCIAL               TINYINT            COMMENT '',
  HSD_BULK_ETHERNET            TINYINT            COMMENT '',
  HSD_FIBER_SERVICES           TINYINT            COMMENT '',
  PRI                          TINYINT            COMMENT '',
  PRI_EXTERNAL                 TINYINT            COMMENT '',
  PRI_INTERNAL                 TINYINT            COMMENT '',
  TELEPHONE                    TINYINT            COMMENT '',
  VOD                          TINYINT            COMMENT '',
  WIFI_HOME                    TINYINT            COMMENT '',
  WIFI_WORK                    TINYINT            COMMENT '',
  WIRE_MAINTENANCE             TINYINT            COMMENT '',
  REGION_DESCR                 STRING             COMMENT '', 
  REGION_PS_ID                 STRING             COMMENT '', 
  BUSINESS_UNIT_DESCR          STRING             COMMENT '', 
  BUSINESS_UNIT_PS_ID          STRING             COMMENT '', 
  BUDGET_ENTITY_DESCR          STRING             COMMENT '', 
  BUDGET_ENTITY_PS_ID          STRING             COMMENT '', 
  PLANT_MANAGER_DESCR          STRING             COMMENT '', 
  PLANT_MANAGER_PS_ID          STRING             COMMENT '', 
  HEAD_END_DESCR               STRING             COMMENT '', 
  HEAD_END_STATE_ABBRV         STRING             COMMENT '', 
  HEAD_END_PS_ID               STRING             COMMENT '', 
  FRANCHISE_AUTHORITY_DESCR    STRING             COMMENT '', 
  FRANCHISE_STATE_ABBRV        STRING             COMMENT '',  
  REPORT_FLAG                  TINYINT            COMMENT '', 
  HSD_USAGE_RPT_FLAG           TINYINT            COMMENT '',
  NODE_CD                      STRING             COMMENT ''
)
PARTITIONED BY (P_YYYYMMDD STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################