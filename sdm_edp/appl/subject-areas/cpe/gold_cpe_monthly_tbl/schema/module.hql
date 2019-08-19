--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_cpe_monthly_tbl table at Gold layer
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 01/20/2017
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
--#    1.0     DataMetica Team          01/20/2017       Initial version
--#
--#
--#####################################################################################################################


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
  BEGIN_DATE              STRING        COMMENT '',
  END_DATE                STRING        COMMENT '',
  REGION_PS_ID            STRING        COMMENT '',
  PLANT_MANAGER_DESCR     STRING        COMMENT '',
  HEAD_END_DESCR          STRING        COMMENT '',
  NODE_CD                 STRING        COMMENT '',
  ITEM_NBR                STRING        COMMENT '',
  EQMT_TYPE               STRING        COMMENT '',
  CUST_COUNT              BIGINT        COMMENT '',
  EQMT_COUNT              BIGINT        COMMENT '',
  IVR_TECH_TOTAL          INT           COMMENT '',
  IVR_TECH_CONTAIN        INT           COMMENT '',
  IVR_TECH_OFFER          INT           COMMENT '',
  IVR_TECH_VIDEO_TOTAL    INT           COMMENT '',
  IVR_TECH_VIDEO_CONTAIN  INT           COMMENT '',
  IVR_TECH_VIDEO_OFFER    INT           COMMENT '',
  IVR_TECH_DATA_TOTAL     INT           COMMENT '',
  IVR_TECH_DATA_CONTAIN   INT           COMMENT '',
  IVR_TECH_DATA_OFFER     INT           COMMENT '',
  IVR_TECH_PHONE_TOTAL    INT           COMMENT '',
  IVR_TECH_PHONE_CONTAIN  INT           COMMENT '',
  IVR_TECH_PHONE_OFFER    INT           COMMENT '',
  WO_TC_VIDEO             INT           COMMENT '',
  WO_TC_DATA              INT           COMMENT '',
  WO_TC_PHONE             INT           COMMENT '',
  WO_TC_TOTAL             INT           COMMENT '',
  MODEL                   STRING        COMMENT '',
  MODEL_DESC              STRING        COMMENT '',
  LOB                     STRING        COMMENT '',
  CATEGORY                STRING        COMMENT '',
  MAKE                    STRING        COMMENT '',
  ACTIVE                  STRING        COMMENT '',
  RYG_STATUS              STRING        COMMENT '',
  CPE_FORECAST_CATEGORY   STRING        COMMENT '',
  M2_OR_M4                STRING        COMMENT '',
  MODEL_TRIM              STRING        COMMENT '',
  MODEL_CATEGORY          STRING        COMMENT '',
  DOCSIS                  STRING        COMMENT ''
) 
PARTITIONED BY (P_YYYYMM STRING) 
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################