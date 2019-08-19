--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_acct_hier_fa_dim table at Gold layer
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
  ACCT_HIER_FA_DIM_HIST_PK          BIGINT            COMMENT '',
  ICOMS_SITE                        INT               COMMENT '',
  ICOMS_COMPANY                     INT               COMMENT '',
  ICOMS_DIVISION                    INT               COMMENT '',
  ICOMS_FRANCHISE                   INT               COMMENT '',
  FRANCHISE_PS_ID                   STRING            COMMENT '',
  MSO_DESCR                         STRING            COMMENT '',
  REGION_DESCR                      STRING            COMMENT '',
  REGION_PS_ID                      STRING            COMMENT '',
  ACCT_HIER_REGION_DIM_PK           BIGINT            COMMENT '',
  BUSINESS_UNIT_DESCR               STRING            COMMENT '',
  BUSINESS_UNIT_PS_ID               STRING            COMMENT '',
  ACCT_HIER_BU_DIM_PK               BIGINT            COMMENT '',
  BUDGET_ENTITY_DESCR               STRING            COMMENT '',
  BUDGET_ENTITY_PS_ID               STRING            COMMENT '',
  ACCT_HIER_BE_DIM_PK               BIGINT            COMMENT '',
  PLANT_MANAGER_DESCR               STRING            COMMENT '',
  PLANT_MANAGER_PS_ID               STRING            COMMENT '',
  ACCT_HIER_PM_DIM_PK               BIGINT            COMMENT '',
  HEAD_END_DESCR                    STRING            COMMENT '',
  HEAD_END_STATE_ABBRV              STRING            COMMENT '',
  HEAD_END_PS_ID                    STRING            COMMENT '',
  ACCT_HIER_HE_DIM_PK               BIGINT            COMMENT '',
  FRANCHISE_AUTHORITY_DESCR         STRING            COMMENT '',
  FRANCHISE_STATE_ABBRV             STRING            COMMENT '',
  ACCT_HIER_PSFA_DIM_PK             BIGINT            COMMENT '',
  EFFECTIVE_FROM_BUSINESS_DATE      TIMESTAMP         COMMENT '',
  EFFECTIVE_TO_BUSINESS_DATE        TIMESTAMP         COMMENT '',
  CREATE_TSP                        TIMESTAMP         COMMENT '',
  CREATE_USER                       STRING            COMMENT '',
  LAST_UPD_TSP                      TIMESTAMP         COMMENT '',
  LAST_UPD_USER                     STRING            COMMENT '',
  REC_STATUS                        STRING            COMMENT '',
  REPORT_FLAG                       TINYINT           COMMENT '',
  HSD_USAGE_RPT_FLAG                TINYINT           COMMENT ''
)
PARTITIONED BY (P_YYYYMMDD STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################