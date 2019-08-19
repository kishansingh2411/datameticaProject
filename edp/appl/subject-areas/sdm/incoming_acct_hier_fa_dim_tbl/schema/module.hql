--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create acct_hier_fa_dim table at Incoming layer
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
 ACCT_HIER_FA_DIM_PK           BIGINT,
 ICOMS_SITE                    INT,
 ICOMS_COMPANY                 INT,
 ICOMS_DIVISION                INT,
 ICOMS_FRANCHISE               INT,
 FRANCHISE_PS_ID               STRING,
 MSO_DESCR                     STRING,
 REGION_DESCR                  STRING,
 REGION_PS_ID                  STRING,
 ACCT_HIER_REGION_DIM_PK       BIGINT,
 BUSINESS_UNIT_DESCR           STRING,
 BUSINESS_UNIT_PS_ID           STRING,
 ACCT_HIER_BU_DIM_PK           BIGINT,
 BUDGET_ENTITY_DESCR           STRING,
 BUDGET_ENTITY_PS_ID           STRING,
 ACCT_HIER_BE_DIM_PK           BIGINT,
 PLANT_MANAGER_DESCR           STRING,
 PLANT_MANAGER_PS_ID           STRING,
 ACCT_HIER_PM_DIM_PK           BIGINT,
 HEAD_END_DESCR                STRING,
 HEAD_END_STATE_ABBRV          STRING,
 HEAD_END_PS_ID                STRING,
 ACCT_HIER_HE_DIM_PK           BIGINT,
 FRANCHISE_AUTHORITY_DESCR     STRING,
 FRANCHISE_STATE_ABBRV         STRING,
 ACCT_HIER_PSFA_DIM_PK         BIGINT,
 EFFECTIVE_FROM_BUSINESS_DATE  TIMESTAMP,
 EFFECTIVE_TO_BUSINESS_DATE    TIMESTAMP,
 CREATE_TSP                    TIMESTAMP,
 CREATE_USER                   STRING,
 LAST_UPD_TSP                  TIMESTAMP,
 LAST_UPD_USER                 STRING,
 REC_STATUS                    STRING,
 REPORT_FLAG                   TINYINT,
 HSD_USAGE_RPT_FLAG            TINYINT
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