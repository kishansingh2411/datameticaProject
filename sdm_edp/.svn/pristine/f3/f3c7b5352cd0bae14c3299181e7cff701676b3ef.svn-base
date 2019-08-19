--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_customer table at Gold layer
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 01/23/2017
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
--#    1.0     DataMetica Team          01/23/2017       Initial version
--#
--#
--#####################################################################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
 SITE_ID                         INT               COMMENT '',
 ACCT_NBR                        BIGINT            COMMENT '',
 CUST_GROUP_CD                   STRING            COMMENT '',
 COMPANY_NBR                     INT               COMMENT '',
 DIVISION_NBR                    INT               COMMENT '',
 FRANCHISE_NBR                   INT               COMMENT '',
 FIRST_NM                        STRING            COMMENT '',
 LAST_NM                         STRING            COMMENT '',
 HOME_AREA_CD                    INT               COMMENT '',
 HOME_EXCHG_NBR                  INT               COMMENT '',
 HOME_PHONE_NBR                  INT               COMMENT '',
 PHONE_HOME                      STRING            COMMENT '',
 BUS_AREA_CD                     INT               COMMENT '',
 BUS_EXCHG_NBR                   INT               COMMENT '',
 BUS_PHONE_NBR                   INT               COMMENT '',
 PHONE_BUS                       STRING            COMMENT '',
 OTHER_AREA_CD                   INT               COMMENT '',
 OTHER_EXCHG_NBR                 INT               COMMENT '',
 OTHER_PHONE                     INT               COMMENT '',
 PHONE_OTHER                     STRING            COMMENT '',
 CUST_TYPE_CD                    STRING            COMMENT '',
 CUST_CATG                       STRING            COMMENT '',
 CUST_STS_CD                     STRING            COMMENT '',
 CUST_NM                         STRING            COMMENT '',
 INSTALL_DT                      STRING            COMMENT '',
 CONNECT_DT                      STRING            COMMENT '',
 HOUSE_NBR                       INT               COMMENT '',
 ADDR_LOC                        STRING            COMMENT '',
 ADDR_FRACTION                   STRING            COMMENT '',
 PRE_DIRECTIONAL                 STRING            COMMENT '',
 ADDL_POST_DIR                   STRING            COMMENT '',
 STREET_NM                       STRING            COMMENT '',
 CITY_NM                         STRING            COMMENT '',
 STATE_CD                        STRING            COMMENT '',
 US_ZIP5                         STRING            COMMENT '',
 US_ZIP4                         STRING            COMMENT '',
 BLDG                            STRING            COMMENT '',
 APT                             STRING            COMMENT '',
 DWELLING_TYP                    STRING            COMMENT '',
 BILL_TYP_CD                     STRING            COMMENT '',
 HEADEND                         STRING            COMMENT '',
 HOUSE_STATUS_CD                 STRING            COMMENT '',
 PRISM_CD                        STRING            COMMENT '',
 NODE_CD                         STRING            COMMENT '',
 NODE_ID                         BIGINT            COMMENT '',
 LONGT_NO                        STRING            COMMENT '',
 LAT_NO                          STRING            COMMENT '',
 REGION_DESCR                    STRING            COMMENT '',
 REGION_PS_ID                    STRING            COMMENT '',
 BUSINESS_UNIT_DESCR             STRING            COMMENT '',
 BUSINESS_UNIT_PS_ID             STRING            COMMENT '',
 BUDGET_ENTITY_DESCR             STRING            COMMENT '',
 BUDGET_ENTITY_PS_ID             STRING            COMMENT '',
 PLANT_MANAGER_DESCR             STRING            COMMENT '',
 PLANT_MANAGER_PS_ID             STRING            COMMENT '',
 HEAD_END_DESCR                  STRING            COMMENT '',
 HEAD_END_STATE_ABBRV            STRING            COMMENT '',
 HEAD_END_PS_ID                  STRING            COMMENT '',
 FRANCHISE_AUTHORITY_DESCR       STRING            COMMENT '',
 FRANCHISE_STATE_ABBRV           STRING            COMMENT '',
 REPORT_FLAG                     TINYINT           COMMENT '',
 HSD_USAGE_RPT_FLAG              TINYINT           COMMENT ''
)
PARTITIONED BY (P_YYYYMMDD STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################