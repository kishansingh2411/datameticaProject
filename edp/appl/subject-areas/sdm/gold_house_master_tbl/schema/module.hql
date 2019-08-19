--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_house_master table at Gold layer
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
  SITE_ID                  INT,
  HOUSE_MSTR_RCD_CD        STRING,
  HOUSE_NBR                INT,
  HOUSE_RESIDENT_NBR       INT,
  ADDR_LOC                 STRING,
  ADDR_FRACTION            STRING,
  PRE_DIRECTIONAL          STRING,
  STREET_NM                STRING,
  CITY_NM                  STRING,
  STATE_CD                 STRING,
  BLDG                     STRING,
  APT                      STRING,
  DWELLING_TYP             STRING,
  BILL_TYP_CD              STRING,
  REPL_DROP_DT             INT,
  COMPLEX_NM               STRING,
  AMP                      STRING,
  PRE_WIRED                STRING,
  SIGNAL_ACC_CD            STRING,
  COMPANY_NBR              INT,
  DIVISION_NBR             INT,
  FRANCHISE_NBR            INT,
  HEADEND                  STRING,
  TRUNK_RT                 STRING,
  CASCADE                  STRING,
  PWR_SUPPLY               STRING,
  FEEDER                   STRING,
  LINE_EXT                 STRING,
  SUB_DEF_CD               STRING,
  PRI_LOC_CD               STRING,
  PRI_OUTLETS              INT,
  ADDL_OUTLETS             INT,
  BILLING_UNITS            INT,
  ASSOC_CD                 STRING,
  HOUSE_STATUS_CD          STRING,
  SEC_LOC_CD               STRING,
  FIRST_DROP_DT            INT,
  COORD_RANGE              STRING,
  TOWNSHIP                 STRING,
  SECTION                  STRING,
  AUD_CHK_CD               STRING,
  ADDR_CARR_RT             STRING,
  HOUSE_DEV_NO             STRING,
  TAG_NO                   STRING,
  ADDRESSABLE_CD           STRING,
  TURN_HOT_DT              INT,
  PROJ_NO                  STRING,
  CENSUS_TRACT             STRING,
  CONTRACT_NBR             BIGINT,
  POOL_CD                  STRING,
  AUD_LIST                 STRING,
  UNSERVICEABLE_FLG        STRING,
  UNDELIVERABLE_FLG        STRING,
  ADDL_POST_DIR            STRING,
  NON_US_ZIP_CD            STRING,
  INTERACTIVE_CAPABLE_FLG  STRING,
  ADDR_LINE_1              STRING,
  ADDR_LINE_2              STRING,
  ADDR_LINE_3              STRING,
  ADDR_LINE_4              STRING,
  US_ZIP5                  STRING,
  US_ZIP4                  STRING,
  US_ZIP2                  STRING,
  CNTY_CD                  STRING,
  PRISM_CD                 STRING,
  NODE_CD                  STRING,
  CRE_USER_ID              STRING,
  CRE_DT                   INT,
  LAST_CHG_USER_ID         STRING,
  LAST_CHG_DT              INT,
  REPLACE_OUTLET_DT        INT,
  ADDR_ADDL_LOC            STRING,
  ADDR_ALT_STATE           STRING,
  CABINET_CD               STRING,
  GRID_ID                  STRING,
  INSIDE_WIRE_OWNERSHIP_CD STRING,
  PRI_HOUSE_NBR            INT,
  NPA_STD_NBR              INT,
  SIC_CD                   STRING,
  NXX_EXCHG_NBR            INT,
  LONGT_NO                 STRING,
  LAT_NO                   STRING,
  ELEVATION_NO             STRING,
  TAR_CD                   STRING,
  RATE_CENTER_ID           INT,
  SEC_TAG_NO               STRING,
  MSAG_STREET_NM           STRING,
  MSAG_COMM_NM             STRING,
  SAG_VALID_ADDR           STRING,
  SAG_VALIDN_DT            INT,
  HUB_ID                   STRING,
  USPS_SUFFIX_CD           STRING,
  CALL_CTR_GRP_CD          STRING,
  LOC_ID                   INT,
  FIPS_ST_CD               INT,
  FIPS_CNTY_CD             INT,
  FIPS_CITY_CD             INT,
  HOUSE_MASTER_KEY         BIGINT,
  CUSTOMER_KEY             BIGINT,
  NODE_ID                  BIGINT,
  CREATED_DATE             TIMESTAMP,
  UPDATED_DATE             TIMESTAMP
)
PARTITIONED BY (SOURCE_DATE STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################