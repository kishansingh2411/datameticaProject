--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create incoming_house_master table at Incoming layer
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
      SITE_ID                  INT               COMMENT '',
      HOUSE_MSTR_RCD_CD        STRING            COMMENT '',
      HOUSE_NBR                INT               COMMENT '',
      HOUSE_RESIDENT_NBR       INT               COMMENT '',
      ADDR_LOC                 STRING            COMMENT '',
      ADDR_FRACTION            STRING            COMMENT '',
      PRE_DIRECTIONAL          STRING            COMMENT '',
      STREET_NM                STRING            COMMENT '',
      CITY_NM                  STRING            COMMENT '',
      STATE_CD                 STRING            COMMENT '',
      BLDG                     STRING            COMMENT '',
      APT                      STRING            COMMENT '',
      DWELLING_TYP             STRING            COMMENT '',
      BILL_TYP_CD              STRING            COMMENT '',
      REPL_DROP_DT             INT               COMMENT '',
      COMPLEX_NM               STRING            COMMENT '',
      AMP                      STRING            COMMENT '',
      PRE_WIRED                STRING            COMMENT '',
      SIGNAL_ACC_CD            STRING            COMMENT '',
      COMPANY_NBR              INT               COMMENT '',
      DIVISION_NBR             INT               COMMENT '',
      FRANCHISE_NBR            INT               COMMENT '',
      HEADEND                  STRING            COMMENT '',
      TRUNK_RT                 STRING            COMMENT '',
      CASCADE                  STRING            COMMENT '',
      PWR_SUPPLY               STRING            COMMENT '',
      FEEDER                   STRING            COMMENT '',
      LINE_EXT                 STRING            COMMENT '',
      SUB_DEF_CD               STRING            COMMENT '',
      PRI_LOC_CD               STRING            COMMENT '',
      PRI_OUTLETS              INT               COMMENT '',
      ADDL_OUTLETS             INT               COMMENT '',
      BILLING_UNITS            INT               COMMENT '',
      ASSOC_CD                 STRING            COMMENT '',
      HOUSE_STATUS_CD          STRING            COMMENT '',
      SEC_LOC_CD               STRING            COMMENT '',
      FIRST_DROP_DT            INT               COMMENT '',
      COORD_RANGE              STRING            COMMENT '',
      TOWNSHIP                 STRING            COMMENT '',
      SECTION                  STRING            COMMENT '',
      AUD_CHK_CD               STRING            COMMENT '',
      ADDR_CARR_RT             STRING            COMMENT '',
      HOUSE_DEV_NO             STRING            COMMENT '',
      TAG_NO                   STRING            COMMENT '',
      ADDRESSABLE_CD           STRING            COMMENT '',
      TURN_HOT_DT              INT               COMMENT '',
      PROJ_NO                  STRING            COMMENT '',
      CENSUS_TRACT             STRING            COMMENT '',
      CONTRACT_NBR             BIGINT            COMMENT '',
      POOL_CD                  STRING            COMMENT '',
      AUD_LIST                 STRING            COMMENT '',
      UNSERVICEABLE_FLG        STRING            COMMENT '',
      UNDELIVERABLE_FLG        STRING            COMMENT '',
      ADDL_POST_DIR            STRING            COMMENT '',
      NON_US_ZIP_CD            STRING            COMMENT '',
      INTERACTIVE_CAPABLE_FLG  STRING            COMMENT '',
      ADDR_LINE_1              STRING            COMMENT '',
      ADDR_LINE_2              STRING            COMMENT '',
      ADDR_LINE_3              STRING            COMMENT '',
      ADDR_LINE_4              STRING            COMMENT '',
      US_ZIP5                  STRING            COMMENT '',
      US_ZIP4                  STRING            COMMENT '',
      US_ZIP2                  STRING            COMMENT '',
      CNTY_CD                  STRING            COMMENT '',
      PRISM_CD                 STRING            COMMENT '',
      NODE_CD                  STRING            COMMENT '',
      CRE_USER_ID              STRING            COMMENT '',
      CRE_DT                   INT               COMMENT '',
      LAST_CHG_USER_ID         STRING            COMMENT '',
      LAST_CHG_DT              INT               COMMENT '',
      REPLACE_OUTLET_DT        INT               COMMENT '',
      ADDR_ADDL_LOC            STRING            COMMENT '',
      ADDR_ALT_STATE           STRING            COMMENT '',
      CABINET_CD               STRING            COMMENT '',
      GRID_ID                  STRING            COMMENT '',
      INSIDE_WIRE_OWNERSHIP_CD STRING            COMMENT '',
      PRI_HOUSE_NBR            INT               COMMENT '',
      NPA_STD_NBR              INT               COMMENT '',
      SIC_CD                   STRING            COMMENT '',
      NXX_EXCHG_NBR            INT               COMMENT '',
      LONGT_NO                 STRING            COMMENT '',
      LAT_NO                   STRING            COMMENT '',
      ELEVATION_NO             STRING            COMMENT '',
      TAR_CD                   STRING            COMMENT '',
      RATE_CENTER_ID           INT               COMMENT '',
      SEC_TAG_NO               STRING            COMMENT '',
      MSAG_STREET_NM           STRING            COMMENT '',
      MSAG_COMM_NM             STRING            COMMENT '',
      SAG_VALID_ADDR           STRING            COMMENT '',
      SAG_VALIDN_DT            INT               COMMENT '',
      HUB_ID                   STRING            COMMENT '',
      USPS_SUFFIX_CD           STRING            COMMENT '',
      CALL_CTR_GRP_CD          STRING            COMMENT '',
      LOC_ID                   INT               COMMENT '',
      FIPS_ST_CD               INT               COMMENT '',
      FIPS_CNTY_CD             INT               COMMENT '',
      FIPS_CITY_CD             INT               COMMENT '',
      house_master_KEY         BIGINT            COMMENT '',
      CUSTOMER_KEY             BIGINT            COMMENT '',
      NODE_ID                  BIGINT            COMMENT '',
      CREATED_DATE             TIMESTAMP         COMMENT '',
      UPDATED_DATE             TIMESTAMP         COMMENT ''
)
PARTITIONED BY (P_YYYYMMDD STRING)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################