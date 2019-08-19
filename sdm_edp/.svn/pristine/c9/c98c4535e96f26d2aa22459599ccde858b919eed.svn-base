--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the gold gold_house_master table from incoming.
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 01/22/2017
--#   Log File    : .../log/sdm/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/sdm/${job_name}.log
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

--##############################################################################
--#                           Hive Table Definition                            #
--##############################################################################

INSERT OVERWRITE TABLE ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_table}
  PARTITION(P_YYYYMMDD)
SELECT 
      SITE_ID
     , NVL(HOUSE_MSTR_RCD_CD,'null')
     , HOUSE_NBR
     , HOUSE_RESIDENT_NBR
     , NVL(ADDR_LOC,'null')
     , NVL(ADDR_FRACTION,'null')
     , NVL(PRE_DIRECTIONAL,'null')
     , NVL(STREET_NM,'null')
     , NVL(CITY_NM,'null')
     , NVL(STATE_CD,'null')
     , NVL(BLDG,'null')
     , NVL(APT,'null')
     , NVL(DWELLING_TYP,'null')
     , NVL(BILL_TYP_CD,'null')
     , REPL_DROP_DT
     , NVL(COMPLEX_NM,'null')
     , NVL(AMP,'null')
     , NVL(PRE_WIRED,'null')
     , NVL(SIGNAL_ACC_CD,'null')
     , COMPANY_NBR
     , DIVISION_NBR
     , FRANCHISE_NBR
     , NVL(HEADEND,'null')
     , NVL(TRUNK_RT,'null')
     , NVL(CASCADE,'null')
     , NVL(PWR_SUPPLY,'null')
     , NVL(FEEDER,'null')
     , NVL(LINE_EXT,'null')
     , NVL(SUB_DEF_CD,'null')
     , NVL(PRI_LOC_CD,'null')
     , PRI_OUTLETS
     , ADDL_OUTLETS
     , BILLING_UNITS
     , NVL(ASSOC_CD,'null')
     , NVL(HOUSE_STATUS_CD,'null')
     , NVL(SEC_LOC_CD,'null')
     , FIRST_DROP_DT
     , NVL(COORD_RANGE,'null')
     , NVL(TOWNSHIP,'null')
     , NVL(SECTION,'null')
     , NVL(AUD_CHK_CD,'null')
     , NVL(ADDR_CARR_RT,'null')
     , NVL(HOUSE_DEV_NO,'null')
     , NVL(TAG_NO,'null')
     , NVL(ADDRESSABLE_CD,'null')
     , TURN_HOT_DT
     , NVL(PROJ_NO,'null')
     , NVL(CENSUS_TRACT,'null')
     , NVL(CONTRACT_NBR,'null')
     , NVL(POOL_CD,'null')
     , NVL(AUD_LIST,'null')
     , NVL(UNSERVICEABLE_FLG,'null')
     , NVL(UNDELIVERABLE_FLG,'null')
     , NVL(ADDL_POST_DIR,'null')
     , NVL(NON_US_ZIP_CD,'null')
     , NVL(INTERACTIVE_CAPABLE_FLG,'null')
     , NVL(ADDR_LINE_1,'null')
     , NVL(ADDR_LINE_2,'null')
     , NVL(ADDR_LINE_3,'null')
     , NVL(ADDR_LINE_4,'null')
     , NVL(US_ZIP5,'null')
     , NVL(US_ZIP4,'null')
     , NVL(US_ZIP2,'null')
     , NVL(CNTY_CD,'null')
     , NVL(PRISM_CD,'null')
     , NVL(NODE_CD,'null')
     , NVL(CRE_USER_ID,'null')
     , CRE_DT
     , NVL(LAST_CHG_USER_ID,'null')
     , LAST_CHG_DT
     , REPLACE_OUTLET_DT
     , NVL(ADDR_ADDL_LOC,'null')
     , NVL(ADDR_ALT_STATE,'null')
     , NVL(CABINET_CD,'null')
     , NVL(GRID_ID,'null')
     , NVL(INSIDE_WIRE_OWNERSHIP_CD,'null')
     , PRI_HOUSE_NBR
     , NPA_STD_NBR
     , NVL(SIC_CD,'null')
     , NXX_EXCHG_NBR
     , NVL(LONGT_NO,'null')
     , NVL(LAT_NO,'null')
     , NVL(ELEVATION_NO,'null')
     , NVL(TAR_CD,'null')
     , RATE_CENTER_ID
     , NVL(SEC_TAG_NO,'null')
     , NVL(MSAG_STREET_NM,'null')
     , NVL(MSAG_COMM_NM,'null')
     , NVL(SAG_VALID_ADDR,'null')
     , SAG_VALIDN_DT
     , NVL(HUB_ID,'null')
     , NVL(USPS_SUFFIX_CD,'null')
     , NVL(CALL_CTR_GRP_CD,'null')
     , LOC_ID
     , FIPS_ST_CD
     , FIPS_CNTY_CD
     , FIPS_CITY_CD
     , HOUSE_MASTER_KEY
     , CUSTOMER_KEY
     , NODE_ID
     , CREATED_DATE
     , UPDATED_DATE
     , P_YYYYMMDD
FROM ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_table}
WHERE P_YYYYMMDD='${hivevar:source_date}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################