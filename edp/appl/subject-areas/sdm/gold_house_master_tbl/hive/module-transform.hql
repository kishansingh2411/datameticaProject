--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Deriving the gold gold_house_master table from incoming.
--#   Author(s)   : DataMetica Team
--#   Usage       : pig -p var1=var1 -useHCatalog module-transform.pig
--#   Date        : 12/28/2015
--#   Log File    : .../log/${suite_name}/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/${suite_name}/${job_name}.log
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

--##############################################################################
--#                           Hive Table Definition                            #
--##############################################################################

INSERT OVERWRITE TABLE processed.sdm_house_master_hist  PARTITION(P_YYYYMMDD)
SELECT LAST_CHANGE
     , SITE_ID
     , HOUSE_MSTR_RCD_CD
     , HOUSE_NBR
     , HOUSE_RESIDENT_NBR
     , ADDR_LOC
     , ADDR_FRACTION
     , PRE_DIRECTIONAL
     , STREET_NM
     , CITY_NM
     , STATE_CD
     , BLDG
     , APT
     , DWELLING_TYP
     , BILL_TYP_CD
     , REPL_DROP_DT
     , COMPLEX_NM
     , AMP
     , PRE_WIRED
     , SIGNAL_ACC_CD
     , COMPANY_NBR
     , DIVISION_NBR
     , FRANCHISE_NBR
     , HEADEND
     , TRUNK_RT
     , CASCADE
     , PWR_SUPPLY
     , FEEDER
     , LINE_EXT
     , SUB_DEF_CD
     , PRI_LOC_CD
     , PRI_OUTLETS
     , ADDL_OUTLETS
     , BILLING_UNITS
     , ASSOC_CD
     , HOUSE_STATUS_CD
     , SEC_LOC_CD
     , FIRST_DROP_DT
     , COORD_RANGE
     , TOWNSHIP
     , SECTION
     , AUD_CHK_CD
     , ADDR_CARR_RT
     , HOUSE_DEV_NO
     , TAG_NO
     , ADDRESSABLE_CD
     , TURN_HOT_DT
     , PROJ_NO
     , CENSUS_TRACT
     , CONTRACT_NBR
     , POOL_CD
     , AUD_LIST
     , UNSERVICEABLE_FLG
     , UNDELIVERABLE_FLG
     , ADDL_POST_DIR
     , NON_US_ZIP_CD
     , INTERACTIVE_CAPABLE_FLG
     , ADDR_LINE_1
     , ADDR_LINE_2
     , ADDR_LINE_3
     , ADDR_LINE_4
     , US_ZIP5
     , US_ZIP4
     , US_ZIP2
     , CNTY_CD
     , PRISM_CD
     , NODE_CD
     , CRE_USER_ID
     , CRE_DT
     , LAST_CHG_USER_ID
     , LAST_CHG_DT
     , REPLACE_OUTLET_DT
     , ADDR_ADDL_LOC
     , ADDR_ALT_STATE
     , CABINET_CD
     , GRID_ID
     , INSIDE_WIRE_OWNERSHIP_CD
     , PRI_HOUSE_NBR
     , NPA_STD_NBR
     , SIC_CD
     , NXX_EXCHG_NBR
     , LONGT_NO
     , LAT_NO
     , ELEVATION_NO
     , TAR_CD
     , RATE_CENTER_ID
     , SEC_TAG_NO
     , MSAG_STREET_NM
     , MSAG_COMM_NM
     , SAG_VALID_ADDR
     , SAG_VALIDN_DT
     , HUB_ID
     , USPS_SUFFIX_CD
     , CALL_CTR_GRP_CD
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
FROM incoming.sdm_house_master
;

--##############################################################################
--#                                    End                                     #
--##############################################################################