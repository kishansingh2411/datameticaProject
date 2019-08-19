--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Pig script
--#   Purpose:    : Deriving the incoming gold_item_master table from work.
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

INSERT OVERWRITE TABLE processed.sdm_item_master_hist PARTITION(SOURCE_DATE)
SELECT LAST_CHANGE
     , SITE_ID
     , ITEM_NBR
     , ITEM_DESC
     , ITEM_EQUIP_TYPE_CD
     , ITEM_ADDRESSABLE
     , ITEM_REMOTE_CTRL_CAP
     , ITEM_INV_TRACKING
     , ITEM_SERIAL_NO_CD
     , ITEM_WRRNTY_NO_DY_NEW
     , ITEM_WRRNTY_NO_DAYS_REPAIR
     , ITEM_WRRNTY_NO_DY_FAILED
     , ITEM_WRRNTY_AUTO_EXTEND
     , ITEM_NONRETURN_CHARGE
     , ITEM_SIDECAR_ITEM_NO
     , ITEM_SWAP_SER_NO_AND_ADDR
     , ITEM_DEPOSIT_CD
     , ITEM_TEMP_ENABLE_NO_OF_DYS
     , ITEM_SER_NO_MIN_LNTH
     , ITEM_SER_NO_MAX_LNTH
     , ITEM_UPDATE_CVMOV
     , ASSIGN_LOT_CARTON
     , FORCE_RETURN_TO_INV
     , HEX_BIN_CNV_CD
     , SEND_ACTUAL_STS_CD
     , ITEM_SET_TOP_TYPE
     , ASSIGN_WAREHOUSE_ROW_BIN
     , NODE_DEPENDENT
     , DIGITAL_SRVC_CAPABLE
     , ITEM_SPECIAL_USE_FLAG_09
     , COMPONENT_INV_COUNT_ACT
     , ITEM_MOVEMENT_ACTION
     , ITEM_COMPONENT_TYPE
     , COMPONENT_OUTLET_ASSIGN
     , ALLOW_MULTIPLE_PRIMARIES
     , ITEM_MODEL_NBR
     , ITEM_MODEL_MAKE
     , ITEM_DOCSIS_VER
     , ITEM_NBR_OF_COMPONENTS
     , ITEM_PAIRING_RELATIONSHIP
     , NIU_VERSION
     , NIU_NO_OF_CHANNELS
     , ITEM_OWNERSHIP_TYPE
     , VENDOR
     , SRVC_CATG_CD
     , AR_CODE
     , ADJ_CODE_CHARGE
     , ADJ_CD_CR
     , ITEM_GRP_CD
     , CREATED_USER_ID
     , CREATED_DT
     , LAST_CHG_USER_ID
     , LAST_CHG_DT
     , ITEM_MASTER_KEY
     , SOURCE_DATE
FROM incoming.sdm_item_master
;

--##############################################################################
--#                                    End                                     #
--##############################################################################