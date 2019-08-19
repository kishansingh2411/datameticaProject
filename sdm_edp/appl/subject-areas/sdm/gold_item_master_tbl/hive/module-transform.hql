--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the gold gold_item_master table from incoming.
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
     , NVL(ITEM_NBR,'null')
     , NVL(ITEM_DESC,'null')
     , NVL(ITEM_EQUIP_TYPE_CD,'null')
     , NVL(ITEM_ADDRESSABLE,'null')
     , NVL(ITEM_REMOTE_CTRL_CAP,'null')
     , NVL(ITEM_INV_TRACKING,'null')
     , NVL(ITEM_SERIAL_NO_CD,'null')
     , ITEM_WRRNTY_NO_DY_NEW
     , ITEM_WRRNTY_NO_DAYS_REPAIR
     , ITEM_WRRNTY_NO_DY_FAILED
     , NVL(ITEM_WRRNTY_AUTO_EXTEND,'null')
     , ITEM_NONRETURN_CHARGE
     , NVL(ITEM_SIDECAR_ITEM_NO,'null')
     , NVL(ITEM_SWAP_SER_NO_AND_ADDR,'null')
     , NVL(ITEM_DEPOSIT_CD,'null')
     , ITEM_TEMP_ENABLE_NO_OF_DYS
     , ITEM_SER_NO_MIN_LNTH
     , ITEM_SER_NO_MAX_LNTH
     , NVL(ITEM_UPDATE_CVMOV,'null')
     , NVL(ASSIGN_LOT_CARTON,'null')
     , NVL(FORCE_RETURN_TO_INV,'null')
     , NVL(HEX_BIN_CNV_CD,'null')
     , NVL(SEND_ACTUAL_STS_CD,'null')
     , NVL(ITEM_SET_TOP_TYPE,'null')
     , NVL(ASSIGN_WAREHOUSE_ROW_BIN,'null')
     , NVL(NODE_DEPENDENT,'null')
     , NVL(DIGITAL_SRVC_CAPABLE,'null')
     , NVL(ITEM_SPECIAL_USE_FLAG_09,'null')
     , NVL(COMPONENT_INV_COUNT_ACT,'null')
     , NVL(ITEM_MOVEMENT_ACTION,'null')
     , NVL(ITEM_COMPONENT_TYPE,'null')
     , NVL(COMPONENT_OUTLET_ASSIGN,'null')
     , NVL(ALLOW_MULTIPLE_PRIMARIES,'null')
     , NVL(ITEM_MODEL_NBR,'null')
     , NVL(ITEM_MODEL_MAKE,'null')
     , NVL(ITEM_DOCSIS_VER,'null')
     , ITEM_NBR_OF_COMPONENTS
     , NVL(ITEM_PAIRING_RELATIONSHIP,'null')
     , NVL(NIU_VERSION,'null')
     , NIU_NO_OF_CHANNELS
     , NVL(ITEM_OWNERSHIP_TYPE,'null')
     , NVL(VENDOR,'null')
     , NVL(SRVC_CATG_CD,'null')
     , AR_CODE
     , ADJ_CODE_CHARGE
     , ADJ_CD_CR
     , NVL(ITEM_GRP_CD,'null')
     , NVL(CREATED_USER_ID,'null')
     , CREATED_DT
     , NVL(LAST_CHG_USER_ID,'null')
     , LAST_CHG_DT
     , ITEM_MASTER_KEY
     , P_YYYYMMDD
FROM ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_table}
WHERE P_YYYYMMDD='${hivevar:source_date}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################