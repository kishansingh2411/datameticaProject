--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Deriving the gold gold_equipment_master table from incoming.
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

INSERT OVERWRITE TABLE processed.sdm_equipment_master_hist  PARTITION(SOURCE_DATE)
SELECT LAST_CHANGE
     , ITEM_ADDRESSABLE
     , SITE_ID
     , SERIAL_NBR
     , ITEM_NBR
     , EQUIPMENT_KEY
     , ACCT_NBR
     , CUSTOMER_KEY
     , ROOM_CD
     , EQUIP_STAT_CD
     , STAT_DT
     , QUALITY_ASSURANCE_CD
     , QUALITY_ASSURANCE_DT
     , INVENTORY_RECEIPT_DT
     , WARRANTY_EXPIRATION_DT
     , PICKUP_WO_OMIT_TAG
     , CAPITALIZATION_CD
     , CAPITALIZATION_DT
     , HEADEND
     , DT_INSTALLED
     , INSTALLER_NBR
     , PURCHASE_CD
     , PURCHASE_DT
     , COMPANY_NBR
     , DIVISION_NBR
     , EQUIP_TYPE_CD
     , LOT
     , CARTON
     , RETURN_REASON_CD
     , HOUSE_NBR
     , CURR_COMPONENT_TYPE
     , COMPONENT_QTY
     , DISPLAY_COMPONENT_IN_OE
     , CREATED_USER_ID
     , CREATED_DT
     , LAST_CHG_USER_ID
     , LAST_CHG_DT
     , ITEM_MASTER_KEY
     , SOURCE_DATE
FROM incoming.sdm_equipment_master
WHERE ITEM_NBR IS NOT NULL AND LAST_CHANGE IS NOT NULL  -- remove split records
;

--##############################################################################
--#                                    End                                     #
--##############################################################################