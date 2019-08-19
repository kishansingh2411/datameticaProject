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
--#   Date        : 1/18/2017
--#   Log File    : .../log/sdm/${job_name}.log
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
--#    1.0     DataMetica Team          01/22/2017      Initial version
--#
--#
--#####################################################################################################################

--##############################################################################
--#                           Hive Table Definition                            #
--##############################################################################

INSERT OVERWRITE TABLE ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_table}
  PARTITION(P_YYYYMMDD)
SELECT 
    NVL(ITEM_ADDRESSABLE,'null')
     , SITE_ID
     , NVL(SERIAL_NBR,'null')
     , NVL(ITEM_NBR,'null')
     , EQUIPMENT_KEY
     , ACCT_NBR
     , CUSTOMER_KEY
     , NVL(ROOM_CD,'null')
     , NVL(EQUIP_STAT_CD,'null')
     , STAT_DT
     , NVL(QUALITY_ASSURANCE_CD,'null')
     , QUALITY_ASSURANCE_DT
     , INVENTORY_RECEIPT_DT
     , WARRANTY_EXPIRATION_DT
     , NVL(PICKUP_WO_OMIT_TAG,'null')
     , NVL(CAPITALIZATION_CD,'null')
     , CAPITALIZATION_DT
     , NVL(HEADEND,'null')
     , DT_INSTALLED
     , INSTALLER_NBR
     , NVL(PURCHASE_CD,'null')
     , PURCHASE_DT
     , COMPANY_NBR
     , DIVISION_NBR
     , NVL(EQUIP_TYPE_CD,'null')
     , NVL(LOT,'null')
     , NVL(CARTON,'null')
     , NVL(RETURN_REASON_CD,'null')
     , HOUSE_NBR
     , NVL(CURR_COMPONENT_TYPE,'null')
     , COMPONENT_QTY
     , NVL(DISPLAY_COMPONENT_IN_OE,'null')
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