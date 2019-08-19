--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the gold gold_item_dtl table from incoming.
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
     , PORT_NBR
     , NVL(ITEM_DESC,'null')
     , NVL(ITEM_PROGRAMMABLE_CD,'null')
     , NVL(ITEM_ADDRESSABLE,'null')
     , NVL(ITEM_ADDRESS_REQUIRED,'null')
     , NVL(ITEM_DISABLE_PAY_PER_VIEW,'null')
     , NVL(ITEM_BOX_TYPE,'null')
     , NVL(ITEM_BEHAVIOR_CD,'null')
     , NVL(ITEM_DEACTIVATE_PPV,'null')
     , NVL(ITEM_INTERDICTION_CD,'null')
     , NVL(ITEM_DEFAULT_PARENTAL_CD,'null')
     , NVL(ITEM_SET_TOP_TYPE,'null')
     , FEATURE_BIT_TOT
     , MAX_NBR_OF_COMPONENTS
     , NVL(PORT_CATG_CD,'null')
     , NVL(PORT_TYPE,'null')
     , NVL(PERSONALITY_CD,'null')
     , NVL(SRVC_REQ_REFERENCE_NBR,'null')
     , NVL(SRVC_REQ_TYPE,'null')
     , NVL(CREATED_USER_ID,'null')
     , CREATED_DT
     , NVL(LAST_CHG_USER_ID,'null')
     , LAST_CHG_DT
     , ITEM_MASTER_KEY
     , ITEM_DTL_KEY
     , P_YYYYMMDD
FROM  ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_table}
WHERE P_YYYYMMDD='${hivevar:source_date}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################