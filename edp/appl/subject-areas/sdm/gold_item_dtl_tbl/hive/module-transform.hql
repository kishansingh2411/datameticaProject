--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Deriving the gold gold_item_dtl table from incoming.
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

INSERT OVERWRITE TABLE processed.sdm_item_dtl_hist  PARTITION(SOURCE_DATE)
SELECT LAST_CHANGE   
     , SITE_ID
     , ITEM_NBR
     , PORT_NBR
     , ITEM_DESC
     , ITEM_PROGRAMMABLE_CD
     , ITEM_ADDRESSABLE
     , ITEM_ADDRESS_REQUIRED
     , ITEM_DISABLE_PAY_PER_VIEW
     , ITEM_BOX_TYPE
     , ITEM_BEHAVIOR_CD
     , ITEM_DEACTIVATE_PPV
     , ITEM_INTERDICTION_CD
     , ITEM_DEFAULT_PARENTAL_CD
     , ITEM_SET_TOP_TYPE
     , FEATURE_BIT_TOT
     , MAX_NBR_OF_COMPONENTS
     , PORT_CATG_CD
     , PORT_TYPE
     , PERSONALITY_CD
     , SRVC_REQ_REFERENCE_NBR
     , SRVC_REQ_TYPE
     , CREATED_USER_ID
     , CREATED_DT
     , LAST_CHG_USER_ID
     , LAST_CHG_DT
     , ITEM_MASTER_KEY
     , ITEM_DTL_KEY
     , SOURCE_DATE
FROM incoming.sdm_item_dtl
;

--##############################################################################
--#                                    End                                     #
--##############################################################################