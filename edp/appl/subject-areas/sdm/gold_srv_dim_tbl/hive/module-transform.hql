--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Deriving the gold gold_srv_dim_tbl table from incoming.
--#   Author(s)   : DataMetica Team
--#   Usage       : pig -p var1=var1 -useHCatalog module-transform.hql
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

INSERT OVERWRITE TABLE processed.sdm_srv_dim_hist  PARTITION(SOURCE_DATE)
SELECT LAST_CHANGE
     , SRV_DIM_PK
     , SRV_CODE
     , SRV_CODE_DESCR
     , SRV_TYPE
     , SRV_SUBGROUP_DIM_PK
     , SRV_SUBGROUP_DESCR
     , SRV_GROUP_DIM_PK
     , SRV_GROUP_DESCR
     , SRV_LOB_DIM_PK
     , SRV_LOB_DESCR
     , DATA_DOWN_SPEED
     , DATA_UP_SPEED
     , SUB_COUNT_FLAG
     , CREATE_TSP
     , CREATE_USER
     , LAST_UPD_TSP
     , LAST_UPD_USER
     , REC_STATUS
     , UNITS_IND
     , SRV_COM_SUBGROUP_DIM_PK
     , SRV_COM_SUBGROUP_DESCR
     , SRV_COM_GROUP_DIM_PK
     , SRV_COM_GROUP_DESCR
     , SRV_COM_LOB_DIM_PK
     , SRV_COM_LOB_DESCR
     , COM_SUB_COUNT_FLAG
     , COM_SUB_COUNT_FLAG_NON_100
     , COM_UNITS_IND, CB_IND
     , SRV_EBU_SUBGROUP_DIM_PK
     , SRV_EBU_SUBGROUP_DESCR
     , SRV_EBU_GROUP_DIM_PK
     , SRV_EBU_GROUP_DESCR
     , SRV_EBU_LOB_DIM_PK
     , SRV_EBU_LOB_DESCR
     , EBU_SUB_COUNT_FLAG
     , EBU_UNITS_IND
     , CONSUMPTION_LIMIT
     , CHARGE_PER_UNIT
     , TYPICAL_MONTHLY_USAGE_GB
     , CAPITALIZATION_FLAG
     , CAP_OTC_FLAG
     , CAP_OTC_POINTS
     , SRV_CAP_GROUP_DIM_PK
     , SRV_CAP_GROUP_DESCR
     , BILLING_UNIT
     , SOURCE_DATE
FROM incoming.sdm_srv_dim
;

--##############################################################################
--#                                    End                                     #
--##############################################################################