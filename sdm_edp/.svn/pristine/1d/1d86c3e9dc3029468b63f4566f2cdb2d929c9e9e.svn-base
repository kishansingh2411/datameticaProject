--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the gold gold_srv_dim_tbl table from incoming.
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
     SRV_DIM_PK
     , NVL(SRV_CODE,'null')
     , NVL(SRV_CODE_DESCR,'null')
     , NVL(SRV_TYPE,'null')
     , NVL(SRV_SUBGROUP_DIM_PK,'null')
     , NVL(SRV_SUBGROUP_DESCR,'null')
     , SRV_GROUP_DIM_PK
     , NVL(SRV_GROUP_DESCR,'null')
     , SRV_LOB_DIM_PK
     , NVL(SRV_LOB_DESCR,'null')
     , DATA_DOWN_SPEED
     , DATA_UP_SPEED
     , NVL(SUB_COUNT_FLAG,'null')
     , CREATE_TSP
     , NVL(CREATE_USER,'null')
     , LAST_UPD_TSP
     , NVL(LAST_UPD_USER,'null')
     , NVL(REC_STATUS,'null')
     , NVL(UNITS_IND,'null')
     , SRV_COM_SUBGROUP_DIM_PK
     , NVL(SRV_COM_SUBGROUP_DESCR,'null')
     , SRV_COM_GROUP_DIM_PK
     , NVL(SRV_COM_GROUP_DESCR,'null')
     , SRV_COM_LOB_DIM_PK
     , NVL(SRV_COM_LOB_DESCR,'null')
     , NVL(COM_SUB_COUNT_FLAG,'null')
     , NVL(COM_SUB_COUNT_FLAG_NON_100,'null')
     , NVL(COM_UNITS_IND,'null')
     , NVL(CB_IND,'null')
     , SRV_EBU_SUBGROUP_DIM_PK
     , NVL(SRV_EBU_SUBGROUP_DESCR,'null')
     , SRV_EBU_GROUP_DIM_PK
     , NVL(SRV_EBU_GROUP_DESCR,'null')
     , SRV_EBU_LOB_DIM_PK
     , NVL(SRV_EBU_LOB_DESCR,'null')
     , NVL(EBU_SUB_COUNT_FLAG,'null')
     , NVL(EBU_UNITS_IND,'null')
     , CONSUMPTION_LIMIT
     , CHARGE_PER_UNIT
     , TYPICAL_MONTHLY_USAGE_GB
     , CAPITALIZATION_FLAG
     , CAP_OTC_FLAG
     , CAP_OTC_POINTS
     , SRV_CAP_GROUP_DIM_PK
     , NVL(SRV_CAP_GROUP_DESCR,'null')
     , BILLING_UNIT
     , P_YYYYMMDD
FROM ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_table}
WHERE 
	REC_STATUS = 'C'
	AND P_YYYYMMDD='${hivevar:source_date}'		
;

--##############################################################################
--#                                    End                                     #
--##############################################################################