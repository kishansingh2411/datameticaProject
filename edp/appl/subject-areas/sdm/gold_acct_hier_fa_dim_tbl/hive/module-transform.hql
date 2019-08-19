--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Deriving the gold_acct_hier_fa_dim_tbl table from incoming.
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

INSERT OVERWRITE TABLE processed.sdm_acct_hier_fa_dim_hist  PARTITION(SOURCE_DATE)
SELECT 
   LAST_CHANGE, 
   ACCT_HIER_FA_DIM_PK,
   ICOMS_SITE,
   ICOMS_COMPANY,
   ICOMS_DIVISION,
   ICOMS_FRANCHISE,
   FRANCHISE_PS_ID, 
   MSO_DESCR, 
   REGION_DESCR, 
   REGION_PS_ID, 
   ACCT_HIER_REGION_DIM_PK,
   BUSINESS_UNIT_DESCR, 
   BUSINESS_UNIT_PS_ID, 
   ACCT_HIER_BU_DIM_PK,
   BUDGET_ENTITY_DESCR, 
   BUDGET_ENTITY_PS_ID, 
   ACCT_HIER_BE_DIM_PK,
   PLANT_MANAGER_DESCR, 
   PLANT_MANAGER_PS_ID, 
   ACCT_HIER_PM_DIM_PK,
   HEAD_END_DESCR, 
   HEAD_END_STATE_ABBRV, 
   HEAD_END_PS_ID, 
   ACCT_HIER_HE_DIM_PK,
   FRANCHISE_AUTHORITY_DESCR, 
   FRANCHISE_STATE_ABBRV, 
   ACCT_HIER_PSFA_DIM_PK,
   EFFECTIVE_FROM_BUSINESS_DATE, 
   EFFECTIVE_TO_BUSINESS_DATE, 
   CREATE_TSP, 
   CREATE_USER, 
   LAST_UPD_TSP, 
   LAST_UPD_USER, 
   REC_STATUS, 
   REPORT_FLAG,
   HSD_USAGE_RPT_FLAG,
   SOURCE_DATE
FROM incoming.sdm_acct_hier_fa_dim
;