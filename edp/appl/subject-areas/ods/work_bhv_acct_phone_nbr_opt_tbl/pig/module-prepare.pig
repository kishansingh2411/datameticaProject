--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Loading data in work_bhv_acct_phone_nbr_opt table
--#   Author(s)   : DataMetica Team
--#   Usage       : pig -p var1=var1 -useHCatalog module-transform.pig
--#   Date        : 02/08/2017
--#   Log File    : .../log/ods/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/ods/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          02/08/2017       Initial version
--#
--#
--#####################################################################################################################
--#                                   Load                                     #
--##############################################################################

register ${pig_udf_jar}/pig-udf-bank_seqGen-${version}.jar;
register ${postgres_jar_path};

define seqGen com.cablevision.edh.udf.SequenceGenerator();

incoming_info = 
   LOAD '${incoming_hive_database}.${incoming_info}' 
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

incoming_info_curr =
   FILTER incoming_info BY ( source_date =='${source_date}' )
;

incoming_tns = 
   LOAD '${incoming_hive_database}.${incoming_tns}' 
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

incoming_tns_curr =
   FILTER incoming_tns BY ( source_date =='${source_date}' )
;

join_info_tns = 
   JOIN  incoming_info_curr BY id_bhv_account,
   incoming_tns_curr BY id_bhv_account
;

work_bhv_acct_phone_nbr_opt = 
   FOREACH join_info_tns
   GENERATE seqGen('BHV_ACC_PHONE_NBR_OPT','${namenode_service}') as bhv_acct_phone_nbr_opt_id
   ,incoming_tns_curr::id_account_tn as id_account_tn
   ,incoming_info_curr::id_bhv_account as id_bhv_account
   ,incoming_tns_curr::tn  as phone_nbr
   ,incoming_info_curr::ddp_account as ddp_account
   ,SUBSTRING(incoming_tns_curr::tn,0,3) as area_code 
   ,SUBSTRING(incoming_tns_curr::tn,3,6) as phone_exchange
   ,SUBSTRING(incoming_tns_curr::tn,6,10) as exchange_extention
   ,(int)SUBSTRING(incoming_info_curr::ddp_account,0,5) as corp  
   ,SUBSTRING(incoming_info_curr::ddp_account,5,11) as house
   ,SUBSTRING(incoming_info_curr::ddp_account,11,13) as cust
   ,incoming_tns_curr::dtm_created as dtm_efftv
   ,(incoming_tns_curr::end_date is null ? ToDate('${default_timestamp}','yyyy-MM-dd HH:mm:ss') : incoming_tns_curr::end_date) as dtm_expired
   ,incoming_info_curr::id_service_rec as id_service_rec
   ,incoming_info_curr::id_cust as id_cust
   ,incoming_info_curr::ppi_vendor_account_id as ppi_vendor_account_id
   ,incoming_tns_curr::id_iptn as id_iptn
   ,incoming_tns_curr::id_tn_type as id_tn_type
   ,incoming_tns_curr::id_iptel_order as id_iptel_order
   ,incoming_tns_curr::id_order_status as id_order_status
   ,incoming_info_curr::dtm_created as dtm_created_src
   ,incoming_tns_curr::status_date as status_date
   ,incoming_tns_curr::end_date as end_date
   ,incoming_tns_curr::ported as ported
   ,incoming_tns_curr::swap_from_id_account_tn as swap_from_id_account_tn
   ,incoming_tns_curr::swap_from_tn as swap_from_tn
   ,incoming_tns_curr::prior_id_order_status as prior_id_order_status
   ,CurrentTime() as dtm_created
   ,CurrentTime() as dtm_last_modified
;

STORE work_bhv_acct_phone_nbr_opt 
   INTO '${work_hive_databse}.${work_bhv_opt}'
   USING org.apache.hive.hcatalog.pig.HCatStorer()
;

--##############################################################################
--#                                    End                                     #
--##############################################################################			