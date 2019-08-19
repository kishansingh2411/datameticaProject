--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Loading data in work_bhv_acct_phone_nbr_sdl table
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

incoming_cust_telephone_nbr = 
   LOAD '${incoming_hive_database}.${incoming_cust_telephone}' 
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

incoming_cust_telephone_nbr_curr =
   FILTER incoming_cust_telephone_nbr BY ( source_date =='${source_date}' )
;

incoming_customer_telephone = 
   LOAD '${incoming_hive_database}.${incoming_customer_telephone}' 
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

incoming_customer_telephone_curr =
   FILTER incoming_customer_telephone BY ( source_date =='${source_date}' )
;

join_customer_telephone_nbr = 
   JOIN  incoming_cust_telephone_nbr_curr 
      BY (site_id,acct_nbr,customer_key, npa_std, exchange, tn_line_nbr),
   incoming_customer_telephone_curr 
      BY (site_id,acct_nbr,customer_key,npa_std_no, nxx_exchange_no, thousand_no)
;

filter_customer_telephone_nbr =
   FILTER join_customer_telephone_nbr BY (incoming_customer_telephone_curr::cust_tel_install_dt > ${cust_tel_install_dt_filter}) 
   AND (UPPER(incoming_cust_telephone_nbr_curr::customer_tn_type_id) == '${customer_tn_type_id_filter}')
;

order_customer_telephone_nbr = 
   ORDER filter_customer_telephone_nbr BY incoming_customer_telephone_curr::cust_phone_sts,
   incoming_cust_telephone_nbr_curr::acct_nbr,
   incoming_customer_telephone_curr::customer_telephone_key,
   incoming_customer_telephone_curr::srvc_occur
;

work_bhv_acct_phone_nbr_sdl =
   FOREACH order_customer_telephone_nbr
   GENERATE seqGen('BHV_ACC_PHONE_NBR_SDL','${namenode_service}') as bhv_acct_phone_nbr_sdl_id
   ,incoming_customer_telephone_curr::site_id as site_id
   ,incoming_customer_telephone_curr::acct_nbr as acct_nbr
   ,SPRINTF('%03d%03d%04d',incoming_cust_telephone_nbr_curr::npa_std,incoming_cust_telephone_nbr_curr::exchange,incoming_cust_telephone_nbr_curr::tn_line_nbr) as phone_nbr
   ,SPRINTF('%03d',incoming_cust_telephone_nbr_curr::npa_std) as area_code
   ,SPRINTF('%03d',incoming_cust_telephone_nbr_curr::exchange) as phone_exchange
   ,SPRINTF('%04d',incoming_cust_telephone_nbr_curr::tn_line_nbr) as exchange_extention
   ,incoming_customer_telephone_curr::cust_phone_sts as cust_phone_sts
   ,incoming_cust_telephone_nbr_curr::customer_tn_type_id as customer_tn_type
   ,incoming_customer_telephone_curr::srvc_class as srvc_class
   ,(ToDate(CONCAT((chararray)((cust_tel_install_dt + ${dtm_efftv_param_1}) == ${dtm_efftv_param_1} ? ${dtm_efftv_param_2} : cust_tel_install_dt + ${dtm_efftv_param_1}),(SPRINTF('%06d',cust_tel_install_tm))),'${timestamp_fmt}')) as dtm_efftv
   ,(ToDate(CONCAT((chararray)((cust_tel_disc_dt + ${dtm_efftv_param_1}) == ${dtm_efftv_param_1} ? ${dtm_expired_default_dt} : cust_tel_disc_dt + ${dtm_efftv_param_1}),(SPRINTF('%06d',cust_tel_disc_tm))),'${timestamp_fmt}')) as dtm_expired
   ,incoming_customer_telephone_curr::cust_tel_install_dt as cust_tel_install_dt
   ,incoming_customer_telephone_curr::cust_tel_disc_dt as cust_tel_disc_dt
   ,incoming_customer_telephone_curr::cust_tel_install_tm as cust_tel_install_tm
   ,incoming_customer_telephone_curr::cust_tel_disc_tm as cust_tel_disc_tm
   ,(ToDate(CONCAT((chararray)((cust_tel_install_dt + ${dtm_efftv_param_1}) == ${dtm_efftv_param_1} ? ${dtm_efftv_param_2} : cust_tel_install_dt + ${dtm_efftv_param_1}),(SPRINTF('%06d',cust_tel_install_tm))),'${timestamp_fmt}')) as dtm_tel_install
   ,(ToDate(CONCAT((chararray)((cust_tel_disc_dt + ${dtm_efftv_param_1}) == ${dtm_efftv_param_1} ? ${dtm_expired_default_dt} : cust_tel_disc_dt + ${dtm_efftv_param_1}),(SPRINTF('%06d',cust_tel_disc_tm))),'${timestamp_fmt}')) as dtm_tel_disconnect
   ,CurrentTime() as dtm_created
   ,CurrentTime() as dtm_last_modified
;

store work_bhv_acct_phone_nbr_sdl
INTO '${work_hive_databse}.${work_bhv_sdl}'
   USING org.apache.hive.hcatalog.pig.HCatStorer()
;

--##############################################################################
--#                                    End                                     #
--##############################################################################			